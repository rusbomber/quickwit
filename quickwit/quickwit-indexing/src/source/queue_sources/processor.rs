// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::QueueParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use serde_json::{json, Value as JsonValue};
use time::OffsetDateTime;
use tracing::debug;

use super::visibility::{spawn_visibility_task, VisibilityTaskHandle};
use super::{PrepocessedPayload, PrepocessedQueueMessage, Queue};
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, PublishLock};
use crate::source::doc_file_reader::DocFileReader;
use crate::source::{SourceActor, SourceContext, SourceRuntime};

#[derive(Default)]
pub struct QueueProcessorObservableState {
    /// Number of bytes processed by the source.
    num_bytes_processed: u64,
    /// Number of messages processed by the source.
    num_messages_processed: u64,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    num_invalid_messages: u64,
    /// Number of time we looped without getting a single message
    num_consecutive_empty_batches: u64,
}

pub struct QueueProcessor {
    source_runtime: SourceRuntime,
    queue: Arc<dyn Queue>,
    observable_state: QueueProcessorObservableState,
    queue_params: QueueParams,
    in_progress: BTreeMap<PartitionId, VisibilityTaskHandle>,
    completed: BTreeMap<PartitionId, OffsetDateTime>,
    init_checkpoint: SourceCheckpoint,
    publish_lock: PublishLock,
}

impl fmt::Debug for QueueProcessor {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("QueueProcessor")
            .field("index_id", &self.source_runtime.index_id())
            .field("queue", &self.queue)
            .finish()
    }
}

impl QueueProcessor {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        queue: Arc<dyn Queue>,
        queue_params: QueueParams,
    ) -> anyhow::Result<Self> {
        let init_checkpoint = source_runtime.fetch_checkpoint().await?;
        let mut completed = BTreeMap::new();
        for (partition_id, position) in init_checkpoint.iter() {
            if position.is_eof() {
                // TODO: use checkpoint timestamp when encoded
                completed.insert(partition_id, OffsetDateTime::now_utc());
            }
        }
        Ok(Self {
            init_checkpoint,
            source_runtime,
            queue,
            observable_state: QueueProcessorObservableState::default(),
            queue_params,
            in_progress: BTreeMap::new(),
            completed,
            publish_lock: PublishLock::default(),
        })
    }

    pub async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let publish_lock = self.publish_lock.clone();
        ctx.send_message(doc_processor_mailbox, NewPublishLock(publish_lock))
            .await?;
        Ok(())
    }

    pub async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        // TODO: handle error instead of unwrap
        // receive() typically uses long polling so it can be quite slow
        let messages = ctx.protect_future(self.queue.receive()).await.unwrap();
        if messages.is_empty() {
            self.observable_state.num_consecutive_empty_batches += 1;
        } else {
            self.observable_state.num_consecutive_empty_batches = 0;
        }
        for message in messages {
            // TODO: handle error instead of unwrap
            let preproc_msg =
                PrepocessedQueueMessage::try_from(self.queue_params.message_type, message).unwrap();

            let partition_id = preproc_msg.partition_id();
            let visiblity_task_handle = spawn_visibility_task(
                self.queue.clone(),
                preproc_msg.metadata.ack_id.clone(),
                preproc_msg.metadata.initial_deadline,
                self.publish_lock.clone(),
            );
            self.in_progress.insert(partition_id, visiblity_task_handle);

            match preproc_msg.payload {
                // TODO: error handling
                PrepocessedPayload::ObjectUri(uri) => {
                    // The last known checkpoint is likely outdated. If this
                    // file was already processed since then, the publish commit
                    // will fail and the pipeline will restart, effectively
                    // updating the checkpoint.
                    let mut file_reader = DocFileReader::from_uri(
                        &self.init_checkpoint,
                        &uri,
                        &self.source_runtime.storage_resolver,
                    )
                    .await?;
                    loop {
                        let batch_resp = file_reader.read_batch(ctx).await?;
                        if let Some(batch) = batch_resp.batch_opt {
                            ctx.send_message(doc_processor_mailbox, batch).await?;
                        }
                        if batch_resp.is_eof {
                            debug!(uri=%uri, "reached end of file");
                            break;
                        }
                    }
                }
            }
        }
        Ok(Duration::default())
    }

    pub async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        let mut completed_visibility_handles = Vec::new();
        for (partition_id, position) in checkpoint.iter() {
            if position.is_eof() {
                if let Some(visibility_handle) = self.in_progress.remove(&partition_id) {
                    completed_visibility_handles.push(visibility_handle);
                }
                self.completed
                    .insert(partition_id, OffsetDateTime::now_utc());
            }
        }
        let ack_ids = completed_visibility_handles
            .iter()
            .map(|handle| handle.ack_id.as_str())
            .collect::<Vec<_>>();

        self.queue.acknowledge(&ack_ids).await?;

        Ok(())
    }

    pub fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
            "num_bytes_processed": self.observable_state.num_bytes_processed,
            "num_messages_processed": self.observable_state.num_messages_processed,
            "num_invalid_messages": self.observable_state.num_invalid_messages,
            "num_consecutive_empty_batches": self.observable_state.num_consecutive_empty_batches,
        })
    }
}
