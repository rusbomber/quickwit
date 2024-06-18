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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::QueueParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::SourceType;
use quickwit_storage::StorageResolver;
use serde_json::{json, Value as JsonValue};

use super::checkpointing::QueueCheckpointing;
use super::message::{CheckpointedMessage, InProgressMessage, PreProcessedMessage};
use super::visibility::{spawn_visibility_task, VisibilityTaskHandle};
use super::Queue;
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, PublishLock};
use crate::source::{SourceContext, SourceRuntime};

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

#[derive(Default)]
pub struct QueueProcessorState {
    ready_messages: Vec<CheckpointedMessage>,
    in_flight: BTreeMap<PartitionId, VisibilityTaskHandle>,
    completed: BTreeSet<PartitionId>,
    in_progress_message: InProgressMessage,
}

pub struct QueueProcessor {
    storage_resolver: StorageResolver,
    pipeline_id: IndexingPipelineId,
    source_type: SourceType,
    queue: Arc<dyn Queue>,
    observable_state: QueueProcessorObservableState,
    queue_params: QueueParams,
    publish_lock: PublishLock,
    checkpointing: QueueCheckpointing,
    state: QueueProcessorState,
}

impl fmt::Debug for QueueProcessor {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("QueueProcessor")
            .field("index_id", &self.pipeline_id.index_uid.index_id)
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
        Ok(Self {
            checkpointing: QueueCheckpointing {
                metastore: source_runtime.metastore,
            },
            pipeline_id: source_runtime.pipeline_id,
            source_type: source_runtime.source_config.source_type(),
            storage_resolver: source_runtime.storage_resolver,
            queue,
            observable_state: QueueProcessorObservableState::default(),
            queue_params,
            publish_lock: PublishLock::default(),
            state: QueueProcessorState::default(),
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

    /// Filters messages that can be processed, acquiring the associated shards
    /// in the process.
    async fn prepare_processable_messages(
        &mut self,
        messages: Vec<PreProcessedMessage>,
    ) -> anyhow::Result<Vec<CheckpointedMessage>> {
        let mut processable_uris = BTreeMap::new();
        let mut completed_uris = Vec::new();
        for message in messages {
            let partition_id = message.partition_id();
            if self.state.completed.contains(&partition_id) {
                completed_uris.push(message);
            } else {
                processable_uris.insert(partition_id, message);
            }
        }

        // TODO acknowledge completed messages

        let processable_partitions = processable_uris.keys().map(|k| k.clone()).collect();
        let acquired_shards = self
            .checkpointing
            .acquire_shards(&self.pipeline_id, processable_partitions)
            .await?;

        acquired_shards
            .into_iter()
            .map(|(partition_id, position)| {
                let content = processable_uris.remove(&partition_id).context("Unexpected partition ID. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")?;
                Ok(CheckpointedMessage {
                    position,
                    content,
                })
            })
            .collect()
    }

    /// Starts background tasks that extend the visibility deadline of the
    /// messages until they are dropped.
    fn spawn_visibility_tasks(&mut self, messages: &[CheckpointedMessage]) {
        for CheckpointedMessage { content, .. } in messages {
            let handle = spawn_visibility_task(
                self.queue.clone(),
                content.metadata.ack_id.clone(),
                content.metadata.initial_deadline,
                self.publish_lock.clone(),
            );
            self.state.in_flight.insert(content.partition_id(), handle);
        }
    }

    /// Polls messages from the queue and prepares them for processing
    async fn poll_messages(&mut self, ctx: &SourceContext) -> anyhow::Result<()> {
        // receive() typically uses long polling so it can be long to respond
        let messages = ctx.protect_future(self.queue.receive()).await?;
        if messages.is_empty() {
            self.observable_state.num_consecutive_empty_batches += 1;
            return Ok(());
        } else {
            self.observable_state.num_consecutive_empty_batches = 0;
        }

        let preprocessed_messages = messages
            .into_iter()
            .map(|m| m.pre_process(self.queue_params.message_type))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let in_progress_messages = self
            .prepare_processable_messages(preprocessed_messages)
            .await?;

        self.spawn_visibility_tasks(&in_progress_messages);

        self.state.ready_messages = in_progress_messages;
        Ok(())
    }

    pub async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        if self.state.in_progress_message.can_process_batch() {
            self.state
                .in_progress_message
                .process_batch(doc_processor_mailbox, ctx)
                .await?;
        } else if let Some(ready_message) = self.state.ready_messages.pop() {
            self.state.in_progress_message = ready_message
                .start_processing(&self.storage_resolver, self.source_type)
                .await?;
        } else {
            self.poll_messages(ctx).await?;
        }

        return Ok(Duration::ZERO);
    }

    pub async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let mut completed_visibility_handles = Vec::new();
        for (partition_id, position) in checkpoint.iter() {
            if position.is_eof() {
                if let Some(visibility_handle) = self.state.in_flight.remove(&partition_id) {
                    completed_visibility_handles.push(visibility_handle);
                }
                self.state.completed.insert(partition_id);
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
            "index_id": self.pipeline_id.index_uid.index_id.clone(),
            "source_id": self.pipeline_id.source_id.clone(),
            "num_bytes_processed": self.observable_state.num_bytes_processed,
            "num_messages_processed": self.observable_state.num_messages_processed,
            "num_invalid_messages": self.observable_state.num_invalid_messages,
            "num_consecutive_empty_batches": self.observable_state.num_consecutive_empty_batches,
        })
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::{ActorContext, Universe};
    use quickwit_config::{QueueMessageType, SourceConfig, SourceParams, SqsSourceParams};
    use quickwit_proto::types::IndexUid;
    use tokio::sync::watch;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::queue_sources::memory_queue::MemoryQueue;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_emit_batch() {
        let universe = Universe::with_accelerated_time();
        let in_memory_queue = Arc::new(MemoryQueue::default());
        let index_id = "test-queue-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let queue_params = QueueParams {
            message_type: QueueMessageType::S3Notification,
            deduplication_window_duration_sec: 60,
            deduplication_window_max_messages: 100,
        };
        let source_params = SourceParams::Sqs(SqsSourceParams {
            queue_url: "dummy://not-a-valid-url".to_string(),
            queue_params: queue_params.clone(),
        });
        let source_config = SourceConfig::for_test("test-queue-source", source_params);
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        {
            let mut processor = QueueProcessor::try_new(
                source_runtime.clone(),
                in_memory_queue.clone(),
                queue_params.clone(),
            )
            .await
            .unwrap();

            let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
            let (doc_processor_mailbox, doc_processor_inbox) =
                universe.create_test_mailbox::<DocProcessor>();
            let (observable_state_tx, _observable_state_rx) =
                watch::channel(serde_json::Value::Null);
            let ctx: SourceContext =
                ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

            processor
                .initialize(&doc_processor_mailbox, &ctx)
                .await
                .unwrap();

            processor
                .emit_batches(&doc_processor_mailbox, &ctx)
                .await
                .unwrap();

            let next_message = doc_processor_inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .next();
            assert!(next_message.is_none());
        }
    }
}
