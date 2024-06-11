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

mod error;
mod processor;
#[cfg(feature = "sqs")]
mod sqs_queue;
#[cfg(feature = "sqs")]
mod sqs_source;
mod visibility;

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use error::QueueResult;
use quickwit_common::uri::Uri;
use quickwit_config::QueueMessageType;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_storage::OwnedBytes;
use serde_json::Value;

pub struct QueueMessageMetadata {
    /// The handle that should be used to acknowledge the message or change its visibility deadline
    pub ack_id: String,

    /// The unique message id assigned by the queue
    pub message_id: String,

    /// The approximate number of times the message was delivered. 1 means it is
    /// the first time this message is being delivered.
    pub delivery_attempts: usize,

    /// The first deadline when the message is received. It can be extended later using the ack_id.
    pub initial_deadline: Instant,
}

pub struct QueueMessage {
    pub metadata: QueueMessageMetadata,
    pub payload: OwnedBytes,
}

#[async_trait]
pub trait Queue: fmt::Debug + Send + Sync + 'static {
    /// Poll the queue to receive messages.
    async fn receive(&self) -> QueueResult<Vec<QueueMessage>>;

    /// Try to acknowledge the messages, effectively deleting them from the queue.
    ///
    /// The call might return `Ok(())` yet fail partially:
    /// - if it's a transient failure? -> TODO check
    /// - if the message was already acknowledged
    async fn acknowledge(&self, ack_id: &[&str]) -> QueueResult<()>;

    /// Modify the visibility deadline of the messages.
    ///
    /// We try to set the initial visibility large enough to avoid having to
    /// call this too often. The implementation can retry as long as desired,
    /// it's the caller's responsibility to cancel the task if the deadline is
    /// about to expire. The returned `Instant` is a conservative estimate of
    /// the new deadline expiration time.
    async fn modify_deadlines(
        &self,
        ack_id: &str,
        suggested_deadline: Duration,
    ) -> QueueResult<Instant>;
}

pub enum PrepocessedPayload {
    ObjectUri(Uri),
    // RawData(OwnedBytes),
}

pub struct PrepocessedQueueMessage {
    pub payload: PrepocessedPayload,
    pub metadata: QueueMessageMetadata,
}

impl PrepocessedQueueMessage {
    pub fn try_from(message_type: QueueMessageType, message: QueueMessage) -> anyhow::Result<Self> {
        let res = match message_type {
            QueueMessageType::S3Notification => {
                PrepocessedPayload::ObjectUri(Self::uri_from_s3_notification(message.payload)?)
            }
        };
        Ok(PrepocessedQueueMessage {
            payload: res,
            metadata: message.metadata,
        })
    }

    pub fn partition_id(&self) -> PartitionId {
        match &self.payload {
            PrepocessedPayload::ObjectUri(uri) => uri.to_string().into(),
        }
    }

    fn uri_from_s3_notification(message: OwnedBytes) -> anyhow::Result<Uri> {
        let value: Value = serde_json::from_slice(message.as_slice())?;
        let key = value["Records"][0]["s3"]["object"]["key"]
            .as_str()
            .context("Invalid S3 notification")?;
        let bucket = value["Records"][0]["s3"]["bucket"]["name"]
            .as_str()
            .context("Invalid S3 notification")?;
        Uri::from_str(&format!("s3://{}/{}", bucket, key))
    }
}
