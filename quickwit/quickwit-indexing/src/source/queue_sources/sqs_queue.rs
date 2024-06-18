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

use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_sqs::config::{BehaviorVersion, SharedAsyncSleep};
// use aws_sdk_sqs::error::{DisplayErrorContext, SdkError};
// use aws_sdk_sqs::operation::change_message_visibility::ChangeMessageVisibilityError;
// use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchError;
// use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;
use aws_sdk_sqs::types::{DeleteMessageBatchRequestEntry, MessageSystemAttributeName};
use aws_sdk_sqs::{Client, Config};
use itertools::Itertools;
use quickwit_aws::get_aws_config;
use quickwit_storage::OwnedBytes;

use super::message::MessageMetadata;
use super::{Queue, RawMessage};

#[derive(Debug)]
pub struct SqsQueue {
    sqs_client: Client,
    queue_url: String,
}

impl SqsQueue {
    pub async fn try_new(queue_url: String) -> anyhow::Result<Self> {
        let sqs_client = get_sqs_client().await?;
        Ok(SqsQueue {
            sqs_client,
            queue_url,
        })
    }
}

#[async_trait]
impl Queue for SqsQueue {
    async fn receive(&self) -> anyhow::Result<Vec<RawMessage>> {
        let visibility_timeout_sec = 120;
        // TODO: We estimate the message deadline using the start of the
        // ReceiveMessage request. This might be overly pessimistic: the docs
        // state that it starts when the message is returned.
        let initial_deadline = Instant::now() + Duration::from_secs(visibility_timeout_sec as u64);
        let res = self
            .sqs_client
            .receive_message()
            .queue_url(&self.queue_url)
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .wait_time_seconds(20)
            .set_max_number_of_messages(Some(1))
            .visibility_timeout(visibility_timeout_sec)
            .send()
            .await?;

        res.messages
            .unwrap_or_default()
            .into_iter()
            .map(|m| {
                let delivery_attempts: usize = m
                    .attributes
                    .as_ref()
                    .and_then(|attrs| {
                        attrs.get(&MessageSystemAttributeName::ApproximateReceiveCount)
                    })
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let ack_id = m
                    .receipt_handle
                    .context("missing receipt_handle in received message")?;
                let message_id = m
                    .message_id
                    .context("missing message_id in received message")?;
                Ok(RawMessage {
                    metadata: MessageMetadata {
                        ack_id,
                        message_id,
                        initial_deadline,
                        delivery_attempts,
                    },
                    payload: OwnedBytes::new(m.body.unwrap_or(String::new()).into_bytes()),
                })
            })
            .collect()
    }

    async fn acknowledge(&self, ack_ids: &[&str]) -> anyhow::Result<()> {
        let entry_batches: Vec<_> = ack_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                DeleteMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(id.to_string())
                    .build()
                    .unwrap()
            })
            .chunks(10)
            .into_iter()
            .map(|chunk| chunk.collect())
            .collect();

        // TODO: retries, partial success and parallelization
        let mut errors = 0;
        let num_batches = entry_batches.len();
        for batch in entry_batches {
            let res = self
                .sqs_client
                .delete_message_batch()
                .queue_url(&self.queue_url)
                .set_entries(Some(batch))
                .send()
                .await;
            if res.is_err() {
                errors += 1;
                if errors == num_batches {
                    // fail when all batches fail and return last err
                    res?;
                }
            }
        }

        Ok(())
    }

    async fn modify_deadlines(
        &self,
        ack_id: &str,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Instant> {
        let visibility_timeout = std::cmp::min(suggested_deadline.as_secs() as i32, 43200);
        let new_deadline = Instant::now() + suggested_deadline;
        // TODO: retry if transient
        self.sqs_client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .visibility_timeout(visibility_timeout)
            .receipt_handle(ack_id)
            .send()
            .await?;
        Ok(new_deadline)
    }
}

pub async fn get_sqs_client() -> anyhow::Result<Client> {
    let aws_config = get_aws_config().await;

    let mut sqs_config = Config::builder().behavior_version(BehaviorVersion::v2024_03_28());
    sqs_config.set_retry_config(aws_config.retry_config().cloned());
    sqs_config.set_credentials_provider(aws_config.credentials_provider());
    sqs_config.set_http_client(aws_config.http_client());
    sqs_config.set_timeout_config(aws_config.timeout_config().cloned());
    if let Some(identity_cache) = aws_config.identity_cache() {
        sqs_config.set_identity_cache(identity_cache);
    }
    sqs_config.set_sleep_impl(Some(SharedAsyncSleep::new(
        quickwit_aws::TokioSleep::default(),
    )));

    Ok(Client::from_conf(sqs_config.build()))
}
