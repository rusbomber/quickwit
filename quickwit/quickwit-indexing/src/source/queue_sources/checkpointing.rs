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

use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, OpenShardSubrequest, OpenShardsRequest,
};
use quickwit_proto::types::{Position, ShardId};

pub struct QueueCheckpointing {
    pub metastore: MetastoreServiceClient,
}

impl QueueCheckpointing {
    /// Tries to acquire the shards associated with the given partitions.
    /// Partitions that are assumed to be processed by another indexing
    /// pipeline are filtered out.
    pub async fn acquire_shards(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        partitions: Vec<PartitionId>,
    ) -> anyhow::Result<Vec<(PartitionId, Position)>> {
        let local_publish_token = pipeline_id.pipeline_uid.to_string();
        let open_shard_subrequests = partitions
            .iter()
            .enumerate()
            .map(|(idx, partition_id)| OpenShardSubrequest {
                subrequest_id: idx as u32,
                index_uid: Some(pipeline_id.index_uid.clone()),
                source_id: pipeline_id.source_id.clone(),
                // TODO: make this optional?
                leader_id: String::new(),
                follower_id: None,
                shard_id: Some(ShardId::from(partition_id.as_str())),
                publish_token: Some(local_publish_token.clone()),
            })
            .collect();

        let open_shard_resp = self
            .metastore
            .open_shards(OpenShardsRequest {
                subrequests: open_shard_subrequests,
            })
            .await
            .unwrap();

        let new_shards = open_shard_resp
            .subresponses
            .iter()
            .filter(|sub| sub.open_shard().publish_token.as_ref() == Some(&local_publish_token))
            .map(|sub| {
                Ok((
                    partitions[sub.subrequest_id as usize].clone(),
                    sub.open_shard().publish_position_inclusive(),
                ))
            });

        // TODO: Add logic to re-acquire shards that have a token that is not
        // the local token but haven't been updated recently

        new_shards.collect()
    }
}
