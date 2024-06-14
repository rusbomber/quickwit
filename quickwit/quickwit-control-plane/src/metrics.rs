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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    new_counter, new_gauge, new_gauge_vec, IntCounter, IntGauge, IntGaugeVec,
};

#[derive(Debug, Clone, Copy)]
pub struct ShardLocalityMetrics {
    pub num_remote_shards: usize,
    pub num_local_shards: usize,
}

pub struct ControlPlaneMetrics {
    pub indexes_total: IntGauge,
    pub restart_total: IntCounter,
    pub schedule_total: IntCounter,
    pub metastore_error_aborted: IntCounter,
    pub metastore_error_maybe_executed: IntCounter,
    pub local_shards: IntGauge,
    pub remote_shards: IntGauge,
    // this is always modified while index_to_group is held
    open_shards_total: IntGaugeVec<1>,
    // two phase locking with no deadlock: we always acquire locks in the order of the fields.
    open_shards_per_index: Mutex<BTreeMap<String, i64>>,
    index_to_group: Mutex<BTreeMap<String, String>>,
}

impl ControlPlaneMetrics {
    pub fn set_shard_locality_metrics(&self, shard_locality_metrics: ShardLocalityMetrics) {
        self.local_shards
            .set(shard_locality_metrics.num_local_shards as i64);
        self.remote_shards
            .set(shard_locality_metrics.num_remote_shards as i64);
    }

    pub(crate) fn update_open_shard(&self, index_id: &str, new_value: i64) {
        let mut open_shards_per_index_lock = self.open_shards_per_index.lock().unwrap();
        let delta = if let Some(old_value) = open_shards_per_index_lock.get_mut(index_id) {
            let old_value_kept = *old_value;
            *old_value = new_value;
            new_value - old_value_kept
        } else {
            open_shards_per_index_lock.insert(index_id.to_string(), new_value);
            new_value
        };
        let index_to_group_lock = self.index_to_group.lock().unwrap();
        drop(open_shards_per_index_lock);
        let label = if let Some(group_name) = index_to_group_lock.get(index_id) {
            group_name
        } else {
            index_id
        };
        self.open_shards_total.with_label_values([label]).add(delta);
        drop(index_to_group_lock);
    }

    pub(crate) fn remove_index(&self, index_id: &str) {
        let mut open_shards_per_index_lock = self.open_shards_per_index.lock().unwrap();
        let previous_value = open_shards_per_index_lock.remove(index_id).unwrap_or(0);
        let mut index_to_group_lock = self.index_to_group.lock().unwrap();
        drop(open_shards_per_index_lock);
        let label = if let Some(group_name) = index_to_group_lock.remove(index_id) {
            Cow::Owned(group_name)
        } else {
            Cow::Borrowed(index_id)
        };
        // if we used the index_id, or the group now has zero shards, we assume the group is empty
        // and can be deleted
        if self.open_shards_total.with_label_values([&label]).get() <= previous_value {
            self.open_shards_total.remove_label_values([&label]);
        } else {
            self.open_shards_total
                .with_label_values([&label])
                .sub(previous_value);
        }
        drop(index_to_group_lock);
    }

    #[allow(dead_code)]
    pub(crate) fn add_index_id_to_group(&self, index_id: &str, group: Option<String>) {
        let open_shards_per_index_lock = self.open_shards_per_index.lock().unwrap();
        let current_value = *open_shards_per_index_lock.get(index_id).unwrap_or(&0);
        let new_label = group.as_deref().unwrap_or(index_id);

        let mut index_to_group_lock = self.index_to_group.lock().unwrap();
        drop(open_shards_per_index_lock);

        // set the new value before to save one clone
        // if it happends that new_label == old_label, we will successively add and remove as much.
        // Transiently the value is double what it should be (which is observable as one doesn't
        // need a lock to read the metrics), but the after this function returns, the result is
        // correct
        self.open_shards_total
            .with_label_values([new_label])
            .add(current_value);

        // replace/remove the previous group
        let old_label_opt = if let Some(group) = group {
            index_to_group_lock.insert(index_id.to_string(), group)
        } else {
            index_to_group_lock.remove(index_id)
        };
        // get the old label
        let old_label = if let Some(group_name) = old_label_opt {
            Cow::Owned(group_name)
        } else {
            Cow::Borrowed(index_id)
        };
        // remove the old metric if it looks like it became obsolet, otherwise update it
        // accordingly
        if self.open_shards_total.with_label_values([&old_label]).get() <= current_value {
            self.open_shards_total.remove_label_values([&old_label]);
        } else {
            self.open_shards_total
                .with_label_values([&old_label])
                .sub(current_value);
        }
        drop(index_to_group_lock);
    }
}

impl Default for ControlPlaneMetrics {
    fn default() -> Self {
        let shards = new_gauge_vec(
            "shards",
            "Number of (remote/local) shards in the indexing plan",
            "control_plane",
            &[],
            ["locality"],
        );
        let local_shards = shards.with_label_values(["local"]);
        let remote_shards = shards.with_label_values(["remote"]);
        ControlPlaneMetrics {
            indexes_total: new_gauge("indexes_total", "Number of indexes.", "control_plane", &[]),
            restart_total: new_counter(
                "restart_total",
                "Number of control plane restart.",
                "control_plane",
                &[],
            ),
            schedule_total: new_counter(
                "schedule_total",
                "Number of control plane `schedule` operations.",
                "control_plane",
                &[],
            ),
            metastore_error_aborted: new_counter(
                "metastore_error_aborted",
                "Number of aborted metastore transaction (= do not trigger a control plane \
                 restart)",
                "control_plane",
                &[],
            ),
            metastore_error_maybe_executed: new_counter(
                "metastore_error_maybe_executed",
                "Number of metastore transaction with an uncertain outcome (= do trigger a \
                 control plane restart)",
                "control_plane",
                &[],
            ),
            open_shards_total: new_gauge_vec(
                "open_shards_total",
                "Number of open shards per source.",
                "control_plane",
                &[],
                ["index_id"],
            ),
            local_shards,
            remote_shards,
            index_to_group: Mutex::new(BTreeMap::new()),
            open_shards_per_index: Mutex::new(BTreeMap::new()),
        }
    }
}

pub static CONTROL_PLANE_METRICS: Lazy<ControlPlaneMetrics> =
    Lazy::new(ControlPlaneMetrics::default);
