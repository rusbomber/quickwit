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

use std::num::NonZeroUsize;

use anyhow::bail;
use quickwit_proto::types::SourceId;
use serde::{Deserialize, Serialize};

use super::{TransformConfig, RESERVED_SOURCE_IDS};
use crate::{
    validate_identifier, ConfigFormat, FileSourceParams, SourceConfig, SourceInputFormat,
    SourceParams,
};

type SourceConfigForSerialization = SourceConfigV0_8;

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(tag = "version")]
pub enum VersionedSourceConfig {
    #[serde(rename = "0.9")]
    #[serde(alias = "0.8")]
    V0_8(SourceConfigV0_8),
    // Retro compatibility.
    #[serde(rename = "0.7")]
    V0_7(SourceConfigV0_7),
}

impl From<VersionedSourceConfig> for SourceConfigForSerialization {
    fn from(versioned_source_config: VersionedSourceConfig) -> Self {
        match versioned_source_config {
            VersionedSourceConfig::V0_7(v0_7) => v0_7.into(),
            VersionedSourceConfig::V0_8(v0_8) => v0_8,
        }
    }
}

/// Parses and validates an [`SourceConfig`] as supplied by a user with a given [`ConfigFormat`],
/// and config content.
pub fn load_source_config_from_user_config(
    config_format: ConfigFormat,
    config_content: &[u8],
) -> anyhow::Result<SourceConfig> {
    let versioned_source_config: VersionedSourceConfig = config_format.parse(config_content)?;
    let source_config_for_serialization: SourceConfigForSerialization =
        versioned_source_config.into();
    source_config_for_serialization.validate_and_build()
}

impl SourceConfigForSerialization {
    /// Checks the validity of the `SourceConfig` as a "deserializable source".
    ///
    /// Two remarks:
    /// - This does not check connectivity, it just validate configuration, without performing any
    ///   IO. See `check_connectivity(..)`.
    /// - This is used each time the `SourceConfig` is deserialized (at creation but also during
    ///   communications with the metastore). When ingesting from stdin, we programmatically create
    ///   an invalid `SourceConfig` and only use it locally.
    fn validate_and_build(self) -> anyhow::Result<SourceConfig> {
        if !RESERVED_SOURCE_IDS.contains(&self.source_id.as_str()) {
            validate_identifier("source", &self.source_id)?;
        }
        let num_pipelines = NonZeroUsize::new(self.num_pipelines)
            .ok_or_else(|| anyhow::anyhow!("`desired_num_pipelines` must be strictly positive"))?;
        match &self.source_params {
            SourceParams::Stdin => {
                bail!(
                    "stdin can only be used as source through the CLI command `quickwit tool \
                     local-ingest`"
                );
            }
            SourceParams::File(_)
            | SourceParams::Kafka(_)
            | SourceParams::Kinesis(_)
            | SourceParams::Pulsar(_) => {
                // TODO consider any validation opportunity
            }
            SourceParams::PubSub(_)
            | SourceParams::Ingest
            | SourceParams::IngestApi
            | SourceParams::IngestCli
            | SourceParams::Vec(_)
            | SourceParams::Void(_) => {}
        }
        match &self.source_params {
            SourceParams::PubSub(_)
            | SourceParams::Kafka(_)
            | SourceParams::File(FileSourceParams::Notifications(_)) => {}
            _ => {
                if self.num_pipelines > 1 {
                    bail!("Quickwit currently supports multiple pipelines only for GCP PubSub or Kafka sources. open an issue https://github.com/quickwit-oss/quickwit/issues if you need the feature for other source types");
                }
            }
        }

        if let Some(transform_config) = &self.transform {
            if matches!(
                self.input_format,
                SourceInputFormat::OtlpLogsJson
                    | SourceInputFormat::OtlpLogsProtobuf
                    | SourceInputFormat::OtlpTracesJson
                    | SourceInputFormat::OtlpTracesProtobuf
            ) {
                bail!("VRL transforms are not supported for OTLP input formats");
            }
            transform_config.validate_vrl_script()?;
        }

        Ok(SourceConfig {
            source_id: self.source_id,
            num_pipelines,
            enabled: self.enabled,
            source_params: self.source_params,
            transform_config: self.transform,
            input_format: self.input_format,
        })
    }
}

impl From<SourceConfig> for SourceConfigV0_8 {
    fn from(source_config: SourceConfig) -> Self {
        SourceConfigV0_8 {
            source_id: source_config.source_id,
            num_pipelines: source_config.num_pipelines.get(),
            enabled: source_config.enabled,
            source_params: source_config.source_params,
            transform: source_config.transform_config,
            input_format: source_config.input_format,
        }
    }
}

impl From<SourceConfig> for VersionedSourceConfig {
    fn from(source_config: SourceConfig) -> Self {
        VersionedSourceConfig::V0_8(source_config.into())
    }
}

impl TryFrom<VersionedSourceConfig> for SourceConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_source_config: VersionedSourceConfig) -> anyhow::Result<Self> {
        let v1: SourceConfigV0_8 = versioned_source_config.into();
        v1.validate_and_build()
    }
}

fn default_max_num_pipelines_per_indexer() -> usize {
    1
}

fn default_num_pipelines() -> usize {
    1
}

fn default_source_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceConfigV0_7 {
    #[schema(value_type = String)]
    pub source_id: SourceId,

    #[serde(
        default = "default_max_num_pipelines_per_indexer",
        alias = "num_pipelines"
    )]
    pub max_num_pipelines_per_indexer: usize,

    #[serde(default = "default_num_pipelines")]
    pub desired_num_pipelines: usize,

    // Denotes if this source is enabled.
    #[serde(default = "default_source_enabled")]
    pub enabled: bool,

    #[serde(flatten)]
    pub source_params: SourceParams,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformConfig>,

    // Denotes the input data format.
    #[serde(default)]
    pub input_format: SourceInputFormat,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceConfigV0_8 {
    #[schema(value_type = String)]
    pub source_id: SourceId,

    #[serde(default = "default_num_pipelines")]
    pub num_pipelines: usize,

    // Denotes if this source is enabled.
    #[serde(default = "default_source_enabled")]
    pub enabled: bool,

    #[serde(flatten)]
    pub source_params: SourceParams,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformConfig>,

    // Denotes the input data format.
    #[serde(default)]
    pub input_format: SourceInputFormat,
}

impl From<SourceConfigV0_7> for SourceConfigV0_8 {
    fn from(source_config_v0_7: SourceConfigV0_7) -> Self {
        let SourceConfigV0_7 {
            source_id,
            max_num_pipelines_per_indexer: _,
            desired_num_pipelines,
            enabled,
            source_params,
            transform,
            input_format,
        } = source_config_v0_7;
        SourceConfigV0_8 {
            source_id,
            num_pipelines: desired_num_pipelines,
            enabled,
            source_params,
            transform,
            input_format,
        }
    }
}
