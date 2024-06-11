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

use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Queue error kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QueueErrorKind {
    /// The queue resource does not exist.
    NotFound,
    /// The request credentials do not allow for this operation.
    Unauthorized,
    /// A third-party service forbids this operation, or is misconfigured.
    Service,
    /// Any generic internal error.
    Internal,
    /// A timeout occurred during the operation.
    Timeout,
    /// Io error.
    Io,
}

impl QueueErrorKind {
    /// Creates a QueueError.
    pub fn with_error(self, source: impl Into<anyhow::Error>) -> QueueError {
        QueueError {
            kind: self,
            source: Arc::new(source.into()),
        }
    }
}

/// Generic AueueError.
#[derive(Debug, Clone, thiserror::Error)]
#[error("queue error(kind={kind:?}, source={source})")]
#[allow(missing_docs)]
pub struct QueueError {
    pub kind: QueueErrorKind,
    #[source]
    source: Arc<anyhow::Error>,
}

/// Generic Result type for queue operations.
pub type QueueResult<T> = Result<T, QueueError>;

// impl QueueError {
//     /// Add some context to the wrapper error.
//     pub fn add_context<C>(self, ctx: C) -> Self
//     where C: fmt::Display + Send + Sync + 'static {
//         QueueError {
//             kind: self.kind,
//             source: Arc::new(anyhow::anyhow!("{ctx}").context(self.source)),
//         }
//     }

//     /// Returns the corresponding `QueueErrorKind` for this error.
//     pub fn kind(&self) -> QueueErrorKind {
//         self.kind
//     }
// }
