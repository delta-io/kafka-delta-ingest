use jmespatch::JmespathError;
use serde_json;

use crate::filters::naive_filter::error::NaiveFilterError;

/// Errors returned by filters
#[derive(thiserror::Error, Debug)]
pub enum FilterError {
    /// Failed compile expression
    #[error("Failed compile expression: {source}")]
    CompileExpressionError {
        /// Wrapped [JmespathError]
        source: JmespathError,
    },

    /// Message does not match filter pattern
    #[error("Can't filter message: {source}")]
    JmespathError {
        /// Wrapped [JmespathError]
        #[from]
        source: JmespathError,
    },

    /// NaiveFilterError
    #[error("NaiveFilter failure: {source}")]
    NaiveFilterError {
        /// Wrapped [`crate::filters::naive_filter::error::NaiveFilterError`]
        #[from]
        source: NaiveFilterError,
    },

    /// Error from [`serde_json`]
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        /// Wrapped [`serde_json::Error`]
        #[from]
        source: serde_json::Error,
    },

    /// Not found filter engine
    #[error("Not found filter engine: {reason}")]
    NotFound {
        /// 
        reason: String
    },

    /// Error returned for skipping message
    #[error("Skipped a message by filter")]
    FilterSkipMessage,
}
