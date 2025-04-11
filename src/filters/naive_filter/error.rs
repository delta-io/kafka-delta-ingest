#[derive(thiserror::Error, Debug)]
pub enum NaiveFilterError {
    /// Error from [`serde_json`]
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        /// Wrapped [`serde_json::Error`]
        #[from]
        source: serde_json::Error,
    },

    /// Error occurs when trying to execute a filter
    #[error("NaiveFilter execution error: {reason}")]
    RuntimeError { reason: String },

    /// Error occurs when trying to prepare filters for execution
    #[error("NaiveFilter prepare error: {reason}")]
    PrepareError { reason: String },
}
