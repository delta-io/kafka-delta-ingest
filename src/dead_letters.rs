use async_trait::async_trait;
use chrono::prelude::*;
use core::fmt::Debug;
use log::{error, info};
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{deltalake_ext::*, transforms::TransformError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetter {
    pub base64_bytes: Option<String>,
    pub json_string: Option<String>,
    pub error: Option<String>,
    pub timestamp: String,
    pub date: String,
}

impl DeadLetter {
    pub fn from_failed_deserialization(bytes: &[u8], err: serde_json::Error) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: Some(base64::encode(bytes)),
            json_string: None,
            error: Some(err.to_string()),
            timestamp: timestamp.to_rfc3339(),
            date: timestamp.date().to_string(),
        }
    }

    pub fn from_failed_transform(value: &Value, err: TransformError) -> Self {
        let timestamp = Utc::now();
        match serde_json::to_string(value) {
            Ok(s) => Self {
                base64_bytes: None,
                json_string: Some(s),
                error: Some(err.to_string()),
                timestamp: timestamp.to_rfc3339(),
                date: timestamp.date().to_string(),
            },
            _ => unreachable!(),
        }
    }

    pub fn from_failed_parquet_row(value: &Value, err: ParquetError) -> Self {
        let timestamp = Utc::now();
        match serde_json::to_string(value) {
            Ok(s) => Self {
                base64_bytes: None,
                json_string: Some(s),
                error: Some(err.to_string()),
                timestamp: timestamp.to_rfc3339(),
                date: timestamp.date().to_string(),
            },
            _ => unreachable!(),
        }
    }

    pub fn vec_from_failed_parquet_rows(failed: Vec<(Value, ParquetError)>) -> Vec<Self> {
        failed
            .iter()
            .map(|(v, e)| Self::from_failed_parquet_row(v, e.to_owned()))
            .collect()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeadLetterQueueError {
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        #[from]
        source: serde_json::Error,
    },

    #[error("Delta write failed: {source}")]
    DeltaWriter {
        #[from]
        source: DeltaWriterError,
    },
}

pub struct DeadLetterQueueOptions {
    /// Table URI of the delta table to write dead letters to. Implies usage of the DeltaSinkDeadLetterQueue.
    pub delta_table_uri: Option<String>,
}

#[async_trait]
pub trait DeadLetterQueue: Send + Sync {
    async fn write_dead_letter(
        &mut self,
        dead_letter: DeadLetter,
    ) -> Result<(), DeadLetterQueueError> {
        self.write_dead_letters(vec![dead_letter]).await
    }

    async fn write_dead_letters(
        &mut self,
        dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError>;
}

pub async fn dlq_from_opts(
    options: DeadLetterQueueOptions,
) -> Result<Box<dyn DeadLetterQueue>, DeadLetterQueueError> {
    if let Some(table_uri) = options.delta_table_uri {
        Ok(Box::new(
            DeltaSinkDeadLetterQueue::for_table_uri(table_uri.as_str()).await?,
        ))
    } else {
        Ok(Box::new(NoopDeadLetterQueue {}))
    }
}

pub struct NoopDeadLetterQueue {}

#[async_trait]
impl DeadLetterQueue for NoopDeadLetterQueue {
    async fn write_dead_letters(
        &mut self,
        _dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError> {
        // noop
        Ok(())
    }
}

pub struct LoggingDeadLetterQueue {}

#[async_trait]
impl DeadLetterQueue for LoggingDeadLetterQueue {
    async fn write_dead_letters(
        &mut self,
        dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError> {
        for dead_letter in dead_letters {
            info!("DeadLetter: {:?}", dead_letter);
        }

        Ok(())
    }
}

pub struct DeltaSinkDeadLetterQueue {
    delta_writer: DeltaWriter,
}

impl DeltaSinkDeadLetterQueue {
    pub async fn for_table_uri(table_uri: &str) -> Result<Self, DeadLetterQueueError> {
        Ok(Self {
            delta_writer: DeltaWriter::for_table_path(table_uri).await?,
        })
    }
}

#[async_trait]
impl DeadLetterQueue for DeltaSinkDeadLetterQueue {
    async fn write_dead_letters(
        &mut self,
        dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError> {
        let values: Result<Vec<Value>, _> = dead_letters
            .iter()
            .map(|dl| serde_json::to_value(dl))
            .collect();
        let values = values?;

        info!("Starting insert_all");
        let version = self.delta_writer.insert_all(values).await?;

        // TODO: take opt for checkpoint creation
        if version % 10 == 0 {
            // TODO: create checkpoint on every 10th version
        }

        info!("Completed insert_all");

        Ok(())
    }
}
