use async_trait::async_trait;
use deltalake::DeltaDataTypeVersion;
use log::error;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::DataTypeOffset;
use crate::DataTypePartition;
use crate::DataTypeTransactionId;

// NOTE: For now, dynamodb is the only implementation of the WriteAheadLog trait.
// Add feature flags when we have another.

mod dynamodb;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum WriteAheadLogError {
    #[error("Transaction not found: {0}")]
    TransactionNotFound(DataTypeTransactionId),

    #[error("Invalid transaction state for put entry: {0}")]
    InvalidTransactionStateForPutEntry(TransactionState),

    #[error("The write ahead log entry with transaction id {0} is corrupted in storage")]
    CorruptedEntry(DataTypeTransactionId),

    #[error("Dynamo error: {0}")]
    Dynamo(#[from] dynamodb::DynamoError),
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionState {
    Prepared,
    Completed,
    Aborted,
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            TransactionState::Prepared => "Prepared",
            TransactionState::Completed => "Completed",
            TransactionState::Aborted => "Aborted",
        };

        write!(f, "{}", s)
    }
}

#[derive(Debug)]
pub struct WriteAheadLogEntry {
    pub transaction_id: DataTypeTransactionId,
    pub transaction_state: TransactionState,
    pub table_version: Option<DeltaDataTypeVersion>,
    pub partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
}

impl WriteAheadLogEntry {
    pub fn new(
        transaction_id: DataTypeTransactionId,
        transaction_state: TransactionState,
        delta_table_version: Option<DeltaDataTypeVersion>,
        partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Self {
        Self {
            transaction_id,
            transaction_state,
            table_version: delta_table_version,
            partition_offsets,
        }
    }

    pub fn prepare_next(
        &self,
        updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Self {
        let mut partition_offsets = self.partition_offsets.clone();

        for (k, v) in updated_offsets {
            let offset = v.clone();
            *partition_offsets.entry(*k).or_insert(offset) = offset;
        }

        Self {
            transaction_id: self.transaction_id + 1,
            transaction_state: TransactionState::Prepared,
            table_version: None,
            partition_offsets,
        }
    }
}

#[async_trait]
pub trait WriteAheadLog {
    async fn get_entry_by_transaction_id(
        &self,
        transaction_id: DataTypeTransactionId,
    ) -> Result<WriteAheadLogEntry, WriteAheadLogError>;

    async fn put_entry(&self, entry: &WriteAheadLogEntry) -> Result<(), WriteAheadLogError>;

    async fn complete_entry(
        &self,
        transaction_id: DataTypeTransactionId,
        delta_table_version: DeltaDataTypeVersion,
    ) -> Result<(), WriteAheadLogError>;

    async fn abort_entry(
        &self,
        transaction_id: DataTypeTransactionId,
    ) -> Result<(), WriteAheadLogError>;
}

pub async fn new_write_ahead_log(
    log_name: String,
) -> Result<Arc<dyn WriteAheadLog + Sync + Send>, WriteAheadLogError> {
    // TODO: For now - dynamodb is the only backend for the write ahead log.
    // We will need to update this factory fn as other implementations are added.

    let wal = dynamodb::DynamoDbWriteAheadLog::new(log_name);

    Ok(Arc::new(wal))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_with_updates() {
        let partition_offsets = vec![(0, 1), (1, 3), (3, 2)];
        let partition_offsets: HashMap<DataTypePartition, DataTypeOffset> =
            partition_offsets.into_iter().collect();

        let wal_entry =
            WriteAheadLogEntry::new(1, TransactionState::Completed, Some(1), partition_offsets);

        let updates = vec![(2, 2), (3, 4), (4, 1)];
        let updates: HashMap<DataTypePartition, DataTypeOffset> = updates.into_iter().collect();

        let next_wal_entry = wal_entry.prepare_next(&updates);

        assert_eq!(2, next_wal_entry.transaction_id);
        assert_eq!(None, next_wal_entry.table_version);

        let expected_offsets = vec![(0, 1), (1, 3), (2, 2), (3, 4), (4, 1)];
        let expected_offsets: HashMap<DataTypePartition, DataTypeOffset> =
            expected_offsets.into_iter().collect();
        assert_eq!(expected_offsets, next_wal_entry.partition_offsets);
    }
}
