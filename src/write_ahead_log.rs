use deltalake::DeltaDataTypeVersion;
use log::{debug, error, info, warn};
use rdkafka::{error::KafkaError, Offset, TopicPartitionList};
use std::collections::HashMap;

use crate::DataTypeOffset;
use crate::DataTypePartition;
use crate::DataTypeTransactionId;

#[derive(thiserror::Error, Debug)]
pub enum WriteAheadLogError {}

pub enum TransactionState {
    Prepared,
    Completed,
    Failed,
}

pub struct WriteAheadLogEntry {
    pub transaction_id: DataTypeTransactionId,
    pub transaction_state: TransactionState,
    pub delta_table_version: Option<DeltaDataTypeVersion>,
    pub owner: String,
    pub partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
}

impl WriteAheadLogEntry {
    pub fn new(
        transaction_id: DataTypeTransactionId,
        transaction_state: TransactionState,
        delta_table_version: Option<DeltaDataTypeVersion>,
        owner: String,
        partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Self {
        Self {
            transaction_id,
            transaction_state,
            delta_table_version,
            owner,
            partition_offsets,
        }
    }

    pub fn prepare_next(
        &self,
        owner: String,
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
            delta_table_version: None,
            owner,
            partition_offsets,
        }
    }
}

pub struct WriteAheadLog {
    consumer_id: String,
}

impl WriteAheadLog {
    pub fn new(consumer_id: String) -> Self {
        Self { consumer_id }
    }

    pub fn complete_entry(
        &self,
        transaction_id: DataTypeTransactionId,
        delta_table_version: DeltaDataTypeVersion,
    ) -> Result<(), WriteAheadLogError> {
        todo!();
    }

    pub fn fail_entry(
        &self,
        transaction_id: DataTypeTransactionId,
    ) -> Result<(), WriteAheadLogError> {
        todo!();
    }

    pub fn get_last_completed_entry(
        &self,
    ) -> Result<Option<WriteAheadLogEntry>, WriteAheadLogError> {
        todo!();
    }

    pub fn prepare_entry(
        &self,
        updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Result<WriteAheadLogEntry, WriteAheadLogError> {
        let previous_entry = self.get_last_completed_entry()?;

        let entry = match previous_entry {
            Some(entry) => entry.prepare_next(self.consumer_id.clone(), updated_offsets),
            _ => WriteAheadLogEntry::new(
                1, // assume first record
                TransactionState::Prepared,
                None,
                self.consumer_id.clone(),
                updated_offsets.to_owned(),
            ),
        };

        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_with_updates() {
        let partition_offsets = vec![(0, 1), (1, 3), (3, 2)];
        let partition_offsets: HashMap<DataTypePartition, DataTypeOffset> =
            partition_offsets.into_iter().collect();

        let wal_entry = WriteAheadLogEntry::new(
            1,
            TransactionState::Completed,
            Some(1),
            String::from("abc"),
            partition_offsets,
        );

        let updates = vec![(2, 2), (3, 4), (4, 1)];
        let updates: HashMap<DataTypePartition, DataTypeOffset> = updates.into_iter().collect();

        let next_wal_entry = wal_entry.prepare_next(String::from("abc"), &updates);

        assert_eq!(2, next_wal_entry.transaction_id);
        assert_eq!(None, next_wal_entry.delta_table_version);
        assert_eq!(String::from("abc"), next_wal_entry.owner);

        let expected_offsets = vec![(0, 1), (1, 3), (2, 2), (3, 4), (4, 1)];
        let expected_offsets: HashMap<DataTypePartition, DataTypeOffset> =
            expected_offsets.into_iter().collect();
        assert_eq!(expected_offsets, next_wal_entry.partition_offsets);
    }
}
