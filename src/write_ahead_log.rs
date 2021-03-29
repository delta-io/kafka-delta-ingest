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

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum WriteAheadLogError {
    #[error("Transaction not found: {0}")]
    TransactionNotFound(DataTypeTransactionId),

    #[error("The write ahead log entry with transaction id {0} is corrupted in storage")]
    CorruptedEntry(DataTypeTransactionId),

    #[error("Dynamo error: {0}")]
    Dynamo(#[from] dynamodb::DynamoError),
}

#[derive(Debug, PartialEq)]
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

mod dynamodb {
    use super::{
        DataTypeOffset, DataTypePartition, DataTypeTransactionId, DeltaDataTypeVersion,
        TransactionState, WriteAheadLog, WriteAheadLogEntry, WriteAheadLogError,
    };
    use async_trait::async_trait;
    use log::debug;
    use rusoto_core::{Region, RusotoError};
    use rusoto_dynamodb::*;
    use std::collections::HashMap;

    #[derive(thiserror::Error, Debug, PartialEq)]
    pub enum DynamoError {
        #[error("Put item error: {source}")]
        PutItemError {
            #[from]
            source: RusotoError<PutItemError>,
        },

        #[error("Update item error: {source}")]
        UpdateItemError {
            #[from]
            source: RusotoError<UpdateItemError>,
        },

        #[error("Get item error: {source}")]
        GetItemError {
            #[from]
            source: RusotoError<GetItemError>,
        },
    }

    pub struct DynamoDbWriteAheadLog {
        table_name: String,
        client: DynamoDbClient,
    }

    impl DynamoDbWriteAheadLog {
        pub fn new(table_name: String) -> Self {
            let region = if let Ok(url) = std::env::var("AWS_ENDPOINT_URL") {
                Region::Custom {
                    name: "custom".to_string(),
                    endpoint: url,
                }
            } else {
                Region::default()
            };
            let client = DynamoDbClient::new(region);
            Self { table_name, client }
        }
    }

    #[async_trait]
    impl WriteAheadLog for DynamoDbWriteAheadLog {
        async fn get_entry_by_transaction_id(
            &self,
            transaction_id: DataTypeTransactionId,
        ) -> Result<WriteAheadLogEntry, WriteAheadLogError> {
            debug!("Getting entry with transaction id {}", transaction_id);

            let output = self
                .client
                .get_item(get_item_input(self.table_name.clone(), transaction_id))
                .await
                .map_err(|e| get_item_error(e))?;

            let item = output
                .item
                .ok_or_else(|| WriteAheadLogError::TransactionNotFound(transaction_id))?;

            debug!("{:?}", item);

            let transaction_id = item
                .get(constants::TRANSACTION_ID_FIELD)
                .and_then(|f| f.n.as_ref())
                .ok_or_else(|| WriteAheadLogError::CorruptedEntry(transaction_id))?
                .parse::<DataTypeTransactionId>()
                .unwrap();

            let transaction_state = item
                .get(constants::TRANSACTION_STATE_FIELD)
                .and_then(|f| f.s.as_ref())
                .ok_or_else(|| WriteAheadLogError::CorruptedEntry(transaction_id))?;

            let transaction_state = match transaction_state.as_str() {
                "Prepared" => Ok(TransactionState::Prepared),
                "Completed" => Ok(TransactionState::Completed),
                "Aborted" => Ok(TransactionState::Aborted),
                _ => Err(WriteAheadLogError::CorruptedEntry(transaction_id)),
            }?;

            let mut partition_offsets: HashMap<DataTypePartition, DataTypeOffset> = HashMap::new();
            let stored_offsets = item
                .get(constants::PARTITION_OFFSETS_FIELD)
                .and_then(|f| f.m.as_ref())
                .ok_or_else(|| WriteAheadLogError::CorruptedEntry(transaction_id))?;

            for (k, v) in stored_offsets.iter() {
                let p = k
                    .parse::<DataTypePartition>()
                    .map_err(|_| WriteAheadLogError::CorruptedEntry(transaction_id))?;
                let o =
                    v.n.as_ref()
                        .ok_or_else(|| WriteAheadLogError::CorruptedEntry(transaction_id))?
                        .parse::<DataTypeOffset>()
                        .map_err(|_| WriteAheadLogError::CorruptedEntry(transaction_id))?;

                partition_offsets.insert(p, o);
            }

            Ok(WriteAheadLogEntry {
                transaction_id,
                transaction_state,
                partition_offsets,
                table_version: None,
            })
        }

        async fn put_entry(&self, entry: &WriteAheadLogEntry) -> Result<(), WriteAheadLogError> {
            debug!("Putting entry {:?}", entry);

            self.client
                .put_item(put_item_input(self.table_name.clone(), entry))
                .await
                .map_err(|e| put_item_error(e))?;

            Ok(())
        }

        async fn complete_entry(
            &self,
            transaction_id: DataTypeTransactionId,
            table_version: DeltaDataTypeVersion,
        ) -> Result<(), WriteAheadLogError> {
            debug!("Completing entry with id {:?}", transaction_id);

            self.client
                .update_item(update_item_input(
                    self.table_name.clone(),
                    transaction_id,
                    TransactionState::Completed,
                    Some(table_version),
                ))
                .await
                .map_err(|e| update_item_error(e))?;

            Ok(())
        }

        async fn abort_entry(
            &self,
            transaction_id: DataTypeTransactionId,
        ) -> Result<(), WriteAheadLogError> {
            debug!("Aborting entry with id {:?}", transaction_id);

            self.client
                .update_item(update_item_input(
                    self.table_name.clone(),
                    transaction_id,
                    TransactionState::Aborted,
                    None,
                ))
                .await
                .map_err(|e| update_item_error(e))?;

            Ok(())
        }
    }

    fn put_item_error(err: RusotoError<PutItemError>) -> DynamoError {
        DynamoError::PutItemError { source: err }
    }

    fn get_item_error(err: RusotoError<GetItemError>) -> DynamoError {
        DynamoError::GetItemError { source: err }
    }

    fn update_item_error(err: RusotoError<UpdateItemError>) -> DynamoError {
        DynamoError::UpdateItemError { source: err }
    }

    mod constants {
        pub const TRANSACTION_ID_FIELD: &str = "transaction_id";
        pub const TRANSACTION_STATE_FIELD: &str = "transaction_state";
        pub const TABLE_VERSION_FIELD: &str = "table_version";
        pub const PARTITION_OFFSETS_FIELD: &str = "partition_offsets";
    }

    mod expressions {
        use super::constants::*;

        pub fn is_in_state(transaction_state_field: &str) -> String {
            format!(
                "#{} = :{}",
                TRANSACTION_STATE_FIELD, transaction_state_field
            )
        }

        pub fn update_state_and_table_version() -> String {
            format!(
                "SET #{} = :{}, #{} = :{}",
                TRANSACTION_STATE_FIELD,
                TRANSACTION_STATE_FIELD,
                TABLE_VERSION_FIELD,
                TABLE_VERSION_FIELD
            )
        }

        pub fn update_state() -> String {
            format!(
                "SET #{} = :{}",
                TRANSACTION_STATE_FIELD, TRANSACTION_STATE_FIELD
            )
        }
    }

    fn path_for_field(field: &str) -> String {
        format!("#{}", field)
    }

    fn param_for_field(field: &str) -> String {
        format!(":{}", field)
    }

    fn get_item_input(table_name: String, transaction_id: DataTypeTransactionId) -> GetItemInput {
        GetItemInput {
            table_name,
            consistent_read: Some(true),
            key: hashmap! {
                constants::TRANSACTION_ID_FIELD.to_string() => AttributeValue {
                    n: Some(transaction_id.to_string()),
                    ..Default::default()
                }
            },
            ..Default::default()
        }
    }

    fn put_item_input(table_name: String, wal_entry: &WriteAheadLogEntry) -> PutItemInput {
        let item = hashmap! {
            constants::TRANSACTION_ID_FIELD.to_string() => AttributeValue {
                n: Some(wal_entry.transaction_id.to_string()),
                ..Default::default()
            },
            constants::TRANSACTION_STATE_FIELD.to_string() => AttributeValue {
                s: Some(wal_entry.transaction_state.to_string()),
                ..Default::default()
            },
            constants::PARTITION_OFFSETS_FIELD.to_string() => AttributeValue {
                m: Some(wal_entry.partition_offsets.iter().map(|(k,v)| {
                    (k.to_string(), AttributeValue {
                        n: Some(v.to_string()),
                        ..Default::default()
                    })
                }).collect()),
                ..Default::default()
            },
        };

        PutItemInput {
            table_name,
            item,
            ..Default::default()
        }
    }

    fn update_item_input(
        table_name: String,
        transaction_id: DataTypeTransactionId,
        new_transaction_state: TransactionState,
        table_version: Option<DeltaDataTypeVersion>,
    ) -> UpdateItemInput {
        let existing_transaction_state_field = "existing_transaction_state";

        let mut names = hashmap! {
            path_for_field(constants::TRANSACTION_STATE_FIELD) => constants::TRANSACTION_STATE_FIELD.to_string(),
        };
        let mut values = hashmap! {
            param_for_field(constants::TRANSACTION_STATE_FIELD) => AttributeValue {
                s: Some(new_transaction_state.to_string()),
                ..Default::default()
            },
            param_for_field(existing_transaction_state_field) => AttributeValue {
                s: Some(TransactionState::Prepared.to_string()),
                ..Default::default()
            },
        };

        if let Some(table_version) = table_version {
            names.insert(
                path_for_field(constants::TABLE_VERSION_FIELD),
                constants::TABLE_VERSION_FIELD.to_string(),
            );
            values.insert(
                param_for_field(constants::TABLE_VERSION_FIELD),
                AttributeValue {
                    n: Some(table_version.to_string()),
                    ..Default::default()
                },
            );
        }

        let update_expression = if let Some(_) = table_version {
            expressions::update_state_and_table_version()
        } else {
            expressions::update_state()
        };

        UpdateItemInput {
            table_name,
            key: hashmap! {
                constants::TRANSACTION_ID_FIELD.to_string() => AttributeValue {
                    n: Some(transaction_id.to_string()),
                    ..Default::default()
                },
            },
            update_expression: Some(update_expression),
            condition_expression: Some(expressions::is_in_state(existing_transaction_state_field)),
            expression_attribute_names: Some(names),
            expression_attribute_values: Some(values),
            ..Default::default()
        }
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
