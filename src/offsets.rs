use crate::delta_helpers::*;
use crate::{DataTypeOffset, DataTypePartition};
use deltalake_core::kernel::Action;
use deltalake_core::operations::transaction::TableReference;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::protocol::OutputMode;
use deltalake_core::{DeltaTable, DeltaTableError};
use log::{error, info};

/// Errors returned by `write_offsets_to_delta` function.
#[derive(thiserror::Error, Debug)]
pub enum WriteOffsetsError {
    /// Error returned when stored offsets in delta table are lower than provided seek offsets.
    #[error("Stored offsets are lower than provided: {0}")]
    InconsistentStoredOffsets(String),

    /// Error from [`deltalake::DeltaTable`]
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// Wrapped [`deltalake::DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },
}

/// Write provided seeking offsets as a new delta log version with a set of `txn` actions.
/// The `txn` id for each partition is constructed as `<app_id>-<partition>` and used across
/// kafka-delta-ingest to track messages offsets and protect from data duplication.
///
/// However, if table has already stored offset for given app_id/partition then this action could
/// be ignored if stored offsets are equals or greater than provided seeking offsets.
/// But, if stored offsets are lower then the `InconsistentStoredOffsets` is returned since it
/// could break the data integrity.
/// Hence, one is advised to supply the new `app_id` if skipping offsets is what required.
pub(crate) async fn write_offsets_to_delta(
    table: &mut DeltaTable,
    app_id: &str,
    offsets: &[(DataTypePartition, DataTypeOffset)],
) -> Result<(), WriteOffsetsError> {
    let offsets_as_str: String = offsets
        .iter()
        .map(|(p, o)| format!("{}:{}", p, o))
        .collect::<Vec<String>>()
        .join(",");

    info!("Writing offsets [{}]", offsets_as_str);

    let mapped_offsets: Vec<(String, DataTypeOffset)> = offsets
        .iter()
        .map(|(p, o)| (txn_app_id_for_partition(app_id, *p), *o))
        .collect();

    if is_safe_to_commit_transactions(table, &mapped_offsets) {
        // table has no stored offsets for given app_id/partitions so it is safe to write txn actions
        commit_partition_offsets(table, mapped_offsets, &offsets_as_str, app_id.to_owned()).await?;
        Ok(())
    } else {
        // there's at least one app_id/partition stored in delta,
        // checking whether it's safe to proceed further
        let mut conflict_offsets = Vec::new();

        for (txn_app_id, offset) in mapped_offsets {
            match table.get_app_transaction_version().get(&txn_app_id) {
                Some(stored_offset) if stored_offset.version < offset => {
                    conflict_offsets.push((txn_app_id, stored_offset.version, offset));
                }
                _ => (),
            }
        }

        if conflict_offsets.is_empty() {
            // there's no conflicted offsets in delta, e.g. it's either missing or is higher than seek offset
            info!("The provided offsets are already applied.");
            Ok(())
        } else {
            let partitions = conflict_offsets
                .iter()
                .map(|p| p.0.split('-').last().unwrap_or("N/A"))
                .collect::<Vec<&str>>()
                .join(",");

            error!(
                "Stored offsets for partitions [{}] are lower than seek offsets.",
                partitions
            );

            let detailed_error_msg = conflict_offsets
                .iter()
                .map(|(partition, stored, provided)| {
                    format!("{}:stored={}/seek={}", partition, stored, provided)
                })
                .collect::<Vec<String>>()
                .join(", ");

            Err(WriteOffsetsError::InconsistentStoredOffsets(format!(
                "[{}]",
                detailed_error_msg
            )))
        }
    }
}

async fn commit_partition_offsets(
    table: &mut DeltaTable,
    offsets: Vec<(String, DataTypeOffset)>,
    offsets_as_str: &str,
    app_id: String,
) -> Result<(), DeltaTableError> {
    let actions: Vec<Action> = offsets
        .iter()
        .map(|(txn_id, offset)| create_txn_action(txn_id.to_string(), *offset))
        .collect();
    let epoch_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64;

    table.update().await?;
    let commit = deltalake_core::operations::transaction::CommitBuilder::default()
        .with_actions(actions)
        .build(
            table.state.as_ref().map(|s| s as &dyn TableReference),
            table.log_store().clone(),
            DeltaOperation::StreamingUpdate {
                output_mode: OutputMode::Complete,
                query_id: app_id,
                epoch_id,
            },
        )
        .await
        .map_err(DeltaTableError::from);
    match commit {
        Ok(v) => {
            info!(
                "Delta version {} completed with new txn offsets {}.",
                v.version, offsets_as_str
            );
            Ok(())
        }
        Err(e) => match e {
            DeltaTableError::VersionAlreadyExists(_) => {
                error!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing", crate::DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                Err(e)
            }
            _ => Err(e),
        },
    }
}

fn is_safe_to_commit_transactions(
    table: &DeltaTable,
    offsets: &[(String, DataTypeOffset)],
) -> bool {
    offsets
        .iter()
        .all(|(id, _)| !table.get_app_transaction_version().contains_key(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use uuid::Uuid;

    const VERSION_0: &str = r#"{"commitInfo":{"timestamp":1564524295023,"operation":"CREATE TABLE","operationParameters":{"isManaged":"false","description":null,"partitionBy":"[]","properties":"{}"},"isBlindAppend":true}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"22ef18ba-191c-4c36-a606-3dad5cdf3830","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1564524294376}}
"#;

    #[tokio::test]
    async fn write_offsets_to_delta_test() {
        env_logger::init();

        let mut table = create_table().await;

        let offsets = vec![(0, 5), (1, 10)];

        // Test successful write
        assert_eq!(table.version(), 0);
        write_offsets_to_delta(&mut table, "test", &offsets)
            .await
            .unwrap();

        // verify that txn action is written
        table.update().await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(
            table
                .get_app_transaction_version()
                .get("test-0")
                .unwrap()
                .version,
            5
        );
        assert_eq!(
            table
                .get_app_transaction_version()
                .get("test-1")
                .unwrap()
                .version,
            10
        );

        // Test ignored write
        write_offsets_to_delta(&mut table, "test", &offsets)
            .await
            .unwrap();

        // verify that txn action is not written
        table.update().await.unwrap();
        assert_eq!(table.version(), 1);

        // Test failed write (lower stored offsets)
        let offsets = vec![(0, 15)];
        let err = write_offsets_to_delta(&mut table, "test", &offsets)
            .await
            .err()
            .unwrap();
        let err = format!("{:?}", err);

        assert_eq!(
            err.as_str(),
            "InconsistentStoredOffsets(\"[test-0:stored=5/seek=15]\")"
        );

        std::fs::remove_dir_all(table.table_uri()).unwrap();
    }

    async fn create_table() -> DeltaTable {
        let table_path = format!("./tests/gen/table-{}", Uuid::new_v4());
        let v0_path = format!("{}/_delta_log/00000000000000000000.json", &table_path);
        std::fs::create_dir_all(Path::new(&v0_path).parent().unwrap()).unwrap();
        std::fs::write(&v0_path, VERSION_0).unwrap();
        deltalake_core::open_table(&table_path).await.unwrap()
    }
}
