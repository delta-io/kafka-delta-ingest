use crate::{DataTypeOffset, DataTypePartition};
use deltalake_core::kernel::transaction::{CommitBuilder, TableReference};
use deltalake_core::kernel::{Action, Add, Transaction};
use deltalake_core::protocol::{DeltaOperation, OutputMode};
use deltalake_core::{DeltaTable, DeltaTableError};
use std::collections::HashMap;

/// Commits `actions` as one append-mode `StreamingUpdate` and returns the new version.
///
/// Callers get a single attempt, with no retry loop here, because delta-rs `CommitBuilder`
/// already retries when the log has moved past the snapshot in `table`. It fails with
/// `CommitConflictError::ConcurrentTransaction` when a newer commit holds a `txn` for an app
/// id in `actions`. Retrying that conflict from the same snapshot fails the same way every
/// time, so the caller has to reload the table and re-seek instead.
pub(crate) async fn commit_streaming_append(
    table: &DeltaTable,
    query_id: &str,
    actions: Vec<Action>,
) -> Result<i64, DeltaTableError> {
    let epoch_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64;
    let commit = CommitBuilder::default()
        .with_actions(actions)
        .build(
            table.state.as_ref().map(|s| s as &dyn TableReference),
            table.log_store().clone(),
            DeltaOperation::StreamingUpdate {
                output_mode: OutputMode::Append,
                query_id: query_id.to_string(),
                epoch_id,
            },
        )
        .await?;
    Ok(commit.version)
}

pub(crate) async fn load_table(
    table_uri: &str,
    options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let mut table = deltalake_core::open_table_with_storage_options(table_uri, options).await?;
    table.load().await?;
    Ok(table)
}

pub(crate) fn build_actions(
    partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    app_id: &str,
    mut add: Vec<Add>,
) -> Vec<Action> {
    partition_offsets
        .iter()
        .map(|(partition, offset)| {
            create_txn_action(txn_app_id_for_partition(app_id, *partition), *offset)
        })
        .chain(add.drain(..).map(Action::Add))
        .collect()
}

pub(crate) fn create_txn_action(txn_app_id: String, offset: DataTypeOffset) -> Action {
    Action::Txn(Transaction {
        app_id: txn_app_id,
        version: offset,
        last_updated: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        ),
    })
}

pub(crate) async fn try_create_checkpoint(
    table: &mut DeltaTable,
    version: i64,
) -> Result<(), DeltaTableError> {
    if version % 10 == 0 {
        let table_version = table.version();
        // if there's new version right after current commit, then we need to reset
        // the table right back to version to create the checkpoint
        let version_updated = table_version != version;
        if version_updated {
            table.load_version(version).await?;
        }

        deltalake_core::checkpoints::create_checkpoint(table, None).await?;
        log::info!("Created checkpoint version {}.", version);

        let removed = deltalake_core::checkpoints::cleanup_metadata(table, None).await?;
        if removed > 0 {
            log::info!("Metadata cleanup, removed {} obsolete logs.", removed);
        }

        if version_updated {
            table.update().await?;
        }
    }
    Ok(())
}

pub(crate) fn txn_app_id_for_partition(app_id: &str, partition: DataTypePartition) -> String {
    format!("{}-{}", app_id, partition)
}

/// Returns the last transaction version for the given transaction id recorded in the delta table.
pub(crate) fn last_txn_version(table: &DeltaTable, txn_id: &str) -> Option<i64> {
    table
        .get_app_transaction_version()
        .get(txn_id)
        .map(|t| t.version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake_core::kernel::transaction::{CommitConflictError, TransactionError};

    const VERSION_0: &str = r#"{"commitInfo":{"timestamp":1564524295023,"operation":"CREATE TABLE","operationParameters":{"isManaged":"false","description":null,"partitionBy":"[]","properties":"{}"},"isBlindAppend":true}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"22ef18ba-191c-4c36-a606-3dad5cdf3830","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1564524294376}}
"#;

    fn create_table(dir: &tempfile::TempDir) -> String {
        let log_dir = dir.path().join("_delta_log");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(log_dir.join("00000000000000000000.json"), VERSION_0).unwrap();
        dir.path().to_str().unwrap().to_string()
    }

    async fn open(table_uri: &str) -> DeltaTable {
        deltalake_core::open_table(table_uri).await.unwrap()
    }

    /// What one worker commits for one partition: its `txn` plus one data file.
    fn partition_commit(partition: DataTypePartition, offset: DataTypeOffset) -> Vec<Action> {
        vec![
            create_txn_action(txn_app_id_for_partition("app", partition), offset),
            Action::Add(Add {
                path: format!("part-{partition}-{offset}.parquet"),
                size: 1,
                data_change: true,
                ..Default::default()
            }),
        ]
    }

    #[tokio::test]
    async fn commit_streaming_append_lands_after_commit_for_other_partition() {
        let dir = tempfile::tempdir().unwrap();
        let table_uri = create_table(&dir);
        // Both workers load the table at version 0.
        let worker_a = open(&table_uri).await;
        let worker_b = open(&table_uri).await;

        let version = commit_streaming_append(&worker_a, "app", partition_commit(0, 5))
            .await
            .unwrap();
        assert_eq!(version, 1);

        // worker_b's snapshot is stale. delta-rs finds version 1 taken and commits at version 2.
        let version = commit_streaming_append(&worker_b, "app", partition_commit(1, 7))
            .await
            .unwrap();
        assert_eq!(version, 2);

        let table = open(&table_uri).await;
        assert_eq!(last_txn_version(&table, "app-0"), Some(5));
        assert_eq!(last_txn_version(&table, "app-1"), Some(7));
    }

    #[tokio::test]
    async fn commit_streaming_append_rejects_commit_for_same_partition() {
        let dir = tempfile::tempdir().unwrap();
        let table_uri = create_table(&dir);
        let worker_a = open(&table_uri).await;
        let worker_b = open(&table_uri).await;

        commit_streaming_append(&worker_a, "app", partition_commit(0, 5))
            .await
            .unwrap();

        // worker_b still thinks partition 0 has no offset and tries to write it too.
        let result = commit_streaming_append(&worker_b, "app", partition_commit(0, 9)).await;
        assert!(
            matches!(
                result,
                Err(DeltaTableError::Transaction {
                    source: TransactionError::CommitConflict(
                        CommitConflictError::ConcurrentTransaction
                    )
                })
            ),
            "expected ConcurrentTransaction, got {result:?}"
        );

        // Nothing from worker_b reached the log.
        let table = open(&table_uri).await;
        assert_eq!(table.version(), 1);
        assert_eq!(last_txn_version(&table, "app-0"), Some(5));
    }
}
