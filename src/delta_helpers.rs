use crate::{DataTypeOffset, DataTypePartition};
use deltalake_core::kernel::{Action, Add, Transaction};
use deltalake_core::{DeltaTable, DeltaTableError};
use std::collections::HashMap;

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
        table.load_version(version).await?;

        deltalake_core::checkpoints::create_checkpoint(table, None).await?;
        log::info!("Created checkpoint version {}.", version);

        let removed = deltalake_core::checkpoints::cleanup_metadata(table, None).await?;
        if removed > 0 {
            log::info!("Metadata cleanup, removed {} obsolete logs.", removed);
        }

        // An incremental update starts after the loaded version and would skip the
        // checkpoint just written, so drop the snapshot and reload from it (#227).
        table.state = None;
        table.update().await?;
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
    use deltalake_core::kernel::transaction::{CommitBuilder, TableReference};
    use deltalake_core::protocol::{DeltaOperation, OutputMode};

    const V0_COMMIT: &str = concat!(
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        "\n",
        r#"{"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":0}}"#,
    );

    /// Returns how many record batches the table's EagerSnapshot holds for its file list.
    fn snapshot_batch_count(table: &DeltaTable) -> usize {
        // EagerSnapshot serializes as [snapshot, tracked_actions, transactions, batch, ...].
        let value = serde_json::to_value(table.snapshot().unwrap().snapshot()).unwrap();
        value.as_array().unwrap().len() - 3
    }

    /// Commits one add action the way `complete_file` does: update, commit, then checkpoint.
    async fn commit_one_file(table: &mut DeltaTable, n: i64) -> i64 {
        table.update().await.unwrap();
        let add = Add {
            path: format!("part-{n:05}.parquet"),
            size: 100,
            modification_time: n,
            data_change: true,
            ..Default::default()
        };
        let version = CommitBuilder::default()
            .with_actions(vec![Action::Add(add)])
            .build(
                table.state.as_ref().map(|s| s as &dyn TableReference),
                table.log_store().clone(),
                DeltaOperation::StreamingUpdate {
                    output_mode: OutputMode::Append,
                    query_id: "test".to_string(),
                    epoch_id: n,
                },
            )
            .await
            .unwrap()
            .version;
        try_create_checkpoint(table, version).await.unwrap();
        version
    }

    #[tokio::test]
    async fn checkpoint_resets_snapshot_file_batches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_path = temp_dir.path().join("_delta_log");
        std::fs::create_dir(&log_path).unwrap();
        std::fs::write(log_path.join("00000000000000000000.json"), V0_COMMIT).unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        let mut table = load_table(table_uri, HashMap::new()).await.unwrap();
        for n in 1..=30 {
            let version = commit_one_file(&mut table, n).await;
            if version % 10 == 0 {
                let fresh = load_table(table_uri, HashMap::new()).await.unwrap();
                assert_eq!(
                    snapshot_batch_count(&table),
                    snapshot_batch_count(&fresh),
                    "version {version}: a long-lived table should hold as many file batches as a fresh load right after a checkpoint",
                );
            }
        }
    }
}
