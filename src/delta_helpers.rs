use crate::{DataTypeOffset, DataTypePartition};
use deltalake::action::{Action, Add, Txn};
use deltalake::checkpoints::CheckpointError;
use deltalake::{DeltaDataTypeVersion, DeltaTable, DeltaTableError};
use std::collections::HashMap;

pub(crate) async fn load_table(
    table_uri: &str,
    options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let backend = deltalake::get_backend_for_uri_with_options(table_uri, options)?;
    let mut table = DeltaTable::new(
        table_uri,
        backend,
        deltalake::DeltaTableConfig {
            require_tombstones: true,
        },
    )?;
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
        .chain(add.drain(..).map(Action::add))
        .collect()
}

pub(crate) fn create_txn_action(txn_app_id: String, offset: DataTypeOffset) -> Action {
    Action::txn(Txn {
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
    version: DeltaDataTypeVersion,
) -> Result<(), CheckpointError> {
    if version % 10 == 0 {
        let table_version = table.version;
        // if there's new version right after current commit, then we need to reset
        // the table right back to version to create the checkpoint
        let version_updated = table_version != version;
        if version_updated {
            table.load_version(version).await?;
        }

        deltalake::checkpoints::create_checkpoint(table).await?;
        log::info!("Created checkpoint version {}.", version);

        let removed = deltalake::checkpoints::cleanup_metadata(table).await?;
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
pub(crate) fn last_txn_version(table: &DeltaTable, txn_id: &str) -> Option<DeltaDataTypeVersion> {
    table
        .get_app_transaction_version()
        .get(txn_id)
        .map(|v| v.to_owned())
}
