use crate::{DataTypeOffset, DataTypePartition};
use deltalake::action::{Action, Add, Txn};
use std::collections::HashMap;

pub(crate) fn build_actions(
    partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    app_id: &str,
    mut add: Vec<Add>,
) -> Vec<Action> {
    partition_offsets
        .iter()
        .map(|(partition, offset)| {
            Action::txn(Txn {
                app_id: txn_app_id_for_partition(app_id, *partition),
                version: *offset,
                last_updated: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                ),
            })
        })
        .chain(add.drain(..).map(Action::add))
        .collect()
}

pub(crate) fn txn_app_id_for_partition(app_id: &str, partition: DataTypePartition) -> String {
    format!("{}-{}", app_id, partition)
}
