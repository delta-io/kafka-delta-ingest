#[allow(dead_code)]
mod helpers;

use deltalake_core::kernel::{Action, Add};
use deltalake_core::operations::transaction::TableReference;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::DeltaTableError;
use kafka_delta_ingest::writer::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
struct TestMsg {
    id: u32,
    color: Option<String>,
}

impl TestMsg {
    fn new(id: u32, color: &str) -> Self {
        Self {
            id,
            color: Some(color.to_string()),
        }
    }

    fn new_color_null(id: u32) -> Self {
        Self { id, color: None }
    }
}

#[tokio::test]
async fn test_delta_partitions() {
    let table_path = helpers::create_local_table(
        json!({
            "id": "integer",
            "color": "string",
        }),
        vec!["color"],
        "test_delta_partitions",
    );

    let table = deltalake_core::open_table(&table_path).await.unwrap();
    let mut delta_writer = DataWriter::for_table(&table, HashMap::new()).unwrap();

    let batch1 = vec![
        TestMsg::new(1, "red"),
        TestMsg::new(2, "red"),
        TestMsg::new(3, "blue"),
        TestMsg::new(4, "red"),
    ];

    let batch2 = vec![
        TestMsg::new(5, "blue"),
        TestMsg::new(6, "red"),
        TestMsg::new(7, "blue"),
        TestMsg::new(8, "blue"),
        TestMsg::new_color_null(9),
    ];

    delta_writer.write(msgs_to_values(batch1)).await.unwrap();
    delta_writer.write(msgs_to_values(batch2)).await.unwrap();

    let result = delta_writer
        .write_parquet_files(&table.table_uri())
        .await
        .unwrap();

    for add in result.iter() {
        match add
            .partition_values
            .get("color")
            .unwrap()
            .clone()
            .as_deref()
        {
            Some("red") => {
                assert!(add.path.starts_with("color=red"));
                assert_eq!(&get_stats_value(add, "numRecords"), "4");
                assert_eq!(msg(get_stats_value(add, "minValues")).id, 1);
                assert_eq!(msg(get_stats_value(add, "maxValues")).id, 6);
            }
            Some("blue") => {
                assert!(add.path.starts_with("color=blue"));
                assert_eq!(&get_stats_value(add, "numRecords"), "4");
                assert_eq!(msg(get_stats_value(add, "minValues")).id, 3);
                assert_eq!(msg(get_stats_value(add, "maxValues")).id, 8);
            }
            None => {
                assert!(add.path.starts_with("color=__HIVE_DEFAULT_PARTITION__"));
                assert_eq!(&get_stats_value(add, "numRecords"), "1");
                assert_eq!(msg(get_stats_value(add, "minValues")).id, 9);
                assert_eq!(msg(get_stats_value(add, "maxValues")).id, 9);
            }
            other => {
                panic!("{:?}", other);
            }
        }
    }

    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };

    let version = deltalake_core::operations::transaction::CommitBuilder::default()
        .with_actions(result.iter().cloned().map(Action::Add).collect())
        .build(
            table.state.as_ref().map(|s| s as &dyn TableReference),
            table.log_store().clone(),
            operation,
        )
        .await
        .map_err(DeltaTableError::from)
        .expect("Failed to create transaction")
        .version;

    deltalake_core::checkpoints::create_checkpoint(&table)
        .await
        .unwrap();

    let table = deltalake_core::open_table(&table_path).await.unwrap();
    assert_eq!(table.version(), version);

    std::fs::remove_dir_all(&table_path).unwrap();
}

fn msgs_to_values(values: Vec<TestMsg>) -> Vec<Value> {
    values
        .iter()
        .map(|j| serde_json::to_value(j).unwrap())
        .collect()
}

fn get_stats_value(add: &Add, key: &str) -> String {
    let v: Value = serde_json::from_str(add.stats.as_ref().unwrap()).unwrap();
    v.as_object().unwrap().get(key).unwrap().to_string()
}

fn msg(s: String) -> TestMsg {
    serde_json::from_str(&s).unwrap()
}
