#[macro_use]
extern crate maplit;

#[allow(dead_code)]
mod helpers;

use deltalake::action::Add;
use kafka_delta_ingest::deltalake_ext::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
struct TestMsg {
    id: u32,
    color: String,
}

impl TestMsg {
    fn new(id: u32, color: &str) -> Self {
        Self {
            id,
            color: color.to_string(),
        }
    }
}

#[tokio::test]
async fn test_delta_partitions() {
    let table_path = helpers::create_local_table(
        hashmap! {
            "id" => "integer",
            "color" => "string",
        },
        vec!["color"],
    );

    let mut delta_writer = DeltaWriter::for_table_path(&table_path).await.unwrap();

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
    ];

    delta_writer.write(msgs_to_values(batch1)).await.unwrap();
    delta_writer.write(msgs_to_values(batch2)).await.unwrap();

    let result = delta_writer.write_parquet_files().await.unwrap();

    for add in result {
        let p = add.partition_values.get("color").unwrap().clone().unwrap();

        if p == "red" {
            assert!(add.path.starts_with("color=red"));
            assert_eq!(&get_stats_value(&add, "numRecords"), "4");
            assert_eq!(msg(get_stats_value(&add, "minValues")).id, 1);
            assert_eq!(msg(get_stats_value(&add, "maxValues")).id, 6);
        } else if p == "blue" {
            assert!(add.path.starts_with("color=blue"));
            assert_eq!(&get_stats_value(&add, "numRecords"), "4");
            assert_eq!(msg(get_stats_value(&add, "minValues")).id, 3);
            assert_eq!(msg(get_stats_value(&add, "maxValues")).id, 8);
        } else {
            panic!("{}", p);
        }
    }

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
