extern crate kafka_delta_ingest;

#[allow(dead_code)]
mod helpers;

use deltalake::action::Action;
use deltalake::delta_arrow::*;
use deltalake::checkpoints::CheckPointWriter;
use dipstick::{Input, Prefixed, Statsd};
use kafka_delta_ingest::{
    deltalake_ext::*,
    instrumentation::StatsHandler,
    KafkaJsonToDelta,
    Options,
    transforms::Transformer
};
use log::debug;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use rdkafka::{producer::FutureProducer, producer::FutureRecord, util::Timeout, ClientConfig};
use serde_json::{json, Value};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Once;
use std::{collections::HashMap, fs, path::PathBuf};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// const TEST_TABLE_URI: &str = "tests/temp/checkpoint_bug";
const TEST_TABLE_URI: &str = "tests/temp/timestamp_bug";

#[tokio::test]
async fn checkpoint_debugging_test() {
    let test_json_uri = "tests/json/web_requests-100.json";

    cleanup_delta_files(TEST_TABLE_URI);

    let mut transforms = HashMap::new();
    transforms.insert(
        "date".to_string(),
        "substr(meta.producer.timestamp, `0`, `10`)".to_string(),
    );
    transforms.insert("meta.kafka.topic".to_string(), "kafka.topic".to_string());
    transforms.insert(
        "meta.kafka.offset".to_string(),
        "kafka.offset".to_string(),
    );
    transforms.insert(
        "meta.kafka.partition".to_string(),
        "kafka.partition".to_string(),
    );
    transforms.insert(
        "meta.kafka.timestamp".to_string(),
        "kafka.timestamp".to_string(),
    );
    transforms.insert(
        "meta.kafka.timestamp_type".to_string(),
        "kafka.timestamp_type".to_string(),
    );
    let transformer = Transformer::from_transforms(&transforms).unwrap();

    let s = std::fs::read_to_string(test_json_uri).unwrap();
    let mut values: Vec<Value> = s.lines().map(|l| {
        serde_json::from_str(l).unwrap()
    }).collect();

    for (i, v) in values.iter_mut().enumerate() {
        let message = rdkafka::message::OwnedMessage::new(
            Some(v.to_string().into_bytes()),
            None,
            "test".to_string(),
            rdkafka::Timestamp::CreateTime(chrono::Utc::now().timestamp_millis()),
            0,
            i as i64,
            None
        );
        transformer.transform(v, &message).unwrap();
    }

    let chunks = values.chunks(10);

    let mut writer = DeltaWriter::for_table_path(TEST_TABLE_URI).await.unwrap();

    for chunk in chunks {
        writer.write(chunk.into()).await.unwrap();
        let adds = writer.write_parquet_files().await.unwrap();
        let mut tx = writer.table.create_transaction(None);
        tx.add_actions(adds.iter().map(|a| deltalake::action::Action::add(a.to_owned())).collect());
        let prepared = tx.prepare_commit(None).await.unwrap();
        let version = writer.table_version() + 1;
        let res = writer.table.try_commit_transaction(&prepared, version).await;
    }

    let checkpoint_writer = CheckPointWriter::new_for_table_uri(TEST_TABLE_URI).unwrap();
    checkpoint_writer.create_checkpoint_from_state(10, writer.table.get_state()).await.unwrap();

    // ROUND 2

    let chunks = values.chunks(10);

    let mut writer = DeltaWriter::for_table_path(TEST_TABLE_URI).await.unwrap();

    for chunk in chunks {
        writer.write(chunk.into()).await.unwrap();
        let adds = writer.write_parquet_files().await.unwrap();
        let mut tx = writer.table.create_transaction(None);
        tx.add_actions(adds.iter().map(|a| deltalake::action::Action::add(a.to_owned())).collect());
        let prepared = tx.prepare_commit(None).await.unwrap();
        let version = writer.table_version() + 1;
        let res = writer.table.try_commit_transaction(&prepared, version).await;
    }

    let checkpoint_writer = CheckPointWriter::new_for_table_uri(TEST_TABLE_URI).unwrap();
    checkpoint_writer.create_checkpoint_from_state(20, writer.table.get_state()).await.unwrap();
}

#[test]
fn checkpoint_parquet_test() {
    let checkpoint_file = format!("{}/_delta_log/00000000000000000010.checkpoint.parquet", TEST_TABLE_URI);
    let p = SerializedFileReader::new(File::open(checkpoint_file).unwrap()).unwrap();
    let row_group = p.metadata().row_group(0);
    let schema_descriptor = row_group.schema_descr();
    let root_schema = schema_descriptor.root_schema();

    // println!("{:#?}", schema_descriptor);
    // println!("{:#?}", root_schema);

    for r in p.get_row_iter(None).unwrap() {
        println!("{:#?}", r.to_json_value());
    }
}

fn cleanup_delta_files(table_location: &str) {
    let table_path = PathBuf::from(table_location);

    let log_dir = table_path.join("_delta_log");

    let paths = fs::read_dir(log_dir.as_path()).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let path = d.path();

                if let Some(extension) = path.extension() {
                    // Keep the staged log entry that contains the metadata with schemaString action, but delete all the rest
                    if (extension == "json" && path.file_stem().unwrap() != "00000000000000000000") || extension == "parquet" {
                        fs::remove_file(path).unwrap();
                    }
                } else if path.file_name().unwrap() == "_last_checkpoint" {
                    fs::remove_file(path).unwrap()
                }
            }
            _ => {}
        }
    }

    let paths = fs::read_dir(table_path.as_path()).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let path = d.path();
                if path.is_dir() && path.to_str().unwrap().contains("=") {
                    fs::remove_dir_all(path).unwrap();
                } else if let Some(extension) = path.extension() {
                    if extension == "parquet" {
                        fs::remove_file(path).unwrap();
                    }
                }
            }
            _ => {}
        }
    }
}