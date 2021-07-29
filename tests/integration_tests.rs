extern crate kafka_delta_ingest;

#[allow(dead_code)]
mod helpers;

use deltalake::action::Action;
use dipstick::{Input, Prefixed, Statsd};
use kafka_delta_ingest::{instrumentation::StatsHandler, KafkaJsonToDelta, Options};
use log::debug;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use rdkafka::{producer::FutureProducer, producer::FutureRecord, util::Timeout, ClientConfig};
use serde_json::json;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Once;
use std::{collections::HashMap, fs, path::PathBuf};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// NOTE: This test file depends on kafka and localstack docker services
// Run:
// `docker-compose up`
// `RUST_LOG=debug cargo test -- --nocapture`

const LOCALSTACK_ENDPOINT: &str = "http://0.0.0.0:4566";

const TEST_APP_ID: &str = "e2e_smoke_test";
const TEST_BROKER: &str = "0.0.0.0:9092";
const TEST_CONSUMER_GROUP_ID: &str = "kafka_delta_ingest";
const TEST_DELTA_TABLE_LOCATION: &str = "./tests/data/e2e_smoke_test";

static INIT: Once = Once::new();

#[tokio::test]
async fn e2e_smoke_test() {
    setup();

    // Cleanup previous test run output
    cleanup_delta_files(TEST_DELTA_TABLE_LOCATION);
    // Create a topic specific to the test run
    let topic = format!("e2e_smoke_test-{}", Uuid::new_v4());
    helpers::create_topic(topic.as_str(), 1).await;

    let start_time = chrono::Utc::now();

    //
    // Start a stream and a producer
    //

    let mut additional_kafka_settings = HashMap::new();
    additional_kafka_settings.insert("auto.offset.reset".to_string(), "earliest".to_string());

    let allowed_latency = 1500;
    let max_messages_per_batch = 2;
    let min_bytes_per_file = 370;

    let mut transforms = HashMap::new();
    transforms.insert(
        "modified_date".to_string(),
        "substr(modified, `0`, `10`)".to_string(),
    );
    transforms.insert("_kafka_offset".to_string(), "kafka.offset".to_string());
    transforms.insert(
        "_kafka_timestamp".to_string(),
        "kafka.timestamp".to_string(),
    );
    transforms.insert(
        "_kafka_timestamp_type".to_string(),
        "kafka.timestamp_type".to_string(),
    );

    let stast_scope = Statsd::send_to("localhost:8125")
        .expect("Failed to create Statsd recorder")
        .named(TEST_APP_ID)
        .metrics();
    let stats_handler = StatsHandler::new(stast_scope);
    let stats_sender = stats_handler.tx.clone();

    let opts = Options::new(
        topic.to_string(),
        TEST_DELTA_TABLE_LOCATION.to_string(),
        TEST_APP_ID.to_string(),
        allowed_latency,
        max_messages_per_batch,
        min_bytes_per_file,
    );

    let mut stream = KafkaJsonToDelta::new(
        opts,
        TEST_BROKER.to_string(),
        TEST_CONSUMER_GROUP_ID.to_string(),
        Some(additional_kafka_settings),
        transforms,
        stats_sender,
    )
    .unwrap();

    let token = CancellationToken::new();

    // Start and join a future for produce, consume
    let consume_future = stream.start(Some(&token));
    let produce_future = produce_example(topic.as_str(), &token);

    let _ = tokio::join!(consume_future, produce_future);

    //
    // Load the DeltaTable and make assertions about it
    //

    let delta_table = deltalake::open_table(TEST_DELTA_TABLE_LOCATION)
        .await
        .unwrap();

    assert_eq!(
        2, delta_table.version,
        "Version should be 2 (i.e. Exactly two transactions should be written)"
    );

    let file_paths = delta_table.get_file_uris();

    debug!("Delta log file paths {:?}", file_paths);

    assert_eq!(2, file_paths.len(), "Table should contain 2 data files");

    //
    // Reload the table and validate some data
    //

    let delta_table = deltalake::open_table(TEST_DELTA_TABLE_LOCATION)
        .await
        .unwrap();

    let files = delta_table.get_file_uris();

    let mut row_num = 0;

    for f in files.iter() {
        let p = SerializedFileReader::new(File::open(f).unwrap()).unwrap();
        let mut row_iter = p.get_row_iter(None).unwrap();

        let r = row_iter.next().unwrap();
        row_num += 1;

        assert_eq!(&row_num.to_string(), r.get_string(0).unwrap());
        assert_eq!(row_num, r.get_int(1).unwrap());
        assert_eq!("2021-03-01T14:38:58Z", r.get_string(2).unwrap());
        assert_eq!("2021-03-01", r.get_string(3).unwrap());

        let r = row_iter.next().unwrap();
        row_num += 1;

        assert_eq!(&row_num.to_string(), r.get_string(0).unwrap());
        assert_eq!(row_num, r.get_int(1).unwrap());
        assert_eq!("2021-03-01T14:38:58Z", r.get_string(2).unwrap());
        assert_eq!("2021-03-01", r.get_string(3).unwrap());
        let kafka_timestamp = r.get_timestamp_micros(4).unwrap() as i64;
        assert!(start_time.timestamp_millis() * 1000 < kafka_timestamp);
        assert!(chrono::Utc::now().timestamp_millis() * 1000 > kafka_timestamp);
        assert_eq!(0, r.get_int(5).unwrap());
    }

    // read and assert on delta log stats in first log entry
    // TODO: nested struct values are not handled yet, since parquet crate is not reporting these.

    let delta_log_location = format!("{}/_delta_log", TEST_DELTA_TABLE_LOCATION);

    let log_file = format!("{}/00000000000000000001.json", delta_log_location);
    let log_file = BufReader::new(File::open(log_file).unwrap());
    let action = log_file.lines().last().unwrap().unwrap();
    let action = action.as_str();
    let action: Action = serde_json::from_str(action).unwrap();

    let stats = match action {
        Action::add(add) => add.get_stats().unwrap().unwrap(),
        _ => panic!(),
    };

    assert_eq!(2, stats.num_records);
    assert_eq!(
        "1",
        stats.min_values["id"].as_value().unwrap().as_str().unwrap()
    );
    assert_eq!(
        1,
        stats.min_values["value"]
            .as_value()
            .unwrap()
            .as_i64()
            .unwrap()
    );
    assert_eq!(
        "2",
        stats.max_values["id"].as_value().unwrap().as_str().unwrap()
    );
    assert_eq!(
        2,
        stats.max_values["value"]
            .as_value()
            .unwrap()
            .as_i64()
            .unwrap()
    );
}

fn setup() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();
        env::set_var("AWS_ENDPOINT_URL", LOCALSTACK_ENDPOINT);
        env::set_var("AWS_ACCESS_KEY_ID", "test");
        env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    });
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
                    if extension == "json" && path.file_stem().unwrap() != "00000000000000000000" {
                        fs::remove_file(path).unwrap();
                    }
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

async fn produce_example(topic: &str, token: &CancellationToken) {
    let mut producer_config = ClientConfig::new();

    producer_config.set("bootstrap.servers", TEST_BROKER);

    let producer: FutureProducer = producer_config.create().expect("Producer creation failed");

    // first message

    debug!("Sending message 1");
    let m1 = serde_json::to_string(&json!({
        "id": "1",
        "value": 1,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let _ = producer
        .send(future_from_json(topic, &m1), Timeout::Never)
        .await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // second message

    debug!("Sending message 2");
    let m2 = serde_json::to_string(&json!({
        "id": "2",
        "value": 2,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let _ = producer
        .send(future_from_json(topic, &m2), Timeout::Never)
        .await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // third message

    debug!("Sending message 3");
    let m3 = serde_json::to_string(&json!({
        "id": "3",
        "value": 3,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let _ = producer
        .send(future_from_json(topic, &m3), Timeout::Never)
        .await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // set the cancellation token so the consume stream stops after the next message

    debug!("Setting cancellation token.");
    token.cancel();

    // fourth message

    debug!("Sending message 4");
    let m4 = serde_json::to_string(&json!({
        "id": "4",
        "value": 4,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let _ = producer
        .send(future_from_json(topic, &m4), Timeout::Never)
        .await;

    debug!("All done sending messages.");
}

fn future_from_json<'a>(topic: &'a str, message: &'a String) -> FutureRecord<'a, String, String> {
    FutureRecord::to(topic).payload(message)
}
