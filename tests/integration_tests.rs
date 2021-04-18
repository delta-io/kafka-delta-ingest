#[macro_use]
extern crate maplit;

extern crate kafka_delta_ingest;

use kafka_delta_ingest::KafkaJsonToDelta;
use log::debug;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::FutureProducer,
    producer::FutureRecord,
    util::Timeout,
    ClientConfig,
};
use rusoto_core::Region;
use rusoto_dynamodb::*;
use serde_json::json;
use std::env;
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
    create_topic(topic.as_str()).await;
    // Cleanup the write ahead log
    cleanup_write_ahead_log().await;

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

    let mut stream = KafkaJsonToDelta::new(
        topic.to_string(),
        TEST_DELTA_TABLE_LOCATION.to_string(),
        TEST_BROKER.to_string(),
        TEST_CONSUMER_GROUP_ID.to_string(),
        Some(additional_kafka_settings),
        TEST_APP_ID.to_string(),
        allowed_latency,
        max_messages_per_batch,
        min_bytes_per_file,
        transforms,
    )
    .unwrap();

    let token = CancellationToken::new();

    // Start and join a future for produce, consume and cancel
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

    let file_paths = delta_table.get_file_paths();

    debug!("Delta log file paths {:?}", file_paths);

    assert_eq!(2, file_paths.len(), "Table should contain 2 data files");

    // TODO: Add additional assertions about the table
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

async fn create_topic(topic: &str) {
    let mut admin_client_config = ClientConfig::new();
    admin_client_config.set("bootstrap.servers", TEST_BROKER);

    let admin_client: AdminClient<_> = admin_client_config
        .create()
        .expect("AdminClient creation failed");
    let admin_options = AdminOptions::default();
    let num_partitions = 1;
    let new_topic = NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1));

    admin_client
        .create_topics(&[new_topic], &admin_options)
        .await
        .unwrap();
}

async fn cleanup_write_ahead_log() {
    let client = DynamoDbClient::new(Region::Custom {
        name: "custom".to_string(),
        endpoint: LOCALSTACK_ENDPOINT.to_string(),
    });

    for n in 1i32..=2i32 {
        let _ = client
            .delete_item(DeleteItemInput {
                key: hashmap! {
                    "transaction_id".to_string() => AttributeValue {
                        n: Some(n.to_string()),
                        ..Default::default()
                    },
                },
                table_name: TEST_APP_ID.to_string(),
                ..Default::default()
            })
            .await;
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
