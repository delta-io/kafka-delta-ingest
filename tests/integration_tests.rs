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
use serde_json::json;
use std::{collections::HashMap, fs, path::PathBuf};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// NOTE: localhost:9092 kafka must be running for this test
// Also note: this test takes ~10 seconds to run
// Run with `RUST_LOG=debug cargo test -- --ignored --nocapture`

#[tokio::test]
#[ignore]
async fn e2e_smoke_test() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();

    let topic = format!("e2e_smoke_test-{}", Uuid::new_v4());
    let table_location = "./tests/data/e2e_smoke_test";

    debug!("Cleaning up old test files");

    // Cleanup previous test run output
    cleanup_delta_files(table_location);

    debug!("Creating test topic");

    // Create a topic specific to the test run
    create_topic(topic.as_str()).await;

    debug!("Created test topic");

    //
    // Start a KafkaJsonToDelta stream and a producer (which plays test messages)
    //

    // Start from earliest offsets so the consumer can handle all messages written by the test producer regardless of timing
    let mut additional_config = HashMap::new();
    additional_config.insert("auto.offset.reset".to_string(), "earliest".to_string());

    let mut transforms = HashMap::new();
    transforms.insert(
        "modified_date".to_string(),
        "substr(modified, '0', '10')".to_string(),
    );
    transforms.insert("_kafka_offset".to_string(), "kafka.offset".to_string());

    let mut stream = KafkaJsonToDelta::new(
        topic.to_string(),
        table_location.to_string(),
        "localhost:9092".to_string(),
        "kafka_delta_ingest".to_string(),
        Some(additional_config),
        60000,
        2,
        // co-ordinate expected min bytes per file with the expected bytes written by the producer to predict test output
        370,
        transforms,
    );

    let token = CancellationToken::new();

    debug!("Starting consume stream");
    let consume_future = stream.start(Some(&token));
    debug!("Starting produce stream");
    let produce_future = produce_example(topic.as_str(), std::time::Duration::from_secs(2));
    debug!("Starting pause future");
    let cancel_future =
        cancel_consumer_after_duration(Duration::from_secs(10), &token, topic.as_str());

    // Wait for the consumer and producer
    debug!("Joining futures");
    let _ = tokio::join!(consume_future, produce_future, cancel_future);

    //
    // Load the DeltaTable and make assertions about it
    //

    let delta_table = deltalake::open_table(table_location).await.unwrap();

    // assert_eq!(2, delta_table.version, "Version should be 2");

    let file_paths = delta_table.get_file_paths();

    assert_eq!(2, file_paths.len(), "Table should contain 2 data files");

    // TODO: Add additional assertions about the table
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
    admin_client_config.set("bootstrap.servers", "localhost:9092");
    let admin_client: AdminClient<_> = admin_client_config
        .create()
        .expect("AdminClient creation failed");
    let admin_options = AdminOptions::default();

    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));

    admin_client
        .create_topics(&[new_topic], &admin_options)
        .await
        .unwrap();
}

async fn produce_example(topic: &str, sleep_duration: std::time::Duration) {
    let mut producer_config = ClientConfig::new();

    producer_config.set("bootstrap.servers", "localhost:9092");

    let producer: FutureProducer = producer_config.create().expect("Producer creation failed");

    let json1 = serde_json::to_string(&json!({
        "id": "1",
        "value": 1,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let json2 = serde_json::to_string(&json!({
        "id": "2",
        "value": 2,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let json3 = serde_json::to_string(&json!({
        "id": "2",
        "value": 2,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();
    let json4 = serde_json::to_string(&json!({
        "id": "4",
        "value": 4,
        "modified": "2021-03-01T14:38:58Z",
    }))
    .unwrap();

    let messages: Vec<FutureRecord<String, String>> = vec![
        FutureRecord::to(topic).payload(&json1),
        FutureRecord::to(topic).payload(&json2),
        FutureRecord::to(topic).payload(&json3),
        FutureRecord::to(topic).payload(&json4),
    ];

    for m in messages {
        debug!("producing a message");
        let _ = producer
            .send(m, Timeout::After(std::time::Duration::from_secs(0)))
            .await;

        std::thread::sleep(sleep_duration);
    }
}

async fn cancel_consumer_after_duration(
    duration: Duration,
    token: &CancellationToken,
    topic: &str,
) {
    debug!("Sleeping");
    tokio::time::sleep(duration).await;

    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", "localhost:9092");
    let producer: FutureProducer = producer_config.create().expect("Producer creation failed");

    debug!("Cancelling stream");

    token.cancel();

    // TODO: TEMP HACK
    // Trigger the next loop iteration with an extra message so the cancellation token state is picked up. Ultimately, the KafkaJsonToDelta API needs to listen for cancellation events.

    let jsonx = serde_json::to_string(&json!({
        "id": "X",
        "value": 999,
        "modified": "2021-03-01",
    }))
    .unwrap();
    let m: FutureRecord<String, String> = FutureRecord::to(topic).payload(&jsonx);
    let _ = producer
        .send(m, Timeout::After(std::time::Duration::from_secs(0)))
        .await;
}
