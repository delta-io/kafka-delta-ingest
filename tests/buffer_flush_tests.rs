#[allow(dead_code)]
mod helpers;

use deltalake;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serial_test::serial;
use tokio::time::{sleep, Duration};

use kafka_delta_ingest::IngestOptions;

#[tokio::test]
#[serial]
async fn test_flush_when_latency_expires() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "flush_when_latency_expires",
        json!({
            "id": "integer",
            "date": "string",
        }),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "flush_when_latency_expires".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 5000,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            kafka_brokers: helpers::test_broker(),
            ..Default::default()
        }),
    )
    .await;

    for m in create_generator(1).take(10) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    for m in create_generator(11).take(10) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    // wait for latency flush
    helpers::wait_until_version_created(&table, 2);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    let v2_rows: Vec<TestMsg> = helpers::read_table_content_as(&table).await;

    assert_eq!(v1_rows.len(), 10);
    assert_eq!(v2_rows.len(), 20);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[tokio::test]
#[serial]
async fn test_dont_write_an_empty_buffer() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "dont_write_an_empty_buffer",
        json!({
            "id": "integer",
            "date": "string",
        }),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "dont_write_an_empty_buffer".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            kafka_brokers: helpers::test_broker(),
            ..Default::default()
        }),
    )
    .await;
    // write one version so we can make sure the stream is up and running.

    for m in create_generator(1).take(10) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    // wait for the latency timer to trigger
    sleep(Duration::from_secs(6)).await;

    // verify that an empty version _was not_ created.
    // i.e. we should still be at version 1

    let t = deltalake::open_table(&table).await.unwrap();

    assert_eq!(1, t.version());

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[tokio::test]
#[serial]
async fn test_flush_on_size_without_latency_expiration() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "flush_on_size_without_latency_expiration",
        json!({
            "id": "integer",
            "date": "string",
        }),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "flush_on_size_without_latency_expiration".to_string(),
            // buffer for an hour
            allowed_latency: 3600,
            // create a record batch when we have 10 messages
            max_messages_per_batch: 10,
            // tiny buffer size for write flush
            min_bytes_per_file: 20,
            kafka_brokers: helpers::test_broker(),
            ..Default::default()
        }),
    )
    .await;

    for m in create_generator(1).take(10) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    helpers::wait_until_version_created(&table, 1);

    let data: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;

    assert_eq!(data.len(), 10);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestMsg {
    id: u64,
    date: String,
}

fn create_generator(staring_id: u64) -> impl Iterator<Item = TestMsg> {
    std::iter::successors(Some(staring_id), |n| Some(*n + 1)).map(|n| TestMsg {
        id: n,
        date: "2022-06-03".to_string(),
    })
}
