#[allow(dead_code)]
mod helpers;

use log::{debug, info};
use maplit::hashmap;
use rdkafka::{producer::Producer, util::Timeout};
use serde::{Deserialize, Serialize};

// These tests are executed serially to allow for predictable rebalance waits.
// Rebalance times vary too much to produce predictable earliest/latest seek positions
// when the local kafka container is receiving concurrent requests from other tasks.
use serial_test::serial;

use kafka_delta_ingest::{IngestOptions, StartingOffsets};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestMsg {
    id: u64,
    date: String,
}

#[tokio::test]
#[serial]
async fn test_start_from_explicit() {
    helpers::init_logger();

    let table = helpers::create_local_table(
        hashmap! {
            "id" => "integer",
            "date" => "string",
        },
        vec!["date"],
    );

    let topic = format!("starting_offsets_explicit_{}", uuid::Uuid::new_v4());
    helpers::create_topic(&topic, 1).await;

    let producer = helpers::create_producer();

    // Send messages to Kafka before starting kafka-delta-ingest
    for m in create_generator(1).take(10) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    producer.flush(Timeout::Never);

    debug!("Sent test messages to Kafka");

    // Start ingest
    let (kdi, token, rt) = helpers::create_kdi(
        &topic,
        &table,
        IngestOptions {
            app_id: "starting_offsets_explicit".to_string(),
            allowed_latency: 20,
            max_messages_per_batch: 10,
            min_bytes_per_file: 10,
            starting_offsets: StartingOffsets::Explicit(hashmap! {
                // starting offset is set at 4
                0 => 4,
            }),
            ..Default::default()
        },
    );

    // Wait for the rebalance assignment
    std::thread::sleep(std::time::Duration::from_secs(3));

    // Send messages to Kafka before starting kafka-delta-ingest
    for m in create_generator(11).take(5) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    info!("Waiting for version 1");
    helpers::wait_until_version_created(&table, 1);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    let written_ids: Vec<u64> = helpers::read_table_content(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value::<TestMsg>(v.clone()).unwrap().id)
        .collect();

    assert_eq!((5u64..15).collect::<Vec<u64>>(), written_ids);
}

#[tokio::test]
#[serial]
async fn test_start_from_earliest() {
    helpers::init_logger();

    let table = helpers::create_local_table(
        hashmap! {
            "id" => "integer",
            "date" => "string",
        },
        vec!["date"],
    );

    let topic = format!("starting_offsets_earliest{}", uuid::Uuid::new_v4());
    helpers::create_topic(&topic, 3).await;

    let producer = helpers::create_producer();

    let messages: Vec<TestMsg> = create_generator(1).take(10).collect();

    // Send messages to Kafka before starting kafka-delta-ingest
    for m in messages.iter().take(15) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    // Start ingest
    let (kdi, token, rt) = helpers::create_kdi(
        &topic,
        &table,
        IngestOptions {
            app_id: "starting_offsets_earliest".to_string(),
            allowed_latency: 2,
            max_messages_per_batch: 10,
            min_bytes_per_file: 10,
            starting_offsets: StartingOffsets::Earliest,
            ..Default::default()
        },
    );

    info!("Waiting for version 1");
    helpers::wait_until_version_created(&table, 1);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    let mut written_ids: Vec<u64> = helpers::read_table_content(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value::<TestMsg>(v.clone()).unwrap().id)
        .collect();
    written_ids.sort();

    assert_eq!((1u64..11).collect::<Vec<u64>>(), written_ids);
}

#[tokio::test]
#[serial]
async fn test_start_from_latest() {
    helpers::init_logger();

    let table = helpers::create_local_table(
        hashmap! {
            "id" => "integer",
            "date" => "string",
        },
        vec!["date"],
    );

    let topic = format!("starting_offsets_latest{}", uuid::Uuid::new_v4());
    helpers::create_topic(&topic, 1).await;

    let producer = helpers::create_producer();

    // Send messages to Kafka before starting kafka-delta-ingest
    // offsets for this first set should be 0...4
    for m in create_generator(1).take(5) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    producer.flush(Timeout::Never);

    // Start ingest
    let (kdi, token, rt) = helpers::create_kdi(
        &topic,
        &table,
        IngestOptions {
            app_id: "starting_offsets_latest".to_string(),
            allowed_latency: 10,
            max_messages_per_batch: 10,
            min_bytes_per_file: 10,
            starting_offsets: StartingOffsets::Latest,
            ..Default::default()
        },
    );

    // Wait for the rebalance assignment so the position of latest is clear.
    // Precise starting offset in a production environment will depend on message rate, but, "latest is what latest is".
    std::thread::sleep(std::time::Duration::from_secs(3));

    // Send on message to trigger seek to latest
    // This skips a message to account for the seek
    for m in create_generator(6).take(1) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    // Wait for the rebalance assignment so the position of latest is clear.
    std::thread::sleep(std::time::Duration::from_secs(3));

    // These 10 messages should be in the delta log
    for m in create_generator(7).take(10) {
        info!("Writing test message");
        helpers::send_json(&producer, &topic, &serde_json::to_value(m).unwrap()).await;
    }

    info!("Waiting for version 1");
    helpers::wait_until_version_created(&table, 1);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    let mut written_ids: Vec<u64> = helpers::read_table_content(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value::<TestMsg>(v.clone()).unwrap().id)
        .collect();
    written_ids.sort();

    // ids should be 7 -16 (offsets 6-15)
    assert_eq!((7u64..17).collect::<Vec<u64>>(), written_ids);
}

fn create_generator(starting_id: u64) -> impl Iterator<Item = TestMsg> {
    std::iter::successors(Some(starting_id), |n| Some(*n + 1)).map(|n| TestMsg {
        id: n,
        date: "2021-09-25".to_string(),
    })
}
