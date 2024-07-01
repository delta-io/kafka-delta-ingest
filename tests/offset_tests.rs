use deltalake_core::protocol::Stats;
use deltalake_core::DeltaTable;
use log::*;
use rdkafka::{producer::Producer, util::Timeout};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serial_test::serial;
use uuid::Uuid;

use std::path::Path;

use kafka_delta_ingest::{AutoOffsetReset, IngestOptions};
#[allow(dead_code)]
mod helpers;

#[derive(Debug, Serialize, Deserialize)]
struct TestMsg {
    id: u64,
    color: String,
}

impl TestMsg {
    fn new(id: u64) -> Self {
        Self {
            id,
            color: "default".to_string(),
        }
    }
}

#[tokio::test]
async fn zero_offset_issue() {
    let table = "./tests/data/zero_offset";
    helpers::init_logger();
    let topic = format!("zero_offset_issue_{}", Uuid::new_v4());

    helpers::create_topic(&topic, 1).await;

    let (kdi, token, rt) = helpers::create_kdi(
        &topic,
        table,
        IngestOptions {
            app_id: "zero_offset".to_string(),
            allowed_latency: 5,
            max_messages_per_batch: 1,
            min_bytes_per_file: 20,
            ..Default::default()
        },
    );

    {
        // check that there's only 1 record in table
        let table = deltalake_core::open_table(table).await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(count_records(table), 1);
    }

    let producer = helpers::create_producer();

    // submit 3 messages in kafka, but only 2nd and 3rd should go in as msg 0:0 already in delta
    for i in 0..3 {
        helpers::send_json(
            &producer,
            &topic,
            &serde_json::to_value(TestMsg::new(i)).unwrap(),
        )
        .await;
    }

    let v2 = Path::new("./tests/data/zero_offset/_delta_log/00000000000000000002.json");
    let v3 = Path::new("./tests/data/zero_offset/_delta_log/00000000000000000003.json");

    helpers::wait_until_file_created(v2);
    helpers::wait_until_file_created(v3);
    token.cancel();
    // if it succeeds then it means that we successfully seeked into offset 0, e.g. Offset::Beginning
    kdi.await.unwrap();
    rt.shutdown_background();

    // check that there's only 3 records
    let table = deltalake_core::open_table(table).await.unwrap();
    assert_eq!(table.version(), 3);
    assert_eq!(count_records(table), 3);

    //cleanup
    std::fs::remove_file(v2).unwrap();
    std::fs::remove_file(v3).unwrap();
}

fn count_records(table: DeltaTable) -> i64 {
    let mut count = 0;

    if let Ok(adds) = table.state.unwrap().file_actions() {
        for add in adds.iter() {
            if let Some(stats) = add.stats.as_ref() {
                // as of deltalake-core 0.18.0 get_stats_parsed() only returns data when loaded
                // from checkpoints so manual parsing is necessary
                let stats: Stats = serde_json::from_str(stats).unwrap_or(Stats::default());
                count += stats.num_records;
            }
        }
    }
    count
}

#[tokio::test]
#[serial]
async fn test_start_from_explicit() {
    helpers::init_logger();

    let table = helpers::create_local_table(
        json!({
            "id": "integer",
            "color": "string",
        }),
        vec!["color"],
        "starting_offsets_explicit",
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
            seek_offsets: Some(vec![(0, 3)]), // starting offset is goign to be 4
            ..Default::default()
        },
    );

    // Wait for the rebalance assignment
    std::thread::sleep(std::time::Duration::from_secs(8));

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

    let written_ids: Vec<u64> = helpers::read_table_content_as_jsons(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value::<TestMsg>(v.clone()).unwrap().id)
        .collect();

    assert_eq!((5u64..15).collect::<Vec<u64>>(), written_ids);

    helpers::cleanup_kdi(&topic, &table).await;
}

#[tokio::test]
#[serial]
async fn test_start_from_earliest() {
    helpers::init_logger();

    let table = helpers::create_local_table(
        json!({
            "id": "integer",
            "color": "string",
        }),
        vec!["color"],
        "starting_offsets_earliest",
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
            allowed_latency: 10,
            max_messages_per_batch: 10,
            min_bytes_per_file: 10,
            auto_offset_reset: AutoOffsetReset::Earliest,
            ..Default::default()
        },
    );

    info!("Waiting for version 1");
    helpers::wait_until_version_created(&table, 1);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    let mut written_ids: Vec<u64> = helpers::read_table_content_as_jsons(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value::<TestMsg>(v.clone()).unwrap().id)
        .collect();
    written_ids.sort();

    assert_eq!((1u64..11).collect::<Vec<u64>>(), written_ids);

    helpers::cleanup_kdi(&topic, &table).await;
}

#[tokio::test]
#[serial]
async fn test_start_from_latest() {
    helpers::init_logger();

    let table = helpers::create_local_table(
        json! ({
            "id": "integer",
            "color": "string",
        }),
        vec!["color"],
        "starting_offsets_latest",
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
            auto_offset_reset: AutoOffsetReset::Latest,
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
    std::thread::sleep(std::time::Duration::from_secs(8));

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

    let mut written_ids: Vec<u64> = helpers::read_table_content_as_jsons(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value::<TestMsg>(v.clone()).unwrap().id)
        .collect();
    written_ids.sort();

    // ids should be 7 -16 (offsets 6-15)
    assert_eq!((7u64..17).collect::<Vec<u64>>(), written_ids);

    helpers::cleanup_kdi(&topic, &table).await;
}

fn create_generator(starting_id: u64) -> impl Iterator<Item = TestMsg> {
    std::iter::successors(Some(starting_id), |n| Some(*n + 1)).map(|n| TestMsg {
        id: n,
        color: "red".to_string(),
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct Msg {
    id: u32,
    city: String,
}

impl Msg {
    fn new(id: u32) -> Self {
        Self {
            id,
            city: "default".to_string(),
        }
    }
}

#[tokio::test]
async fn end_at_initial_offsets() {
    helpers::init_logger();
    let topic = format!("end_at_offset_{}", Uuid::new_v4());

    let table = helpers::create_local_table(
        json!({
            "id": "integer",
            "city": "string",
        }),
        vec!["city"],
        &topic,
    );
    let table = table.as_str();

    helpers::create_topic(&topic, 3).await;

    let producer = helpers::create_producer();
    // submit 15 messages in kafka
    for i in 0..15 {
        helpers::send_json(
            &producer,
            &topic,
            &serde_json::to_value(Msg::new(i)).unwrap(),
        )
        .await;
    }

    let (kdi, _token, rt) = helpers::create_kdi(
        &topic,
        table,
        IngestOptions {
            app_id: topic.clone(),
            allowed_latency: 5,
            max_messages_per_batch: 20,
            min_bytes_per_file: 20,
            end_at_last_offsets: true,
            ..Default::default()
        },
    );

    helpers::wait_until_version_created(table, 1);

    {
        // check that there's 3 records in table
        let table = deltalake_core::open_table(table).await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(count_records(table), 15);
    }

    // messages in kafka
    for i in 16..31 {
        helpers::send_json(
            &producer,
            &topic,
            &serde_json::to_value(Msg::new(i)).unwrap(),
        )
        .await;
    }

    helpers::expect_termination_within(kdi, 10).await;
    rt.shutdown_background();

    // check that there's only 3 records
    let table = deltalake_core::open_table(table).await.unwrap();
    assert_eq!(table.version(), 1);
    assert_eq!(count_records(table), 15);
}
