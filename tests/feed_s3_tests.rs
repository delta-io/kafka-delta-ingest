#[allow(dead_code)]
mod helpers;

use kafka_delta_ingest::IngestOptions;
use maplit::hashmap;
use rdkafka::producer::FutureProducer;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestMsg {
    id: u64,
    text: String,
    feed: String,
    timestamp: String,
}

const TOTAL_MESSAGES_SENT: usize = 600;
// see seek offsets param, we skip 5 messages in each partition, e,g. 12 x 5
const TOTAL_MESSAGES_RECEIVED: usize = TOTAL_MESSAGES_SENT - 60;
const FEEDS: usize = 3;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn feed_load_test() {
    helpers::init_logger();

    let topic = format!("feed_load_{}", uuid::Uuid::new_v4());

    let table = helpers::create_local_table(
        json!({
            "id": "integer",
            "text": "string",
            "feed": "string",
            "timestamp": "timestamp",
            "date": "string",
            "kafka": {
                "offset": "integer",
                "timestamp": "timestamp",
                "timestamp_type": "integer",
            }
        }),
        vec!["feed"],
        &topic,
    );

    helpers::create_topic(&topic, 12).await;

    let producer = Arc::new(helpers::create_producer());

    // send message in parallel
    let f_a = spawn_send_jsons("A", 0, producer.clone(), &topic);
    let f_b = spawn_send_jsons("B", 1, producer.clone(), &topic);
    let f_c = spawn_send_jsons("C", 2, producer.clone(), &topic);

    let mut workers = Vec::new();

    workers.push(spawn_worker(1, &topic, &table));
    std::thread::sleep(Duration::from_secs(10));
    workers.push(spawn_worker(2, &topic, &table));
    std::thread::sleep(Duration::from_secs(10));
    workers.push(spawn_worker(3, &topic, &table));

    let mut feed_a_ids = f_a.await.unwrap();
    let mut feed_b_ids = f_b.await.unwrap();
    let mut feed_c_ids = f_c.await.unwrap();

    wait_until_all_messages_received(&table).await;
    workers.iter().for_each(|w| w.1.cancel());

    for (kdi, _, rt) in workers {
        println!("wait on worker..");
        kdi.await.unwrap();
        rt.shutdown_background();
    }

    println!("verifying results...");

    let values = helpers::read_table_content_as_jsons(&table).await;
    let mut ids: Vec<i64> = values
        .iter()
        .map(|v| v.as_object().unwrap().get("id").unwrap().as_i64().unwrap())
        .collect();
    ids.sort();

    let id_count = ids.len();
    let expected = (0..TOTAL_MESSAGES_RECEIVED).count();

    if id_count != expected {
        helpers::inspect_table(&table).await;
    }
    assert_eq!(id_count, expected);

    let mut expected = Vec::new();
    expected.append(&mut feed_a_ids);
    expected.append(&mut feed_b_ids);
    expected.append(&mut feed_c_ids);
    expected.sort();

    assert_eq!(ids, expected);

    // verify transforms
    let m = values.first().unwrap().as_object().unwrap();
    assert!(m.contains_key("date"));
    let now = Utc::now().to_rfc3339();
    assert_eq!(m.get("date").unwrap().as_str().unwrap(), &now[..10]);
    assert!(m.contains_key("kafka"));
    let kafka = m.get("kafka").unwrap().as_object().unwrap();
    assert!(kafka.contains_key("offset"));
    assert!(kafka.contains_key("timestamp"));
    assert!(kafka.contains_key("timestamp_type"));

    helpers::cleanup_kdi(&topic, &table).await;
}

fn spawn_worker(
    id: usize,
    topic: &str,
    table: &str,
) -> (JoinHandle<()>, Arc<CancellationToken>, Runtime) {
    let transforms = hashmap! {
         "date" => "substr(timestamp, `0`, `10`)",
         "kafka.offset" => "kafka.offset",
         "kafka.timestamp" => "kafka.timestamp",
         "kafka.timestamp_type" => "kafka.timestamp_type",
    };
    let transforms = transforms
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    helpers::create_kdi_with(
        topic,
        table,
        Some(format!("WORKER-{}", id)),
        IngestOptions {
            app_id: "feed".to_string(),
            allowed_latency: 3,
            max_messages_per_batch: 30,
            min_bytes_per_file: 100,
            transforms,
            seek_offsets: Some(vec![
                (0, 4),
                (1, 4),
                (2, 4),
                (3, 4),
                (4, 4),
                (5, 4),
                (6, 4),
                (7, 4),
                (8, 4),
                (9, 4),
                (10, 4),
                (11, 4),
            ]),
            additional_kafka_settings: Some(hashmap! {
                "auto.offset.reset".to_string() => "earliest".to_string(),
            }),
            ..Default::default()
        },
    )
}

fn spawn_send_jsons(
    feed: &str,
    feed_n: usize,
    producer: Arc<FutureProducer>,
    topic: &str,
) -> JoinHandle<Vec<i64>> {
    let feed = feed.to_string();
    let topic = topic.to_string();
    tokio::spawn(async move { send_jsons(feed.as_str(), feed_n, producer, topic).await })
}
async fn send_jsons(
    feed: &str,
    feed_n: usize,
    producer: Arc<FutureProducer>,
    topic: String,
) -> Vec<i64> {
    let feed = feed.to_string();
    let mut sent = Vec::new();
    for id in 0..TOTAL_MESSAGES_SENT {
        let topic = topic.clone();
        if id % FEEDS == feed_n {
            let m = json!({
                "id": id,
                "text": format!("{}-{}-{}",Uuid::new_v4(),Uuid::new_v4(),Uuid::new_v4()),
                "feed": feed,
                "timestamp": Utc::now().to_rfc3339(),
            });
            let (_, o) = helpers::send_kv_json(&producer, &topic, format!("{}", id), &m).await;
            if o > 4 {
                sent.push(id as i64);
            }
        }
    }

    println!("Sent {} messages for feed {}", sent.len(), feed);
    sent
}

async fn wait_until_all_messages_received(table: &str) {
    let mut waited_ms = 0;
    loop {
        let values = helpers::read_table_content_as_jsons(table)
            .await
            .iter() // just to ensure it's expected value
            .map(|v| serde_json::from_value::<TestMsg>(v.clone()))
            .count();
        if values >= TOTAL_MESSAGES_RECEIVED {
            println!(
                "Received all {}/{} messages",
                values, TOTAL_MESSAGES_RECEIVED
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
        waited_ms += 1000;
        if waited_ms > 300000 {
            panic!(
                "Waited more than 300s to received {} messages, but got only {} for {}",
                TOTAL_MESSAGES_RECEIVED, values, table
            );
        }
        println!(
            "Waiting for messages to be consumed {}/{}",
            values, TOTAL_MESSAGES_RECEIVED
        );
    }
}
