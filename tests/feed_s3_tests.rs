#[allow(dead_code)]
mod helpers;

use kafka_delta_ingest::IngestOptions;
use maplit::hashmap;
use rdkafka::producer::FutureProducer;

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
}

const TOTAL_MESSAGES_SENT: usize = 900;
// see seek offsets param, we skip 10 messages in each partition, e,g. 12 x 10
const TOTAL_MESSAGES_RECEIVED: usize = TOTAL_MESSAGES_SENT - 120;
const FEEDS: usize = 3;

#[tokio::test(flavor = "multi_thread")]
async fn feed_load_test() {
    helpers::init_logger();

    let topic = format!("feed_load_{}", uuid::Uuid::new_v4());

    let table = helpers::create_local_table(
        json!({
            "id": "integer",
            "text": "string",
            "feed": "string",
        }),
        vec!["feed"],
        &topic,
    );

    helpers::create_topic(&topic, 12).await;

    let producer = Arc::new(helpers::create_producer());

    // send message in parallel
    let f_a = send_jsons("A", 0, producer.clone(), &topic);
    let f_b = send_jsons("B", 1, producer.clone(), &topic);
    let f_c = send_jsons("C", 2, producer.clone(), &topic);

    let mut workers = Vec::new();

    workers.push(spawn_worker(1, &topic, &table));
    std::thread::sleep(Duration::from_secs(10));
    workers.push(spawn_worker(2, &topic, &table));
    std::thread::sleep(Duration::from_secs(10));
    workers.push(spawn_worker(3, &topic, &table));
    f_a.await.unwrap();
    f_b.await.unwrap();
    f_c.await.unwrap();
    let (msg_handle, msg_token) = run_empty_messages(producer.clone(), &topic);
    wait_until_all_messages_received(&table).await;
    workers.iter().for_each(|w| w.1.cancel());

    for (kdi, _, rt) in workers {
        println!("wait on worker..");
        kdi.await.unwrap();
        rt.shutdown_background();
    }
    msg_token.cancel();
    msg_handle.await.unwrap();

    println!("verifying results...");

    let values = helpers::read_table_content_as_jsons(&table).await;
    let id_count = values
        .iter()
        .map(|v| v.as_object().unwrap().get("id").unwrap().as_i64().unwrap())
        .count();

    let expected = (0..TOTAL_MESSAGES_RECEIVED).count();

    if id_count != expected {
        helpers::inspect_table(&table).await;
    }
    assert_eq!(id_count, expected);

    helpers::delete_topic(&topic).await;
    std::fs::remove_dir_all(table).unwrap();
}

fn spawn_worker(
    id: usize,
    topic: &str,
    table: &str,
) -> (JoinHandle<()>, Arc<CancellationToken>, Runtime) {
    helpers::create_kdi_with(
        &topic,
        &table,
        Some(format!("WORKER-{}", id)),
        IngestOptions {
            app_id: "feed".to_string(),
            allowed_latency: 2,
            max_messages_per_batch: 10,
            min_bytes_per_file: 100,
            seek_offsets: Some(vec![
                (0, 9),
                (1, 9),
                (2, 9),
                (3, 9),
                (4, 9),
                (5, 9),
                (6, 9),
                (7, 9),
                (8, 9),
                (9, 9),
                (10, 9),
                (11, 9),
            ]),
            additional_kafka_settings: Some(hashmap! {
                "auto.offset.reset".to_string() => "earliest".to_string(),
            }),
            ..Default::default()
        },
    )
}

fn send_jsons(
    feed: &str,
    feed_n: usize,
    producer: Arc<FutureProducer>,
    topic: &str,
) -> JoinHandle<()> {
    let mut last: Option<JoinHandle<(i32, i64)>> = None;
    let feed = feed.to_string();
    for id in 0..TOTAL_MESSAGES_SENT {
        let topic = topic.to_string();
        if id % FEEDS == feed_n {
            let m = serde_json::to_value(TestMsg {
                id: id as u64,
                text: format!(
                    "{}-{}-{}-{}-{}",
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Uuid::new_v4()
                ),
                feed: feed.clone(),
            });
            let p = producer.clone();
            last = Some(tokio::spawn(async move {
                helpers::send_kv_json(&p, &topic, format!("{}", id), &m.unwrap()).await
            }));
        }
    }

    tokio::spawn(async move {
        let (p, o) = last.unwrap().await.unwrap();
        println!(
            "Send last message for feed {} where partition={}, offset={}",
            feed, p, o
        );
    })
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

fn run_empty_messages(
    producer: Arc<FutureProducer>,
    topic: &str,
) -> (JoinHandle<()>, Arc<CancellationToken>) {
    let token = Arc::new(CancellationToken::new());
    let producer = producer.clone();
    let cloned_token = token.clone();
    let topic = topic.to_string();
    let handle = tokio::spawn(async move {
        let topic = topic;
        loop {
            if cloned_token.is_cancelled() {
                return;
            }
            helpers::send_json(&producer, &topic, &json!("{}")).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    (handle, token)
}
