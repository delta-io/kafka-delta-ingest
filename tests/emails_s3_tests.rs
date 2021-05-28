#[allow(dead_code)]
mod helpers;

use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};

use chrono::prelude::*;
use dipstick::{Input, Prefixed, Statsd};
use serde_json::json;
use serial_test::serial;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use kafka_delta_ingest::{instrumentation::StatsHandler, KafkaJsonToDelta, Options};
use rusoto_core::Region;
use rusoto_s3::{CopyObjectRequest, S3};
use tokio::task::JoinHandle;

const TEST_S3_ENDPOINT: &str = "http://localhost:4566";
const TEST_S3_BUCKET: &str = "tests";
const TEST_APP_ID: &str = "emails_test";
const TEST_BROKER: &str = "0.0.0.0:9092";
const TEST_CONSUMER_GROUP_ID: &str = "kafka_delta_ingest_emails";
const TEST_PARTITIONS: i32 = 4;
const TEST_TOTAL_MESSAGES: i32 = 200;

const WORKER_1: &str = "WORKER-1";
const WORKER_2: &str = "WORKER-2";

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_both_workers_started_simultaneously() {
    let version = run_emails_s3_tests().await;
    // when_both_workers_started_simultaneously 23!
    // when_both_workers_started_sequentially
    println!("when_both_workers_started_simultaneously = {}", version)
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_both_workers_started_sequentially() {
    // let version = run_emails_s3_tests().await;
    //println!("when_both_workers_started_sequentially = {}", version)
}

struct TestScope {
    pub topic: String,
    pub table: String,
    pub workers_token: Arc<CancellationToken>,
    pub runtime: HashMap<&'static str, Runtime>,
}

async fn run_emails_s3_tests() -> i64 {
    let scope = TestScope::new().await;

    let w1 = scope.create_and_start(WORKER_1).await;
    let w2 = scope.create_and_start(WORKER_2).await;

    // send expected TOTAL_MESSAGES messages
    scope.send_messages(TEST_TOTAL_MESSAGES).await;

    // once we send our expected messages, it's time to send dummy ones so the run_loop will pick
    // up the cancellation token state change. It's started on dedicated thread with `is_running`
    // mutex to dictate when exactly to stop it
    let dummy_messages_token = Arc::new(CancellationToken::new());
    let dummy_messages_handle = {
        let f = send_messages_until_stopped(scope.topic.clone(), dummy_messages_token.clone());
        tokio::spawn(async move { f.await })
    };

    // this will end up with more app_ids than actual,
    // since we're not sure which partitions will get each worker
    let partitions = create_partitions_app_ids(TEST_PARTITIONS);

    // wait until the destination table will get every expected message, we check this summing up
    // the each offset of each partition to get the TOTAL_MESSAGES value
    let last_version = scope
        .wait_on_total_offset(partitions, TEST_TOTAL_MESSAGES)
        .await;

    println!("Waiting on workers futures to exit...");
    // wait until workers are completely stopped
    w1.await.unwrap();
    println!("Worker 1 finished!");

    w2.await.unwrap();
    println!("Worker 2 finished!");

    // it's safe now to stop sending dummy messages
    dummy_messages_token.cancel();
    dummy_messages_handle.await.unwrap();

    scope.shutdown();
    last_version
}

impl TestScope {
    async fn new() -> Self {
        init_logger();
        let topic = format!("emails-{}", Uuid::new_v4());
        let table = prepare_table(&topic).await;
        let workers_token = Arc::new(CancellationToken::new());
        let mut runtime = HashMap::new();
        runtime.insert(WORKER_1, create_runtime(WORKER_1));
        runtime.insert(WORKER_2, create_runtime(WORKER_2));

        println!("Topic: {}", &topic);
        println!("Table: {}", &table);
        helpers::create_topic(topic.as_str(), TEST_PARTITIONS).await;

        Self {
            topic,
            table,
            workers_token,
            runtime,
        }
    }

    fn shutdown(self) {
        for (_, rt) in self.runtime {
            rt.shutdown_background()
        }
    }

    async fn create_and_start(&self, name: &str) -> JoinHandle<()> {
        let rt = self.runtime.get(name).unwrap();
        let mut worker = self.create_worker(name);
        let token = self.workers_token.clone();
        rt.spawn(async move { worker.start(Some(&token)).await.unwrap() })
    }

    fn create_worker(&self, name: &str) -> KafkaJsonToDelta {
        env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
        env::set_var("DYNAMO_LOCK_TABLE_NAME", "locks");
        env::set_var("DYNAMO_LOCK_OWNER_NAME", name);
        env::set_var("DYNAMO_LOCK_PARTITION_KEY_VALUE", "emails_s3_tests");
        env::set_var("DYNAMO_LOCK_REFRESH_PERIOD_MILLIS", "100");
        env::set_var("DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS", "100");
        env::set_var("DYNAMO_LOCK_LEASE_DURATION", "2");

        let mut additional_kafka_settings = HashMap::new();
        additional_kafka_settings.insert("auto.offset.reset".to_string(), "earliest".to_string());

        let allowed_latency = 2;
        let max_messages_per_batch = 10;
        let min_bytes_per_file = 370;

        let mut transforms = HashMap::new();
        transforms.insert(
            "date".to_string(),
            "substr(timestamp, `0`, `10`)".to_string(),
        );
        transforms.insert("_kafka_offset".to_string(), "kafka.offset".to_string());

        let stast_scope = Statsd::send_to("localhost:8125")
            .expect("Failed to create Statsd recorder")
            .named(TEST_APP_ID)
            .metrics();
        let stats_handler = StatsHandler::new(stast_scope);
        let stats_sender = stats_handler.tx.clone();

        let opts = Options::new(
            self.topic.clone(),
            self.table.clone(),
            TEST_APP_ID.to_string(),
            allowed_latency,
            max_messages_per_batch,
            min_bytes_per_file,
        );

        KafkaJsonToDelta::new(
            opts,
            TEST_BROKER.to_string(),
            TEST_CONSUMER_GROUP_ID.to_string(),
            Some(additional_kafka_settings),
            transforms,
            stats_sender,
        )
        .unwrap()
    }

    async fn send_messages(&self, amount: i32) {
        let producer = helpers::create_producer();
        let now: DateTime<Utc> = Utc::now();

        println!("Sending {} messages to {}", amount, &self.topic);
        for n in 0..amount {
            let json = &json!({
                "id": n.to_string(),
                "sender": format!("sender-{}@example.com", n),
                "recipient": format!("recipient-{}@example.com", n),
                "timestamp": (now + chrono::Duration::seconds(1)).to_rfc3339_opts(SecondsFormat::Secs, true),
            });
            helpers::send_json(&producer, &self.topic, &json).await;
        }
        println!("All messages are sent");
    }

    async fn wait_on_total_offset(&self, apps: Vec<String>, offset: i32) -> i64 {
        let mut table = deltalake::open_table(&self.table).await.unwrap();
        let expected_total = offset - TEST_PARTITIONS;
        loop {
            table.update().await.unwrap();
            let mut total = 0;
            for key in apps.iter() {
                total += table
                    .get_app_transaction_version()
                    .get(key)
                    .map(|x| *x)
                    .unwrap_or(0);
            }

            if total >= expected_total as i64 {
                self.workers_token.cancel();
                println!("All messages are in delta");
                return table.version;
            }

            println!("Expecting offsets in delta {}/{}...", total, expected_total);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn prepare_table(topic: &str) -> String {
    env::set_var("AWS_ENDPOINT_URL", helpers::LOCALSTACK_ENDPOINT);
    env::set_var("AWS_ACCESS_KEY_ID", "test");
    env::set_var("AWS_SECRET_ACCESS_KEY", "test");

    let s3 = rusoto_s3::S3Client::new(Region::Custom {
        name: "custom".to_string(),
        endpoint: TEST_S3_ENDPOINT.to_string(),
    });

    s3.copy_object(CopyObjectRequest {
        bucket: TEST_S3_BUCKET.to_string(),
        key: format!("{}/_delta_log/00000000000000000000.json", topic),
        copy_source: format!(
            "/{}/emails/_delta_log/00000000000000000000.json",
            TEST_S3_BUCKET
        ),
        ..Default::default()
    })
    .await
    .unwrap();

    format!("s3://{}/{}", TEST_S3_BUCKET, topic)
}

fn create_runtime(name: &str) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name(name)
        .thread_stack_size(3 * 1024 * 1024)
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

fn init_logger() {
    let _ = env_logger::Builder::new()
        .format(|buf, record| {
            let thread_name = thread::current().name().unwrap_or("UNKNOWN").to_string();
            writeln!(
                buf,
                "{} [{}] - {}: {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                thread_name,
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .try_init();
}

async fn send_messages_until_stopped(topic: String, token: Arc<CancellationToken>) {
    let producer = helpers::create_producer();
    println!("Sending dummy messages to trigger workers shutdown...");

    loop {
        if token.is_cancelled() {
            println!("Sending dummy messages is stopped...");
            return;
        }
        helpers::send_json(&producer, &topic, &json!({})).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn create_partitions_app_ids(num_p: i32) -> Vec<String> {
    let mut vector = Vec::new();
    for n in 0..num_p {
        vector.push(format!("{}-{}", TEST_APP_ID, n));
    }
    vector
}
