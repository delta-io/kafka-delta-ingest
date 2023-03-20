#[allow(dead_code)]
mod helpers;

use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use azure_storage::{prelude::BlobSasPermissions, shared_access_signature::SasProtocol};
use time::OffsetDateTime;
use tokio::runtime::Runtime;

use chrono::prelude::*;
use serde_json::json;
use serial_test::serial;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use kafka_delta_ingest::{start_ingest, IngestOptions};
use tokio::task::JoinHandle;

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
async fn when_both_workers_started_simultaneously_azure() {
    run_emails_s3_tests(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_rebalance_happens_azure() {
    run_emails_s3_tests(true).await;
}

struct TestScope {
    pub topic: String,
    pub table: String,
    pub workers_token: Arc<CancellationToken>,
    pub runtime: HashMap<&'static str, Runtime>,
}

async fn run_emails_s3_tests(initiate_rebalance: bool) {
    helpers::init_logger();
    let scope = TestScope::new().await;

    let w1 = scope.create_and_start(WORKER_1).await;

    // in order to initiate rebalance we first send messages,
    // ensure that worker 1 consumes some of them and then create worker 2,
    // otherwise, to proceed without rebalance the two workers has to be created simultaneously
    let w2 = if initiate_rebalance {
        scope.send_messages(TEST_TOTAL_MESSAGES).await;
        thread::sleep(Duration::from_secs(1));
        scope.create_and_start(WORKER_2).await
    } else {
        let w = scope.create_and_start(WORKER_2).await;
        thread::sleep(Duration::from_secs(4));
        scope.send_messages(TEST_TOTAL_MESSAGES).await;
        w
    };

    // this will end up with more app_ids than actual,
    // since we're not sure which partitions will get each worker
    let partitions = create_partitions_app_ids(TEST_PARTITIONS);

    // wait until the destination table will get every expected message, we check this summing up
    // the each offset of each partition to get the TOTAL_MESSAGES value
    scope
        .wait_on_total_offset(partitions, TEST_TOTAL_MESSAGES)
        .await;

    println!("Waiting on workers futures to exit...");
    // wait until workers are completely stopped
    let w1_result = w1.await;
    println!("Worker 1 finished - {:?}", w1_result);

    let w2_result = w2.await;
    println!("Worker 2 finished - {:?}", w2_result);

    scope.validate_data().await;
    scope.shutdown();
}

impl TestScope {
    async fn new() -> Self {
        let topic = format!("emails-{}", Uuid::new_v4());
        let table = prepare_table(&topic).await;
        let workers_token = Arc::new(CancellationToken::new());
        let mut runtime = HashMap::new();
        runtime.insert(WORKER_1, helpers::create_runtime(WORKER_1));
        runtime.insert(WORKER_2, helpers::create_runtime(WORKER_2));

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
        let topic = self.topic.clone();
        let table = self.table.clone();
        let options = self.create_options();
        let token = self.workers_token.clone();
        rt.spawn(async move {
            let res = start_ingest(topic, table, options, token.clone()).await;
            res.unwrap_or_else(|e| println!("AN ERROR OCCURED: {}", e));
            println!("Ingest process exited");

            token.cancel();
        })
    }

    fn create_options(&self) -> IngestOptions {
        env::set_var("AZURE_STORAGE_USE_EMULATOR", "true");
        env::set_var("AZURE_ACCOUNT_NAME", "devstoreaccount1");
        env::set_var("AZURE_ACCESS_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        env::set_var("AZURE_STORAGE_CONTAINER_NAME", "tests");
        env::set_var("AZURE_STORAGE_ALLOW_HTTP", "1");
        env::set_var("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000");
        env::set_var(
            "AZURE_STORAGE_CONNECTION_STRING", 
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;QueueEndpoint=http://localhost:10001/devstoreaccount1;");

        let mut additional_kafka_settings = HashMap::new();
        additional_kafka_settings.insert("auto.offset.reset".to_string(), "earliest".to_string());
        let additional_kafka_settings = Some(additional_kafka_settings);

        let allowed_latency = 2;
        let max_messages_per_batch = 10;
        let min_bytes_per_file = 370;

        let mut transforms = HashMap::new();
        transforms.insert("date".to_string(), "substr(timestamp,`0`,`10`)".to_string());
        transforms.insert("_kafka_offset".to_string(), "kafka.offset".to_string());

        IngestOptions {
            transforms,
            kafka_brokers: TEST_BROKER.to_string(),
            consumer_group_id: TEST_CONSUMER_GROUP_ID.to_string(),
            app_id: TEST_APP_ID.to_string(),
            additional_kafka_settings,
            allowed_latency,
            max_messages_per_batch,
            min_bytes_per_file,
            write_checkpoints: true,
            ..Default::default()
        }
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

    async fn wait_on_total_offset(&self, apps: Vec<String>, offset: i32) {
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
                return;
            }

            println!("Expecting offsets in delta {}/{}...", total, expected_total);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn validate_data(&self) {
        let table = deltalake::open_table(&self.table).await.unwrap();
        let result = helpers::read_files_from_store(&table).await;
        let r: Vec<i32> = (0..TEST_TOTAL_MESSAGES).collect();
        println!("Got messages {}/{}", result.len(), TEST_TOTAL_MESSAGES);

        if result.len() != TEST_TOTAL_MESSAGES as usize {
            helpers::inspect_table(&self.table).await;
        }

        assert_eq!(result, r);
    }
}

async fn prepare_table(topic: &str) -> String {
    let container_client =
        azure_storage_blobs::prelude::ClientBuilder::emulator().container_client(TEST_S3_BUCKET);
    let source_blob =
        container_client.blob_client(format!("emails/_delta_log/00000000000000000000.json"));
    let sas_url = {
        let now = OffsetDateTime::now_utc();
        let later = now + time::Duration::hours(1);
        let sas = source_blob
            .shared_access_signature(
                BlobSasPermissions {
                    read: true,
                    ..Default::default()
                },
                later,
            )
            .unwrap()
            .start(now)
            .protocol(SasProtocol::HttpHttps);
        source_blob.generate_signed_blob_url(&sas).unwrap()
    };
    container_client
        .blob_client(format!("{}/_delta_log/00000000000000000000.json", topic))
        .copy_from_url(sas_url)
        .await
        .unwrap();

    format!("az://{}/{}", TEST_S3_BUCKET, topic)
}

fn create_partitions_app_ids(num_p: i32) -> Vec<String> {
    let mut vector = Vec::new();
    for n in 0..num_p {
        vector.push(format!("{}-{}", TEST_APP_ID, n));
    }
    vector
}
