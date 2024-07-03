#![cfg(feature = "azure")]
#[allow(dead_code)]
mod helpers;

use std::collections::HashMap;
use std::env;
use std::thread;
use std::time::Duration;
use time::OffsetDateTime;

use serial_test::serial;
use uuid::Uuid;

use kafka_delta_ingest::IngestOptions;

use helpers::*;

use azure_storage::{prelude::BlobSasPermissions, shared_access_signature::SasProtocol};

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_both_workers_started_simultaneously_azure() {
    run_emails_azure_tests(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_rebalance_happens_azure() {
    run_emails_azure_tests(true).await;
}

async fn run_emails_azure_tests(initiate_rebalance: bool) {
    helpers::init_logger();
    deltalake_azure::register_handlers(None);
    let topic = format!("emails_azure-{}", Uuid::new_v4());
    let table = prepare_table(&topic).await;
    let options = create_options();
    let scope = TestScope::new(&topic, &table, options).await;

    let w1 = scope.create_and_start(helpers::WORKER_1).await;

    // in order to initiate rebalance we first send messages,
    // ensure that worker 1 consumes some of them and then create worker 2,
    // otherwise, to proceed without rebalance the two workers has to be created simultaneously
    let w2 = if initiate_rebalance {
        scope.send_messages(TEST_TOTAL_MESSAGES).await;
        thread::sleep(Duration::from_secs(1));
        scope.create_and_start(helpers::WORKER_2).await
    } else {
        let w = scope.create_and_start(helpers::WORKER_2).await;
        thread::sleep(Duration::from_secs(4));
        scope.send_messages(TEST_TOTAL_MESSAGES).await;
        w
    };

    // this will end up with more app_ids than actual,
    // since we're not sure which partitions will get each worker
    let partitions = create_partitions_app_ids(helpers::TEST_PARTITIONS);

    // wait until the destination table will get every expected message, we check this summing up
    // the each offset of each partition to get the TOTAL_MESSAGES value
    scope
        .wait_on_total_offset(partitions, helpers::TEST_TOTAL_MESSAGES)
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

async fn prepare_table(topic: &str) -> String {
    let container_client = azure_storage_blobs::prelude::ClientBuilder::emulator()
        .container_client(helpers::test_s3_bucket());
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
            .await
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

    format!("az://{}/{}", helpers::test_s3_bucket(), topic)
}

fn create_partitions_app_ids(num_p: i32) -> Vec<String> {
    let mut vector = Vec::new();
    for n in 0..num_p {
        vector.push(format!("{}-{}", helpers::TEST_APP_ID, n));
    }
    vector
}

fn create_options() -> IngestOptions {
    env::set_var("AZURE_STORAGE_USE_EMULATOR", "true");
    env::set_var("AZURE_ACCOUNT_NAME", "devstoreaccount1");
    env::set_var(
        "AZURE_ACCESS_KEY",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    );
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
        kafka_brokers: helpers::test_broker(),
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
