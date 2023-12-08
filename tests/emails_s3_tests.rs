#![cfg(feature = "s3")]

#[allow(dead_code)]
mod helpers;

use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::thread;
use std::time::Duration;

use serial_test::serial;
use uuid::Uuid;

use kafka_delta_ingest::IngestOptions;
use rusoto_core::Region;
use rusoto_s3::{CopyObjectRequest, PutObjectRequest, S3};

use helpers::*;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_both_workers_started_simultaneously() {
    run_emails_s3_tests(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn when_rebalance_happens() {
    run_emails_s3_tests(true).await;
}

async fn run_emails_s3_tests(initiate_rebalance: bool) {
    helpers::init_logger();
    let topic = format!("emails_s3-{}", Uuid::new_v4());
    let table = prepare_table(&topic).await;
    let options = create_options(helpers::WORKER_1);
    let scope = TestScope::new(&topic, &table, options).await;

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

fn create_options(name: &str) -> IngestOptions {
    env::set_var("AWS_ENDPOINT_URL", helpers::test_aws_endpoint());
    env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
    env::set_var("AWS_REGION", "us-east-2");
    env::set_var("AWS_STORAGE_ALLOW_HTTP", "true");
    env::set_var("DYNAMO_LOCK_TABLE_NAME", "locks");
    env::set_var("DYNAMO_LOCK_OWNER_NAME", name);
    env::set_var("DYNAMO_LOCK_PARTITION_KEY_VALUE", "emails_s3_tests");
    env::set_var("DYNAMO_LOCK_REFRESH_PERIOD_MILLIS", "100");
    env::set_var("DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS", "100");
    env::set_var("DYNAMO_LOCK_LEASE_DURATION", "2");

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
        consumer_group_id: helpers::TEST_CONSUMER_GROUP_ID.to_string(),
        app_id: helpers::TEST_APP_ID.to_string(),
        additional_kafka_settings,
        allowed_latency,
        max_messages_per_batch,
        min_bytes_per_file,
        write_checkpoints: true,
        ..Default::default()
    }
}

async fn prepare_table(topic: &str) -> String {
    match env::var("AWS_ACCESS_KEY_ID") {
        Err(_) => env::set_var("AWS_ACCESS_KEY_ID", "test"),
        Ok(_) => {}
    }
    match env::var("AWS_SECRET_ACCESS_KEY") {
        Err(_) => env::set_var("AWS_SECRET_ACCESS_KEY", "test"),
        Ok(_) => {}
    }

    let s3 = rusoto_s3::S3Client::new(Region::Custom {
        name: "custom".to_string(),
        endpoint: helpers::test_aws_endpoint(),
    });

    /*
     * Copy the local fixture to create a simple delta table in storage.
     */
    let mut buf = vec![];
    let _original_log =
        std::fs::File::open("tests/data/emails/_delta_log/00000000000000000000.json")
            .unwrap()
            .read_to_end(&mut buf);

    s3.put_object(PutObjectRequest {
        bucket: helpers::test_s3_bucket(),
        body: Some(buf.into()),
        key: "emails/_delta_log/00000000000000000000.json".into(),
        ..Default::default()
    })
    .await
    .unwrap();

    s3.copy_object(CopyObjectRequest {
        bucket: helpers::test_s3_bucket(),
        key: format!("{}/_delta_log/00000000000000000000.json", topic),
        copy_source: format!(
            "/{}/emails/_delta_log/00000000000000000000.json",
            helpers::test_s3_bucket(),
        ),
        ..Default::default()
    })
    .await
    .unwrap();

    format!("s3://{}/{}", helpers::test_s3_bucket(), topic)
}

fn create_partitions_app_ids(num_p: i32) -> Vec<String> {
    let mut vector = Vec::new();
    for n in 0..num_p {
        vector.push(format!("{}-{}", TEST_APP_ID, n));
    }
    vector
}
