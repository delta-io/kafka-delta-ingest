#[macro_use]
extern crate maplit;

extern crate kafka_delta_ingest;

use rusoto_core::Region;
use rusoto_dynamodb::*;
use std::env;
use std::sync::Once;

use kafka_delta_ingest::write_ahead_log;
use kafka_delta_ingest::write_ahead_log::{TransactionState, WriteAheadLogEntry};

// NOTE: This test file depends on kafka and localstack docker services
// Run:
// `docker-compose up`
// `RUST_LOG=debug cargo test -- --nocapture`

const TEST_LOG_NAME: &str = "write_ahead_log_test";
const LOCALSTACK_ENDPOINT: &str = "http://0.0.0.0:4566";

static INIT: Once = Once::new();

#[tokio::test]
async fn prepare_and_complete_entry() {
    setup();

    let tx_id = 1;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Prepared,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let result = write_ahead_log.put_entry(&entry).await;

    assert_eq!(Ok(()), result);

    let result = write_ahead_log.get_entry_by_transaction_id(tx_id).await;

    assert_eq!(true, result.is_ok());
    let entry = result.unwrap();
    assert_eq!(1, entry.transaction_id);
    assert_eq!(TransactionState::Prepared, entry.transaction_state);
    assert_eq!(None, entry.table_version);
    assert_eq!(
        hashmap! {
            0 => 2,
            1 => 1,
        },
        entry.partition_offsets
    );

    let result = write_ahead_log.complete_entry(tx_id, 1).await;

    assert_eq!(Ok(()), result);

    let result = write_ahead_log.get_entry_by_transaction_id(tx_id).await;

    assert_eq!(true, result.is_ok());
    let entry = result.unwrap();
    assert_eq!(1, entry.transaction_id);
    assert_eq!(TransactionState::Completed, entry.transaction_state);
    assert_eq!(None, entry.table_version);
    assert_eq!(
        hashmap! {
            0 => 2,
            1 => 1,
        },
        entry.partition_offsets
    );
}

#[tokio::test]
async fn prepare_and_abort_entry() {
    setup();

    let tx_id = 2;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Prepared,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let result = write_ahead_log.put_entry(&entry).await;

    assert_eq!(Ok(()), result);

    let result = write_ahead_log.get_entry_by_transaction_id(tx_id).await;

    assert_eq!(true, result.is_ok());

    let entry = result.unwrap();

    assert_eq!(2, entry.transaction_id);
    assert_eq!(TransactionState::Prepared, entry.transaction_state);
    assert_eq!(None, entry.table_version);
    assert_eq!(
        hashmap! {
            0 => 2,
            1 => 1,
        },
        entry.partition_offsets
    );

    let result = write_ahead_log.abort_entry(tx_id).await;

    assert_eq!(Ok(()), result);

    let result = write_ahead_log.get_entry_by_transaction_id(tx_id).await;

    assert_eq!(true, result.is_ok());

    let entry = result.unwrap();

    assert_eq!(2, entry.transaction_id);
    assert_eq!(TransactionState::Aborted, entry.transaction_state);
    assert_eq!(None, entry.table_version);
    assert_eq!(
        hashmap! {
            0 => 2,
            1 => 1,
        },
        entry.partition_offsets
    );
}

#[tokio::test]
async fn complete_aborted_should_fail() {
    setup();

    let tx_id = 3;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Prepared,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let _ = write_ahead_log.put_entry(&entry).await.unwrap();
    let _ = write_ahead_log.abort_entry(tx_id).await.unwrap();
    let result = write_ahead_log.complete_entry(tx_id, 1).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn complete_completed_should_fail() {
    setup();

    let tx_id = 4;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Prepared,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let _ = write_ahead_log.put_entry(&entry).await.unwrap();
    let _ = write_ahead_log.abort_entry(tx_id).await.unwrap();
    let result = write_ahead_log.complete_entry(tx_id, 1).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn abort_completed_should_fail() {
    setup();

    let tx_id = 5;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Prepared,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let _ = write_ahead_log.put_entry(&entry).await.unwrap();
    let _ = write_ahead_log.complete_entry(tx_id, 1).await.unwrap();
    let result = write_ahead_log.abort_entry(tx_id).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn put_completed_should_fail() {
    setup();

    let tx_id = 6;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Completed,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let result = write_ahead_log.put_entry(&entry).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn put_aborted_should_fail() {
    setup();

    let tx_id = 7;

    delete_entry_if_exists(tx_id).await;

    let write_ahead_log = write_ahead_log::new_write_ahead_log(TEST_LOG_NAME.to_owned())
        .await
        .unwrap();

    let entry = WriteAheadLogEntry::new(
        tx_id,
        TransactionState::Aborted,
        None,
        hashmap! {
                0 => 2,
                1 => 1,
        },
    );

    let result = write_ahead_log.put_entry(&entry).await;

    assert!(result.is_err());
}

fn setup() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();
        env::set_var("AWS_ENDPOINT_URL", LOCALSTACK_ENDPOINT);
        env::set_var("AWS_ACCESS_KEY_ID", "test");
        env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    });
}

async fn delete_entry_if_exists(transaction_id: i64) {
    let client = DynamoDbClient::new(Region::Custom {
        name: "custom".to_string(),
        endpoint: LOCALSTACK_ENDPOINT.to_string(),
    });

    let _ = client
        .delete_item(DeleteItemInput {
            key: hashmap! {
                "transaction_id".to_string() => AttributeValue {
                    n: Some(transaction_id.to_string()),
                    ..Default::default()
                },
            },
            table_name: TEST_LOG_NAME.to_string(),
            ..Default::default()
        })
        .await;
}
