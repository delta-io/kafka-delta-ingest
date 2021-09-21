#[allow(dead_code)]
mod helpers;

#[allow(dead_code)]
#[path = "../src/recover_dynamodb_lock.rs"]
mod recover_dynamodb_lock;

use deltalake::storage::s3::dynamodb_lock::dynamo_lock_options::*;
use deltalake::storage::s3::dynamodb_lock::{DynamoDbLockClient, DynamoDbOptions};
use deltalake::storage::s3::LockItem;
use maplit::hashmap;
use recover_dynamodb_lock::recover_dynamodb_lock;
use rusoto_dynamodb::{
    AttributeValue, AttributeValueUpdate, DynamoDb, DynamoDbClient, UpdateItemInput,
};
use std::sync::Arc;
use std::time::Duration;

const SLEEP_PERIOD: Duration = Duration::from_millis(50);

#[tokio::test]
async fn test_no_lock() {
    let (client, _, _) = create_lock_item("test_no_lock").await;
    // make sure the lock is deleted
    assert!(client.delete_lock(&get_lock(&client).await).await.unwrap());

    recover_dynamodb_lock(&client, SLEEP_PERIOD, SLEEP_PERIOD)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_acquirable_lock() {
    let (client, _, _) = create_lock_item("test_acquirable_lock").await;
    // make sure the lock is acquirable
    assert!(!get_lock(&client).await.is_non_acquirable);
    recover_dynamodb_lock(&client, SLEEP_PERIOD, SLEEP_PERIOD)
        .await
        .unwrap();
    assert!(!get_lock(&client).await.is_non_acquirable);
}

#[tokio::test]
async fn test_non_acquirable_lock() {
    let (client, dynamodb, opts) = create_lock_item("test_non_acquirable_lock").await;
    let client = Arc::new(client);

    set_flag(&dynamodb, &opts, "isNonAcquirable").await;
    // make sure the lock is non acquirable
    assert!(get_lock(&client).await.is_non_acquirable);

    let spawn_worker = || {
        let clone = client.clone();
        tokio::spawn(async move {
            recover_dynamodb_lock(&clone, SLEEP_PERIOD, SLEEP_PERIOD)
                .await
                .unwrap()
        })
    };

    let handle_1 = spawn_worker();
    let handle_2 = spawn_worker();

    tokio::time::sleep(SLEEP_PERIOD).await;
    // lock is still on and is non acquirable
    assert!(get_lock(&client).await.is_non_acquirable);

    // soft release
    set_flag(&dynamodb, &opts, "isReleased").await;
    handle_1.await.unwrap();
    handle_2.await.unwrap();

    // check that lock is deleted
    assert!(client.get_lock().await.unwrap().is_none());
}

async fn create_lock_item(name: &str) -> (DynamoDbLockClient, DynamoDbClient, DynamoDbOptions) {
    helpers::setup_envs();
    helpers::init_logger();
    let opts = DynamoDbOptions::from_map(hashmap! {
        DYNAMO_LOCK_TABLE_NAME.to_string() => "locks".to_string(),
        DYNAMO_LOCK_PARTITION_KEY_VALUE.to_string() => name.to_string(),
    });
    let dynamodb = DynamoDbClient::new(helpers::region());
    let client = DynamoDbLockClient::new(dynamodb.clone(), opts.clone());

    // clean up
    if let Some(ref item) = client.get_lock().await.unwrap() {
        assert!(client.delete_lock(item).await.unwrap());
    }

    let _ = client.acquire_lock(None).await.unwrap();
    (client, dynamodb, opts)
}

async fn set_flag(dynamodb: &DynamoDbClient, opts: &DynamoDbOptions, flag: &str) {
    dynamodb
        .update_item(UpdateItemInput {
            table_name: opts.table_name.clone(),
            key: hashmap! {
                "key".to_string() => AttributeValue {
                    s: Some(opts.partition_key_value.clone()),
                    ..Default::default()
                },
            },
            attribute_updates: Some(hashmap! {
                flag.to_string() => AttributeValueUpdate {
                    action: Some("PUT".to_string()),
                    value: Some(AttributeValue {
                        s: Some("1".to_string()),
                        ..Default::default()
                     }),
                    ..Default::default()
                }
            }),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn get_lock(client: &DynamoDbLockClient) -> LockItem {
    client.get_lock().await.unwrap().unwrap()
}
