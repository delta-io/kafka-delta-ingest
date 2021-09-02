use log::info;
use serde::{Deserialize, Serialize};
use serde_json;
use serde_json::Value;
use uuid::Uuid;

#[allow(dead_code)]
mod helpers;

#[macro_use]
extern crate maplit;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestMsgNested {
    value: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestMsg {
    value: String,
    a_list_of_structs: Option<Vec<TestMsgNested>>,
    date: String,
}

#[tokio::test]
async fn test_dlq() {
    helpers::init_logger();

    let table = create_table();
    let data_topic = create_data_topic().await;
    let dlq_table = create_dlq_table().await;

    let allowed_latency = 5;
    let max_messages_per_batch = 6;
    let min_bytes_per_file = 20;

    let (kdi, token, rt) = helpers::create_kdi(
        "dlq_test",
        &data_topic,
        &table,
        Some(dlq_table.clone()),
        hashmap! {
            "date".to_string() => "substr(epoch_micros_to_iso8601(timestamp),`0`,`10`)".to_string(),
        },
        allowed_latency,
        max_messages_per_batch,
        min_bytes_per_file,
    );
    let producer = helpers::create_producer();

    let good_generator = std::iter::repeat(TestMsg {
        value: "good".to_string(),
        a_list_of_structs: Some(vec![TestMsgNested {
            value: "abc".to_string(),
        }]),
        date: "2021-01-01".to_string(),
    });

    let null_struct_generator = std::iter::repeat(TestMsg {
        value: "bad-null_struct".to_string(),
        a_list_of_structs: None,
        date: "2021-01-01".to_string(),
    });

    let expected_date = chrono::Utc::now();
    let expected_date = format!("{}", expected_date.format("%Y-%m-%d"));

    // 4 good messages
    // 2 bad messages that fail parquet write
    let batch_to_send: Vec<TestMsg> = good_generator
        .clone()
        .take(2)
        .chain(null_struct_generator.take(2))
        .chain(good_generator.clone().take(2))
        .collect();

    for m in batch_to_send.iter() {
        helpers::send_json(&producer, &data_topic, &serde_json::to_value(m).unwrap()).await;
    }

    info!("Sent {} records from structs", batch_to_send.len());

    helpers::wait_until_version_created(&dlq_table, 1);
    helpers::wait_until_version_created(&table, 1);

    // 1 message with bad bytes
    let bad_bytes_generator = std::iter::repeat("bad bytes".as_bytes().to_vec());

    for m in bad_bytes_generator.clone().take(1) {
        helpers::send_bytes(&producer, &data_topic, &m).await;
    }

    info!("Sent {} records from bytes", batch_to_send.len());

    helpers::wait_until_version_created(&dlq_table, 2);

    // 6 more good messages just to make sure the stream keep working after hitting some bad
    let good_bytes_generator = good_generator.clone().map(|g| {
        let json = serde_json::to_string(&g).unwrap();
        json.as_bytes().to_vec()
    });

    for m in good_bytes_generator.clone().take(6) {
        helpers::send_bytes(&producer, &data_topic, &m).await;
    }

    helpers::wait_until_version_created(&table, 2);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    // after above sequence - we should have 10 good messages and 3 dead letters
    // good messages should be in the data table and dead letters should be in the dlq_table

    let table_content: Vec<TestMsg> = helpers::read_table_content(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect();

    assert_eq!(table_content.len(), 10);

    // Since we are using timestamp data type in delta -
    // the timestamp column of the table is read back as a JSON string and not immediately deserializable into a `DeadLetter` struct.
    // So we will validate against a serde_json::Value instead.
    //
    // let dlq_content: Vec<DeadLetter> = helpers::read_table_content(&dlq_table)
    //     .await
    //     .iter()
    //     .map(|v| serde_json::from_value(v.clone()).unwrap())
    //     .collect();

    // let bad_serde_records: Vec<DeadLetter> = dlq_content
    //     .iter()
    //     .filter(|d| d.base64_bytes == Some("YmFkIGJ5dGVz".to_string()))
    //     .map(|d| d.to_owned())
    //     .collect();
    // assert_eq!(bad_serde_records.len(), 1);

    // // NOTE: this should break when upstream arrow fix is made for list<struct<...>> (helpful reminder to change test)
    // // See: https://github.com/apache/arrow-rs/pull/704
    // let bad_null_struct_records: Vec<DeadLetter> = dlq_content
    //     .iter()
    //     .filter(|d| {
    //         d.error.is_some()
    //             && d.error
    //                 .as_ref()
    //                 .unwrap()
    //                 .as_str()
    //                 .contains("Inconsistent length of definition and repetition levels")
    //     })
    //     .map(|d| d.to_owned())
    //     .collect();

    // assert_eq!(bad_null_struct_records.len(), 2);

    let dlq_content: Vec<Value> = helpers::read_table_content(&dlq_table).await;
    assert_eq!(dlq_content.len(), 3);

    let bad_serde_records: Vec<Value> = dlq_content
        .iter()
        .filter(|v| {
            v.get("base64_bytes")
                .map(|v| v.as_str() == Some("YmFkIGJ5dGVz"))
                .unwrap_or(false)
        })
        .map(|v| v.to_owned())
        .collect();
    assert_eq!(bad_serde_records.len(), 1);

    let bad_null_struct_records: Vec<Value> = dlq_content
        .iter()
        .filter(|v| {
            v.get("error")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("Inconsistent length of definition and repetition levels")
        })
        .map(|v| v.to_owned())
        .collect();
    assert_eq!(bad_null_struct_records.len(), 2);

    assert!(dlq_content
        .iter()
        .all(|v| v.get("date").unwrap().as_str() == Some(expected_date.as_str())));
}

fn create_table() -> String {
    let struct_schema =
        helpers::create_array_schema_field(helpers::create_struct_schema_field(vec![
            helpers::create_schema_field("value", "string"),
        ]));

    helpers::create_local_table(
        hashmap! {
            "value" => "string",
            "a_list_of_structs" => struct_schema.as_str(),
            "date" => "string",
        },
        vec!["date"],
    )
}

async fn create_data_topic() -> String {
    let topic = format!("dlq_test_source_{}", Uuid::new_v4());
    helpers::create_topic(&topic, 3).await;
    topic
}

async fn create_dlq_table() -> String {
    helpers::create_local_table(
        hashmap! {
            "base64_bytes" => "string",
            "json_string" => "string",
            "error" => "string",
            "timestamp" => "timestamp",
            "date" => "string",
        },
        vec!["date"],
    )
}
