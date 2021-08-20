use kafka_delta_ingest::dead_letters::DeadLetter;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json;
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
async fn test_dlq_partial_parquet_write() {
    helpers::init_logger();

    let table = create_table();
    let data_topic = create_data_topic().await;
    let dlq_table = create_dlq_table().await;

    let allowed_latency = 5;
    let max_messages_per_batch = 6;
    // let max_messages_per_batch = 6;
    let min_bytes_per_file = 20;

    let (kdi, token, rt) = helpers::create_kdi(
        "dlq_test",
        &data_topic,
        &table,
        Some(dlq_table.clone()),
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

    let bad_bytes_generator = std::iter::repeat("bad bytes".as_bytes().to_vec());

    for m in bad_bytes_generator.clone().take(1) {
        helpers::send_bytes(&producer, &data_topic, &m).await;
    }

    info!("Sent {} records from bytes", batch_to_send.len());

    helpers::wait_until_version_created(&dlq_table, 2);

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

    let table_content: Vec<TestMsg> = helpers::read_table_content(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect();

    println!("{:#?}", table_content);

    let dlq_content: Vec<DeadLetter> = helpers::read_table_content(&dlq_table)
        .await
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect();

    println!("{:#?}", dlq_content);

    todo!()
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
            "timestamp" => "string",
            "date" => "string",
        },
        vec!["date"],
    )
}
