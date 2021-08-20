use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use uuid::Uuid;

#[macro_use]
extern crate maplit;

#[allow(dead_code)]
mod helpers;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MsgV1 {
    id: u32,
    date: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
struct MsgV2 {
    id: u32,
    color: Option<String>,
    date: String,
}

#[tokio::test]
async fn schema_update_test() {
    helpers::init_logger();
    let table = helpers::create_local_table(
        hashmap! {
            "id" => "integer",
            "date" => "string",
        },
        vec!["date"],
    );
    let topic = format!("schema_update_{}", Uuid::new_v4());
    helpers::create_topic(&topic, 1).await;

    let (kdi, token, rt) = helpers::create_kdi("schema_update", &topic, &table, None, 5, 1, 20);
    let producer = helpers::create_producer();

    let msg_v1 = MsgV1 {
        id: 1,
        date: "default".to_string(),
    };

    let msg_v2_1 = MsgV2 {
        id: 2,
        color: Some("red".to_string()),
        date: "default".to_string(),
    };

    let msg_v2_2 = MsgV2 {
        id: 3,
        color: Some("blue".to_string()),
        date: "default".to_string(),
    };

    // send msg v1
    helpers::send_json(
        &producer,
        &topic,
        &serde_json::to_value(msg_v1.clone()).unwrap(),
    )
    .await;
    helpers::wait_until_version_created(&table, 1);

    // update delta schema with new col 'color'
    let new_schema = hashmap! {
        "id" => "integer",
        "color" => "string",
        "date" => "string",
    };
    alter_schema(&table, 2, new_schema, vec!["date"]);

    // send few messages with new schema
    helpers::send_json(
        &producer,
        &topic,
        &serde_json::to_value(msg_v2_1.clone()).unwrap(),
    )
    .await;
    helpers::send_json(
        &producer,
        &topic,
        &serde_json::to_value(msg_v2_2.clone()).unwrap(),
    )
    .await;
    helpers::wait_until_version_created(&table, 4);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    // retrieve data from the table
    let content: Vec<MsgV2> = helpers::read_table_content(&table)
        .await
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect();

    // convert msg v1 to v2
    let expected = vec![
        MsgV2 {
            id: msg_v1.id.clone(),
            color: None,
            date: msg_v1.date.clone(),
        },
        msg_v2_1,
        msg_v2_2,
    ];

    //  and compare the results
    assert_eq!(content, expected);

    // cleanup
    for v in 1..=4 {
        std::fs::remove_file(helpers::commit_file_path(&table, v)).unwrap();
    }
}

fn alter_schema(table: &str, version: i64, schema: HashMap<&str, &str>, partitions: Vec<&str>) {
    let mut file = File::create(format!("{}/_delta_log/{:020}.json", table, version)).unwrap();
    let schema = helpers::create_metadata_action_json(&schema, &partitions);
    writeln!(file, "{}", schema).unwrap();
}
