#[allow(dead_code)]
mod helpers;

use chrono::DateTime;
use serde_json::{json, Value};

#[tokio::test]
async fn coercions_tests() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "coercions",
        json!({
            "id": "integer",
            "ts_str": "timestamp",
            "ts_micro": "timestamp",
            "ts_invalid": "timestamp",
            "data_obj": {
                "value": "integer",
                "time": "timestamp",
            },
            "data_str": "string"
        }),
        vec![],
        1,
        None,
    )
    .await;

    let now_iso = "2021-11-12T00:12:01.123Z";
    let now_parquet = "2021-11-12 00:12:01 +00:00";
    let now = DateTime::parse_from_rfc3339(now_iso).unwrap();
    let now_micros = now.timestamp_nanos() / 1000;

    let msg_json = json!({
        "id": 1,
        "ts_str": now_iso, // in ISO 8601 string format
        "ts_micro": now_micros, // in microsecond number format
        "ts_invalid": "whatever", // invalid timestamp, should be parsed as null
        "data_obj": {
            "value": 10,
            "time": now_iso
        },
        "data_str": { // it's string in delta format but we pass it as json object
            "value": 20,
            "time": now_iso
        }
    });

    helpers::send_json(&producer, &topic, &msg_json).await;
    helpers::wait_until_version_created(&table, 1);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    let data = helpers::read_table_content_as_jsons(&table).await;
    assert_eq!(data.len(), 1);
    let result = data.first().unwrap().as_object().unwrap();
    let get = |key| result.get(key).unwrap().clone();

    assert_eq!(get("id"), json!(1));
    assert_eq!(get("ts_str"), json!(now_parquet));
    assert_eq!(get("ts_micro"), json!(now_parquet));
    assert_eq!(get("ts_invalid"), Value::Null);
    assert_eq!(get("data_obj"), json!({"value": 10, "time": now_parquet}));

    // using now_iso because data_str is treated as string and hence was not transformed by parquet
    assert_ne!(get("data_str"), json!({"value": 20, "time": now_iso}));
    let parsed: Value = serde_json::from_str(get("data_str").as_str().unwrap()).unwrap();
    assert_eq!(parsed, json!({"value": 20, "time": now_iso}));

    helpers::cleanup_kdi(&topic, &table).await;
}
