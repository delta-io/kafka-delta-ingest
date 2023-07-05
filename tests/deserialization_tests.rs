#[allow(dead_code)]
mod helpers;

use kafka_delta_ingest::{IngestOptions, MessageFormat, SchemaSource};
use log::info;
use schema_registry_converter::{
    async_impl::{
        easy_avro::EasyAvroEncoder,
        easy_json::EasyJsonEncoder,
        schema_registry::{post_schema, SrSettings},
    },
    error::SRCError,
    schema_registry_common::{RegisteredSchema, SchemaType, SubjectNameStrategy, SuppliedSchema},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serial_test::serial;

static DEFAULT_AVRO_SCHEMA: &'static str = r#"{
    "type": "record",
    "name": "test",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "date", "type": "string"}
    ]
}"#;
static SCHEMA_PATH: &'static str = "tests/data/default_schema.avro";
static DEFAULT_ID: i64 = 1;
static DEFAULT_DATE: &'static str = "2023-06-30";
static DEFAULT_NAME: &'static str = "test";
static SCHEMA_REGISTRY_ADDRESS: &'static str = "http://localhost:8081";

#[tokio::test]
#[serial]
async fn test_json_default() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_json_default",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "test_json_default".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            ..Default::default()
        }),
    )
    .await;

    let data = json!({
        "name": DEFAULT_NAME,
        "id": DEFAULT_ID,
        "date": DEFAULT_DATE,
    });
    info!("Writing test message");
    helpers::send_json(&producer, &topic, &data).await;
    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    assert_eq!(v1_rows.len(), 1);
    assert_defaults(&v1_rows[0]);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[tokio::test]
#[serial]
async fn test_json_with_args() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_json_with_args",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "test_json_with_args".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            input_format: MessageFormat::Json(SchemaSource::None),
            ..Default::default()
        }),
    )
    .await;

    let data = json!({
        "name": DEFAULT_NAME,
        "id": DEFAULT_ID,
        "date": DEFAULT_DATE,
    });
    info!("Writing test message");
    helpers::send_json(&producer, &topic, &data).await;
    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    assert_eq!(v1_rows.len(), 1);
    assert_defaults(&v1_rows[0]);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[tokio::test]
#[serial]
async fn test_json_with_registry() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_json_with_registry",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "test_json_with_registry".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            input_format: MessageFormat::Json(SchemaSource::SchemaRegistry(String::from(
                SCHEMA_REGISTRY_ADDRESS,
            ))),
            ..Default::default()
        }),
    )
    .await;

    let data = json!({
        "name": DEFAULT_NAME,
        "id": DEFAULT_ID,
        "date": DEFAULT_DATE,
    });
    info!("Writing test message");
    prepare_json_schema(topic.clone()).await.unwrap();
    let encoded = json_encode(&data, topic.clone()).await.unwrap();
    helpers::send_encoded(&producer, &topic, encoded).await;
    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    assert_eq!(v1_rows.len(), 1);
    assert_defaults(&v1_rows[0]);
    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

`#[tokio::test]
#[serial]
async fn test_avro_default() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_avro_default",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "test_avro_default".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            input_format: MessageFormat::Avro(SchemaSource::None),
            ..Default::default()
        }),
    )
    .await;

    let schema = apache_avro::Schema::parse_str(DEFAULT_AVRO_SCHEMA).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    let mut record = apache_avro::types::Record::new(writer.schema()).unwrap();
    record.put("id", DEFAULT_ID);
    record.put("name", DEFAULT_NAME);
    record.put("date", DEFAULT_DATE);
    writer.append(record).unwrap();
    let encoded = writer.into_inner().unwrap();
    helpers::send_encoded(&producer, &topic, encoded).await;
    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    assert_eq!(v1_rows.len(), 1);
    assert_defaults(&v1_rows[0]);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[tokio::test]
#[serial]
async fn test_avro_with_file() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_avro_with_file",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "test_avro_with_file".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            input_format: MessageFormat::Avro(SchemaSource::File(String::from(SCHEMA_PATH))),
            ..Default::default()
        }),
    )
    .await;

    let schema = apache_avro::Schema::parse_str(DEFAULT_AVRO_SCHEMA).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    let mut record = apache_avro::types::Record::new(writer.schema()).unwrap();
    record.put("id", DEFAULT_ID);
    record.put("name", DEFAULT_NAME);
    record.put("date", DEFAULT_DATE);
    writer.append(record).unwrap();
    let encoded = writer.into_inner().unwrap();
    helpers::send_encoded(&producer, &topic, encoded).await;
    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    assert_eq!(v1_rows.len(), 1);
    assert_defaults(&v1_rows[0]);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[tokio::test]
#[serial]
async fn test_avro_with_registry() {
    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_avro_with_registry",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "flush_when_latency_expires".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            input_format: MessageFormat::Avro(SchemaSource::SchemaRegistry(String::from(
                SCHEMA_REGISTRY_ADDRESS,
            ))),
            ..Default::default()
        }),
    )
    .await;

    let data = TestMsg {
        id: DEFAULT_ID,
        name: String::from(DEFAULT_NAME),
        date: String::from(DEFAULT_DATE),
    };
    prepare_avro_schema(topic.clone()).await.unwrap();
    let encoded = avro_encode(&data, topic.clone()).await.unwrap();
    helpers::send_encoded(&producer, &topic, encoded).await;
    // wait for latency flush
    helpers::wait_until_version_created(&table, 1);

    let v1_rows: Vec<TestMsg> = helpers::read_table_content_at_version_as(&table, 1).await;
    assert_eq!(v1_rows.len(), 1);
    assert_defaults(&v1_rows[0]);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestMsg {
    id: i64,
    date: String,
    name: String,
}

fn default_settings() -> SrSettings {
    SrSettings::new(String::from(String::from(SCHEMA_REGISTRY_ADDRESS)))
}

async fn avro_encode(item: impl Serialize, topic: String) -> Result<Vec<u8>, SRCError> {
    EasyAvroEncoder::new(default_settings())
        .encode_struct(item, &SubjectNameStrategy::RecordNameStrategy(topic))
        .await
}

async fn json_encode(value: &serde_json::Value, topic: String) -> Result<Vec<u8>, SRCError> {
    EasyJsonEncoder::new(default_settings())
        .encode(value, SubjectNameStrategy::RecordNameStrategy(topic))
        .await
}

async fn prepare_json_schema(topic: String) -> Result<RegisteredSchema, SRCError> {
    let settings = default_settings();
    let schema = SuppliedSchema {
        name: None,
        schema_type: SchemaType::Json,
        schema: String::from(
            r#"{"schemaType": "JSON", "schema": "{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}, \"date\": {\"type\": \"string\"}, \"id\": {\"type\": \"number\"}}}"}"#,
        ),
        references: vec![],
    };
    post_schema(&settings, topic, schema).await
}

async fn prepare_avro_schema(topic: String) -> Result<RegisteredSchema, SRCError> {
    let settings = default_settings();
    let schema = SuppliedSchema {
        name: None,
        schema_type: SchemaType::Avro,
        schema: String::from(DEFAULT_AVRO_SCHEMA),
        references: vec![],
    };
    post_schema(&settings, topic, schema).await
}

fn assert_defaults(msg: &TestMsg) {
    assert_eq!(msg.id, DEFAULT_ID);
    assert_eq!(msg.name, DEFAULT_NAME);
    assert_eq!(msg.date, DEFAULT_DATE);
}

fn default_schema() -> serde_json::Value {
    json!({
        "name": "string",
        "id": "integer",
        "date": "string",
    })
}
