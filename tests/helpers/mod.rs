use bytes::Buf;
use chrono::Local;
use deltalake::action::{Action, Add, MetaData, Protocol, Remove, Txn};
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaDataTypeVersion, DeltaTable, Path};
use kafka_delta_ingest::writer::load_object_store_from_uri;
use kafka_delta_ingest::{start_ingest, IngestOptions};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::{DefaultRuntime, Timeout};
use rdkafka::ClientConfig;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, Cursor};
use std::path::Path as FilePath;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const TEST_BROKER: &str = "0.0.0.0:9092";
pub const LOCALSTACK_ENDPOINT: &str = "http://0.0.0.0:4566";

pub async fn create_topic(topic: &str, num_partitions: i32) {
    let admin_client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", TEST_BROKER)
        .create()
        .unwrap();

    let new_topic = NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1));

    admin_client
        .create_topics(&[new_topic], &AdminOptions::default())
        .await
        .unwrap();
}

pub async fn delete_topic(topic: &str) {
    let admin_client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", TEST_BROKER)
        .create()
        .unwrap();

    admin_client
        .delete_topics(&[topic], &AdminOptions::default())
        .await
        .unwrap();
}

pub fn create_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", TEST_BROKER)
        .create()
        .unwrap()
}

pub async fn send_json(producer: &FutureProducer, topic: &str, json: &Value) -> (i32, i64) {
    let json = serde_json::to_string(json).unwrap();

    let record: FutureRecord<String, String> = FutureRecord::to(topic).payload(&json);
    producer.send(record, Timeout::Never).await.unwrap()
}

pub async fn send_kv_json(
    producer: &FutureProducer,
    topic: &str,
    key: String,
    json: &Value,
) -> (i32, i64) {
    let json = serde_json::to_string(json).unwrap();

    let record: FutureRecord<String, String> = FutureRecord::to(topic).payload(&json).key(&key);
    producer.send(record, Timeout::Never).await.unwrap()
}

pub async fn send_bytes(producer: &FutureProducer, topic: &str, bytes: &Vec<u8>) {
    let record: FutureRecord<String, Vec<u8>> = FutureRecord::to(topic).payload(&bytes);
    let _ = producer.send(record, Timeout::Never).await;
}

// Example parquet read is taken from https://docs.rs/parquet/4.1.0/parquet/arrow/index.html#example-of-reading-parquet-file-into-arrow-record-batch
// TODO Research whether it's possible to read parquet data from bytes but not from file
pub async fn read_files_from_store(table: &DeltaTable) -> Vec<i32> {
    let table_uri = table.table_uri();
    let s3 = load_object_store_from_uri(&table_uri, None).unwrap();
    let paths = table.get_files();
    let tmp = format!(".test-{}.tmp", Uuid::new_v4());
    let mut list = Vec::new();

    for path in paths {
        let get_result = s3.storage_backend().get(&path).await.unwrap();
        let bytes = get_result.bytes().await.unwrap();
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .unwrap();
            file.write(bytes.chunk()).unwrap();
            file.flush().unwrap();
            drop(file)
        }

        let reader = SerializedFileReader::new(File::open(&tmp).unwrap()).unwrap();

        let mut row_iter = reader.get_row_iter(None).unwrap();

        while let Some(record) = row_iter.next() {
            list.push(record.get_string(0).unwrap().parse::<i32>().unwrap());
        }
    }

    std::fs::remove_file(tmp).unwrap();

    list.sort();
    list
}

fn parse_type(schema: &Value) -> Value {
    match schema {
        Value::String(_) => schema.clone(),
        Value::Object(_) => json!({
            "type": "struct",
            "fields": parse_fields(&schema),
        }),
        Value::Array(v) if v.len() == 1 => json!({
            "type": "array",
            "elementType": parse_type(v.first().unwrap()),
            "containsNull": true,
        }),
        _ => panic!("Unsupported type {}", schema.to_string()),
    }
}

fn parse_fields(schema: &Value) -> Value {
    let iter = schema.as_object().unwrap().iter().map(|(name, value)| {
        json!({
            "name": name,
            "type": parse_type(value),
            "metadata": {},
            "nullable": true,
        })
    });
    Value::Array(iter.collect())
}

pub fn create_metadata_action_json(schema: Value, partitions: &[&str]) -> String {
    let schema = json!({
        "type": "struct",
        "fields": parse_fields(&schema),
    });

    json!({
        "metaData": {
            "id": "ec285dbc-6479-4cc1-b038-1de97afabf9b",
            "format": {"provider":"parquet","options":{}},
            "schemaString": schema.to_string(),
            "partitionColumns": partitions,
            "createdTime": 1621845641001u64,
            "configuration": {},
        }
    })
    .to_string()
}

pub async fn cleanup_kdi(topic: &str, table: &str) {
    delete_topic(topic).await;
    std::fs::create_dir_all(table).unwrap();
}

pub async fn create_and_run_kdi(
    app_id: &str,
    schema: Value,
    delta_partitions: Vec<&str>,
    kafka_num_partitions: i32,
    opts: Option<IngestOptions>,
) -> (
    String,
    String,
    FutureProducer<DefaultClientContext, DefaultRuntime>,
    JoinHandle<()>,
    Arc<CancellationToken>,
    Runtime,
) {
    init_logger();
    let topic = format!("{}-{}", app_id, Uuid::new_v4());
    let table = create_local_table(schema, delta_partitions, &topic);
    create_topic(&topic, kafka_num_partitions).await;

    let opts = opts
        .map(|o| IngestOptions {
            app_id: app_id.to_string(),
            ..o
        })
        .unwrap_or_else(|| IngestOptions {
            app_id: app_id.to_string(),
            allowed_latency: 10,
            max_messages_per_batch: 1,
            min_bytes_per_file: 20,
            ..IngestOptions::default()
        });

    let (kdi, token, rt) = create_kdi(&topic, &table, opts);
    let producer = create_producer();
    (topic, table, producer, kdi, token, rt)
}

pub fn create_local_table(schema: Value, partitions: Vec<&str>, table_name: &str) -> String {
    let path = format!("./tests/data/gen/{}-{}", table_name, Uuid::new_v4());
    create_local_table_in(schema, partitions, &path);
    path
}

pub fn create_local_table_in(schema: Value, partitions: Vec<&str>, path: &str) {
    let v0 = format!("{}/_delta_log/00000000000000000000.json", &path);

    std::fs::create_dir_all(FilePath::new(&v0).parent().unwrap()).unwrap();

    let mut file = File::create(v0).unwrap();

    writeln!(file, r#"{{"commitInfo":{{"timestamp":1621845641000,"operation":"CREATE TABLE","operationParameters":{{"isManaged":"false","description":null,"partitionBy":"[]","properties":"{{}}"}},"isBlindAppend":true}}}}"#).unwrap();
    writeln!(
        file,
        r#"{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}"#
    )
    .unwrap();
    writeln!(file, "{}", create_metadata_action_json(schema, &partitions)).unwrap();
}

pub fn create_kdi_with(
    topic: &str,
    table: &str,
    worker_name: Option<String>,
    options: IngestOptions,
) -> (JoinHandle<()>, Arc<CancellationToken>, Runtime) {
    let app_id = options.app_id.to_string();
    let worker_name = worker_name.unwrap_or(app_id.clone());

    env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
    env::set_var("DYNAMO_LOCK_TABLE_NAME", "locks");
    env::set_var("DYNAMO_LOCK_OWNER_NAME", Uuid::new_v4().to_string());
    env::set_var("DYNAMO_LOCK_PARTITION_KEY_VALUE", app_id.clone());
    env::set_var("DYNAMO_LOCK_REFRESH_PERIOD_MILLIS", "100");
    env::set_var("DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS", "100");
    env::set_var("DYNAMO_LOCK_LEASE_DURATION", "2");

    let rt = create_runtime(&worker_name);
    let token = Arc::new(CancellationToken::new());

    let run_loop = {
        let token = token.clone();
        let topic = topic.to_string();
        let table = table.to_string();
        rt.spawn(async move {
            start_ingest(topic, table, options, token.clone())
                .await
                .unwrap()
        })
    };

    (run_loop, token, rt)
}

pub fn create_kdi(
    topic: &str,
    table: &str,
    options: IngestOptions,
) -> (JoinHandle<()>, Arc<CancellationToken>, Runtime) {
    create_kdi_with(topic, table, None, options)
}

pub fn create_runtime(name: &str) -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name(name)
        .thread_stack_size(3 * 1024 * 1024)
        .enable_io()
        .enable_time()
        .build()
        .expect("Tokio runtime error")
}

pub fn init_logger() {
    let _ = env_logger::Builder::new()
        .format(|buf, record| {
            let thread_name = std::thread::current()
                .name()
                .unwrap_or("UNKNOWN")
                .to_string();
            writeln!(
                buf,
                "{} {} [{}] - {}: {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap(),
                record.level(),
                thread_name,
                record.args(),
            )
        })
        // .filter(Some("dipstick"), log::LevelFilter::Info)
        // .filter(Some("rusoto_core"), log::LevelFilter::Info)
        // .filter(Some("deltalake"), log::LevelFilter::Info)
        .filter(None, log::LevelFilter::Info)
        .try_init();
}

pub fn wait_until_file_created(path: &FilePath) {
    let start_time = Local::now();
    loop {
        if path.exists() {
            return;
        }

        let now = Local::now();
        let poll_time = now - start_time;

        if poll_time > chrono::Duration::seconds(180) {
            panic!("File was not created before timeout");
        }
    }
}

pub fn wait_until_version_created(table: &str, version: i64) {
    let path = format!("{}/_delta_log/{:020}.json", table, version);
    wait_until_file_created(FilePath::new(&path));
}

pub async fn read_table_content_as<T: DeserializeOwned>(table_uri: &str) -> Vec<T> {
    read_table_content_as_jsons(table_uri)
        .await
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect()
}

pub async fn read_table_content_at_version_as<T: DeserializeOwned>(
    table_uri: &str,
    version: DeltaDataTypeVersion,
) -> Vec<T> {
    read_table_content_at_version_as_jsons(table_uri, version)
        .await
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect()
}

pub async fn read_table_content_as_jsons(table_uri: &str) -> Vec<Value> {
    let table = deltalake::open_table(table_uri).await.unwrap();
    let store = load_object_store_from_uri(table_uri, None).unwrap();
    json_listify_table_content(table, store).await
}

pub async fn read_table_content_at_version_as_jsons(
    table_uri: &str,
    version: DeltaDataTypeVersion,
) -> Vec<Value> {
    let table = deltalake::open_table_with_version(table_uri, version)
        .await
        .unwrap();
    let store = load_object_store_from_uri(table_uri, None).unwrap();

    json_listify_table_content(table, store).await
}

async fn json_listify_table_content(table: DeltaTable, store: DeltaObjectStore) -> Vec<Value> {
    let tmp = format!(".test-{}.tmp", Uuid::new_v4());
    let mut list = Vec::new();
    for file in table.get_files() {
        let get_result = store
            .storage_backend()
            .get(&Path::from(file))
            .await
            .unwrap();
        let bytes = get_result.bytes().await.unwrap();
        let mut file = File::create(&tmp).unwrap();
        file.write_all(bytes.chunk()).unwrap();
        drop(file);
        let reader = SerializedFileReader::new(File::open(&tmp).unwrap()).unwrap();
        let mut row_iter = reader.get_row_iter(None).unwrap();

        while let Some(record) = row_iter.next() {
            list.push(record.to_json_value());
        }
    }

    if !list.is_empty() {
        std::fs::remove_file(tmp).unwrap();
    }

    list
}

pub fn commit_file_path(table: &str, version: i64) -> String {
    format!("{}/_delta_log/{:020}.json", table, version)
}

pub async fn inspect_table(path: &str) {
    let table = deltalake::open_table(path).await.unwrap();
    println!("Inspecting table {}", path);
    for (k, v) in table.get_app_transaction_version().iter() {
        println!("  {}: {}", k, v);
    }
    let store = load_object_store_from_uri(path, None).unwrap();

    for version in 1..=table.version() {
        let log_path = format!("{}/_delta_log/{:020}.json", path, version);
        let get_result = store
            .storage_backend()
            .get(&Path::parse(&log_path).unwrap())
            .await
            .unwrap();
        let bytes = get_result.bytes().await.unwrap();
        let reader = BufReader::new(Cursor::new(bytes.chunk()));

        println!("Version {}:", version);

        for line in reader.lines() {
            let action: Action = serde_json::from_str(line.unwrap().as_str()).unwrap();
            match action {
                Action::txn(t) => {
                    println!("  Txn: {}: {}", t.app_id, t.version)
                }
                Action::add(a) => {
                    let stats = a.get_stats().unwrap().unwrap();
                    println!("  Add: {}. Records: {}", &a.path, stats.num_records);
                    let full_path = format!("{}/{}", &path, &a.path);
                    let parquet_bytes = store
                        .storage_backend()
                        .get(&Path::parse(&full_path).unwrap())
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                    let reader = SerializedFileReader::new(parquet_bytes).unwrap();
                    for record in reader.get_row_iter(None).unwrap() {
                        println!("  - {}", record.to_json_value())
                    }
                }
                _ => println!("Unknown action {:?}", action),
            }
        }
    }
    println!();
    println!("Checkpoints:");
    for version in 1..=table.version() {
        if version % 10 == 0 {
            println!("Version: {}", version);
            let log_path = format!("{}/_delta_log/{:020}.checkpoint.parquet", path, version);
            let bytes = store
                .storage_backend()
                .get(&Path::parse(&log_path).unwrap())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let reader = SerializedFileReader::new(bytes).unwrap();
            let mut i = 0;
            for record in reader.get_row_iter(None).unwrap() {
                let json = record.to_json_value();
                if let Some(m) = parse_json_field::<MetaData>(&json, "metaData") {
                    println!(" {}. metaData: {}", i, m.id);
                }
                if let Some(p) = parse_json_field::<Protocol>(&json, "protocol") {
                    println!(
                        " {}. protocol: minReaderVersion={}, minWriterVersion={}",
                        i, p.min_reader_version, p.min_writer_version
                    );
                }
                if let Some(t) = parse_json_field::<Txn>(&json, "txn") {
                    println!(" {}. txn: appId={}, version={}", i, t.app_id, t.version);
                }
                if let Some(r) = parse_json_field::<Remove>(&json, "remove") {
                    println!(" {}. remove: {}", i, r.path);
                }
                if let Some(a) = parse_json_field::<Add>(&json, "add") {
                    let records = a
                        .get_stats()
                        .ok()
                        .flatten()
                        .map(|s| s.num_records)
                        .unwrap_or(-1);
                    println!(" {}. add[{}]: {}", i, records, a.path);
                }

                i += 1;
            }
            println!()
        }
    }
}

fn parse_json_field<T: DeserializeOwned>(value: &Value, key: &str) -> Option<T> {
    value
        .as_object()
        .and_then(|v| v.get(key))
        .and_then(|v| serde_json::from_value::<T>(v.clone()).ok())
}
