use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use uuid::Uuid;

pub const TEST_BROKER: &str = "0.0.0.0:9092";
pub const LOCALSTACK_ENDPOINT: &str = "http://0.0.0.0:4566";

pub async fn create_topic(topic: &str, num_partitions: i32) {
    let mut admin_client_config = ClientConfig::new();
    admin_client_config.set("bootstrap.servers", TEST_BROKER);

    let admin_client: AdminClient<_> = admin_client_config
        .create()
        .expect("AdminClient creation failed");
    let admin_options = AdminOptions::default();
    let new_topic = NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1));

    admin_client
        .create_topics(&[new_topic], &admin_options)
        .await
        .unwrap();
}

pub fn create_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", TEST_BROKER)
        .create()
        .unwrap()
}

pub async fn send_json(producer: &FutureProducer, topic: &str, json: &Value) {
    let json = serde_json::to_string(json).unwrap();

    let record: FutureRecord<String, String> = FutureRecord::to(topic).payload(&json);
    let _ = producer.send(record, Timeout::Never).await;
}

// Example parquet read is taken from https://docs.rs/parquet/4.1.0/parquet/arrow/index.html#example-of-reading-parquet-file-into-arrow-record-batch
// TODO Research whether it's possible to read parquet data from bytes but not from file
pub async fn read_files_from_s3(paths: Vec<String>) -> Vec<i32> {
    let s3 = deltalake::storage::get_backend_for_uri(paths.first().unwrap()).unwrap();
    let tmp = format!(".test-{}.tmp", Uuid::new_v4());
    let mut list = Vec::new();

    for path in paths {
        let mut bytes = s3.get_obj(&path).await.unwrap();
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .unwrap();
            file.write(&mut bytes).unwrap();
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
