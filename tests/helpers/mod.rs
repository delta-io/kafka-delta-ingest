use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
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

pub fn create_local_table(schema: HashMap<&str, &str>, partitions: Vec<&str>) -> String {
    let path = format!("./tests/data/gen/table-{}", Uuid::new_v4());
    let v0 = format!("{}/_delta_log/00000000000000000000.json", &path);

    std::fs::create_dir_all(Path::new(&v0).parent().unwrap()).unwrap();

    let mut file = File::create(v0).unwrap();
    let mut fields = Vec::new();

    for (name, tpe) in schema {
        fields.push(format!(
            r#"{{\"metadata\":{{}},\"name\":\"{}\",\"nullable\":true,\"type\":\"{}\"}}"#,
            name, tpe
        ));
    }

    let schema = format!(
        r#"{{\"type\":\"struct\",\"fields\":[{}]}}"#,
        fields.join(",")
    );
    let partitions = partitions
        .iter()
        .map(|s| format!("\"{}\"", s))
        .collect::<Vec<String>>()
        .join(",");

    writeln!(file, r#"{{"commitInfo":{{"timestamp":1621845641000,"operation":"CREATE TABLE","operationParameters":{{"isManaged":"false","description":null,"partitionBy":"[]","properties":"{{}}"}},"isBlindAppend":true}}}}"#).unwrap();
    writeln!(
        file,
        r#"{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}"#
    )
    .unwrap();
    writeln!(file, r#"{{"metaData":{{"id":"ec285dbc-6479-4cc1-b038-1de97afabf9b","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[{}],"configuration":{{}},"createdTime":1621845641001}}}}"#, schema, partitions).unwrap();

    path
}
