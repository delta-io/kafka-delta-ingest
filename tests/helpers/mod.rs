use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde_json::Value;

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
