// use clap::{App, AppSettings, Arg};
use kafka_delta_ingest;
use rdkafka::{config::ClientConfig, consumer::{
    Consumer, 
    StreamConsumer
}};

#[tokio::main]
async fn main()  -> anyhow::Result<()> {
    // TODO: Move hard-coded values to configuration / program arguments and wrap in KafkaToDeltaOptions struct

    let topic = "example";
    let table_path = "./tests/data/example";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "kafka-delta-ingest:example")
        // Commit every 5 seconds... but...
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but... only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")        
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Consumer subscription failed")
    ;

    let allowed_latency = 10u64;
    let max_messages_per_batch = 5usize;
    let min_bytes_per_file = 1000usize;

    let _ = tokio::spawn(kafka_delta_ingest::start(consumer, table_path, allowed_latency, max_messages_per_batch, min_bytes_per_file)).await;

    Ok(())
}
