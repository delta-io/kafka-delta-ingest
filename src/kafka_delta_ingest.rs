extern crate anyhow;

use arrow::{
    datatypes::Schema as ArrowSchema,
};
use deltalake::Schema;
use futures::stream::StreamExt;
use log::{info, warn};
use rdkafka::{Message, consumer::{
        Consumer, 
        StreamConsumer
    }};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

use crate::deltalake_ext;
use crate::deltalake_ext::DeltaParquetWriter;

pub async fn start(consumer: StreamConsumer, delta_table_path: &str, allowed_latency: u64, max_messages_per_batch: usize, min_bytes_per_file: usize) {
    let mut delta_table = deltalake::open_table(delta_table_path).await.unwrap();
    let mut json_buffer: Vec<Value> = Vec::new();
    let mut stream = consumer.stream();
    let metadata = delta_table.get_metadata().unwrap().clone();
    let schema = metadata.schema;
    let mut delta_writer = DeltaParquetWriter::for_table_path_with_schema(delta_table.table_path.clone(), &schema).await.unwrap();

    let metadata = delta_table.get_metadata().unwrap().clone();
    let arrow_schema_ref = Arc::new(<ArrowSchema as From<&Schema>>::from(&metadata.schema));
    let partition_cols = metadata.partition_columns;

    let mut timer = Instant::now();

    // Handle each message on the Kafka topic in a loop.
    while let Some(message) = stream.next().await {
        match message {
            Ok(m) => {
                // Deserialize the rdkafka message into a serde_json::Value
                // TODO: Handle no payload error
                let message_bytes = m.payload().expect("No payload");
                
                // TODO: Handle deserialization error
                let mut value: Value = match serde_json::from_slice(message_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        // TODO: Handle error
                        panic!("Error deserializing message {:?}", e);
                    }
                };

                // Transform the message according to topic configuration.
                // This is a noop for now.
                transform_message(&mut value, &m);

                // Append to json batch
                json_buffer.push(value);

                //
                // When the json batch contains max messages per batch messages
                // or the allowed latency has elapsed, write a record batch.
                //

                let should_complete_record_batch = json_buffer.len() == max_messages_per_batch || timer.elapsed().as_secs() >= allowed_latency;

                if should_complete_record_batch {

                    // Convert the json buffer to an arrow record batch.
                    // TODO: Handle error
                    let record_batch = deltalake_ext::record_batch_from_json_buffer(arrow_schema_ref.clone(), json_buffer.as_slice()).unwrap();

                    // Clear the json buffer so we can add start clean on the next round.
                    json_buffer.clear();

                    // Write the arrow record batch to our delta writer that wraps an in memory cursor for tracking bytes.
                    // TODO: Handle error
                    delta_writer.write_record_batch(&partition_cols, &record_batch).await.unwrap();

                    // When the memory buffer meets min bytes per file or allowed latency is met, complete the file and start a new one.
                    // TODO: Handle error
                    let should_complete_file = delta_writer.buffer_len().unwrap() >= min_bytes_per_file || timer.elapsed().as_secs() >= allowed_latency;

                    if should_complete_file {
                        // TODO: Handle error
                        let add = delta_writer.complete_file(&partition_cols).await.unwrap();
                        let action = deltalake::action::Action::add(add);

                        // Create and commit a delta transaction to add the new data file to table state.
                        let mut tx = delta_table.create_transaction(None);
                        // TODO: Handle error
                        let committed_version = tx.commit_with(&[action], None).await.unwrap();

                        // Reset the timer to track allowed latency for the next round
                        timer = Instant::now();

                        info!("Comitted Delta version {}", committed_version);
                    }

                    // TODO: Kafka partition offsets shall eventually be written to a separate write-ahead-log 
                    // to maintain consistency with the data written in each version of the delta log.
                    // The offset commit to Kafka shall be specifically for consumer lag monitoring support.

                    // Commit the offset to Kafka so consumer lag monitors know where we are.
                    if let Err(e) = consumer.store_offset(&m) {
                        warn!("Failed to commit consumer offsets {:?}", e);
                    } else {
                        info!("Committed offset {}", m.offset());
                    }
                }
            },
            Err(e) => {
                // TODO: Handle error
                panic!("Error while processing stream {:?}", e);
            }
        }
    }
}

fn transform_message<M>(_value: &mut Value, message: &M) where M : Message {
    // TODO: transform the message according to topic configuration options
    // ...
}
