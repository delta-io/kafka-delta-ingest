extern crate anyhow;

use arrow::{
    array::{Array, as_primitive_array}, 
    datatypes::*,
    error::ArrowError,
    datatypes::Schema as ArrowSchema,
    json::reader::Decoder,
    record_batch::RecordBatch,
};
use deltalake::{DeltaTable, DeltaTableError, DeltaTableMetaData, DeltaTransactionError, Schema, StorageBackend, StorageError, UriError, 
    action::{Add, Action, Stats}
};
use futures::stream::StreamExt;
use log::{info, warn};
use parquet::{
    arrow::ArrowWriter, 
    basic::Compression,
    errors::ParquetError,
    file::{properties::WriterProperties, writer::InMemoryWriteableCursor},
};
use rdkafka::{Message, config::ClientConfig, consumer::{
        Consumer, 
        StreamConsumer
    }};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// TODO: Encapsulate the current main within a KafkaJsonToDelta struct

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

    let _ = tokio::spawn(start(consumer, table_path)).await;

    Ok(())
}

async fn start(consumer: StreamConsumer, delta_table_path: &str) {
    let mut delta_table = deltalake::open_table(delta_table_path).await.unwrap();
    let mut json_batch: Vec<Value> = Vec::new();
    let mut stream = consumer.stream();

    while let Some(message) = stream.next().await {
        match message {
            Ok(m) => {
                // Deserialize the rdkafka message into a serde_json::Value
                // TODO: Handle no payload error
                let message_bytes = m.payload().expect("No payload");
                
                let mut value: Value = match serde_json::from_slice(message_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        // TODO: Handle error
                        panic!("Error deserializing message {:?}", e);
                    }
                };

                transform_message(&mut value);

                // Append to json batch
                json_batch.push(value);

                // TODO: use configured message batch size instead of hard-coded `10`
                let should_complete_record_batch = json_batch.len() >= 10;

                if should_complete_record_batch {
                    let _table_version = write_json_batch(&json_batch, &mut delta_table).await;

                    json_batch.clear();

                    // TODO: Hoist DeltaWriter ownership and transaction control flow into this scope
                    // ...

                    if let Err(e) = consumer.store_offset(&m) {
                        info!("Committed offset {}", m.offset());
                        warn!("Failed to commit consumer offsets");
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

fn transform_message(_value: &mut Value) {
    // TODO: transform the message according to topic configuration options
    // ...
}

async fn write_json_batch(message_buffer: &Vec<Value>, delta_table: &mut DeltaTable) -> Result<i64, DeltaTransactionError> {
    // TODO: This method is currently owning the DeltaWriter, closing it and committing the transaction
    // Ultimately, the writer must be held outside of this scope to allow multiple record batches and for the transaction to be committed separately.

    let delta_writer = DeltaWriter::for_table_path(delta_table.table_path.clone()).await.unwrap();

    // start a transaction
    let metadata = delta_table.get_metadata().unwrap().clone();
    let mut transaction = delta_table.create_transaction(None);

    let arrow_schema_ref = Arc::new(<ArrowSchema as From<&Schema>>::from(&metadata.schema));
    let record_batch = record_batch_from_json_buffer(arrow_schema_ref, message_buffer).unwrap();

    // write data and collect add
    let add = delta_writer.write_record_batch(&metadata, &record_batch).await.unwrap();

    // TODO: Add `Operation::StreamingUpdate` to delta-rs
    // TODO: Add `txn` action to the same transaction
    transaction.commit_with(&[Action::add(add)], None).await
}


// ---
// NOTE: Everything below this line is based on https://github.com/delta-io/delta-rs/blob/main/rust/tests/write_exploration.rs and more appropriate to live in delta-rs after a more complete implementation.
// ---

#[derive(thiserror::Error, Debug)]
pub enum DeltaWriterError {
    #[error("Partition column contains more than one value")]
    NonDistinctPartitionValue,

    #[error("Missing partition column: {col_name}")]
    MissingPartitionColumn { col_name: String },

    #[error("Invalid table path: {}", .source)]
    UriError {
        #[from]
        source: UriError,
    },

    #[error("Storage interaction failed: {source}")]
    Storage { 
        #[from]
        source: StorageError 
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable { 
        #[from]
        source: DeltaTableError 
    },

    #[error("Arrow interaction failed: {source}")]
    Arrow { 
        #[from]
        source: ArrowError 
    },

    #[error("Parquet write failed: {source}")]
    Parquet { 
        #[from]
        source: ParquetError 
    },
}

pub struct DeltaWriter {
    table_path: String,
    storage: Box<dyn StorageBackend>,
}

/// A writer that writes record batches in parquet format to a table location.
/// This should be used along side a DeltaTransaction wrapping the same DeltaTable instance.
impl DeltaWriter {
    pub async fn for_table_path(table_path: String) -> Result<DeltaWriter, DeltaWriterError> {
        let storage = deltalake::get_backend_for_uri(table_path.as_str())?;

        Ok(Self {
            table_path,
            storage,
        })
    }

    // Ideally, we should separate the initialization of the cursor and the call to close to enable writing multiple record batches to the same file.
    // Keeping it simple for now and writing a single record batch to each file.
    pub async fn write_record_batch(&self, metadata: &DeltaTableMetaData, record_batch: &RecordBatch) -> Result<Add, DeltaWriterError> {
        let partition_values = extract_partition_values(metadata, record_batch)?;

        // TODO: lookup column stats
        // let column_stats = HashMap::new();

        let cursor = self.write_to_parquet_buffer(metadata, &record_batch).await?;

        let path = self.next_data_path(metadata, &partition_values).unwrap();

        // TODO: handle error
        let obj_bytes = cursor.into_inner().unwrap();

        let storage_path = format!("{}/{}", self.table_path, path);

        self.storage.put_obj(storage_path.as_str(), obj_bytes.as_slice()).await?;

        create_add(&partition_values, path, obj_bytes.len() as i64, &record_batch)
    }

    // Ideally, we should separate the initialization of the cursor and the call to close to enable writing multiple record batches to the same file.
    // Keeping it simple for now and writing a single record batch to each file.
    async fn write_to_parquet_buffer(&self, metadata: &DeltaTableMetaData, batch: &RecordBatch) -> Result<InMemoryWriteableCursor, DeltaWriterError> {
        let schema = &metadata.schema;
        let arrow_schema = <ArrowSchema as From<&Schema>>::from(schema);
        let arrow_schema_ref = Arc::new(arrow_schema);

        let writer_properties = WriterProperties::builder()
            // TODO: Extract config/env for writer properties and set more than just compression
            .set_compression(Compression::SNAPPY)
            .build();
        let cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(cursor.clone(), arrow_schema_ref.clone(), Some(writer_properties)).unwrap();

        writer.write(batch)?;
        writer.close()?;

        Ok(cursor)
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(&self, metadata: &DeltaTableMetaData, partition_values: &HashMap<String, String>) -> Result<String, DeltaWriterError> {
        // TODO: what does 00000 mean?
        let first_part = "00000";
        let uuid_part = Uuid::new_v4();
        // TODO: what does c000 mean?
        let last_part = "c000";

        let file_name = format!("part-{}-{}-{}.parquet", first_part, uuid_part, last_part);

        let partition_cols = metadata.partition_columns.as_slice();

        let data_path = if partition_cols.len() > 0 {
            let mut path_part = String::with_capacity(20);

            // ugly string builder hack
            let mut first = true;

            for k in partition_cols.iter() {
                let partition_value = partition_values.get(k).ok_or(DeltaWriterError::MissingPartitionColumn{ col_name: k.to_string() })?;

                if first {
                    first = false;
                } else {
                    path_part.push_str("/");
                }

                path_part.push_str(k);
                path_part.push_str("=");
                path_part.push_str(partition_value);
            }

            format!("{}/{}", path_part, file_name)
        } else {
            file_name
        };

        Ok(data_path)
    }
}

pub struct InMemValueIter<'a> {
    buffer: &'a[Value],
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    fn from_vec(v: &'a[Value]) -> Self {
        Self {
            buffer: v,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for InMemValueIter<'a> {
    type Item = Result<Value, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.buffer.get(self.current_index);

        self.current_index += 1;

        item.map(|v| Ok(v.to_owned()))
    }
}

pub fn record_batch_from_json_buffer(arrow_schema_ref: Arc<ArrowSchema>, json_buffer: &[Value]) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = json_buffer.len();
    let mut value_ter = InMemValueIter::from_vec(json_buffer);
    let decoder = Decoder::new(arrow_schema_ref.clone(), row_count, None);
    let batch = decoder.next_batch(&mut value_ter)?;

    // handle none
    let batch = batch.unwrap();

    Ok(batch)
}

pub fn extract_partition_values(metadata: &DeltaTableMetaData, record_batch: &RecordBatch) -> Result<HashMap<String, String>, DeltaWriterError> {
    let partition_cols = metadata.partition_columns.as_slice();

    let mut partition_values = HashMap::new();

    for col_name in partition_cols.iter() {
        let arrow_schema = record_batch.schema();

        let i = arrow_schema.index_of(col_name)?;
        let col = record_batch.column(i); 

        let partition_string = stringified_partition_value(col)?;

        partition_values.insert(col_name.clone(), partition_string);
    }

    Ok(partition_values)
}

pub fn create_add(partition_values: &HashMap<String, String>, path: String, size: i64, record_batch: &RecordBatch) -> Result<Add, DeltaWriterError> {
    let stats = Stats {
        numRecords: record_batch.num_rows() as i64,
        // TODO: calculate additional stats
        // look at https://github.com/apache/arrow/blob/master/rust/arrow/src/compute/kernels/aggregate.rs for pulling these stats
        minValues: HashMap::new(),
        maxValues: HashMap::new(),
        nullCount: HashMap::new(),
    };
    let stats_string = serde_json::to_string(&stats).unwrap();

    let modification_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap();
    let modification_time= modification_time.as_millis() as i64;

    let add = Add {
        path,
        size,

        partitionValues: partition_values.to_owned(),
        partitionValues_parsed: None,

        modificationTime: modification_time,
        dataChange: true,

        // TODO: calculate additional stats 
        stats: Some(stats_string),
        stats_parsed: None,
        // ?
        tags: None,
    };

    Ok(add)
}

// very naive implementation for plucking the partition value from the first element of a column array.
// ideally, we would do some validation to ensure the record batch containing the passed partition column contains only distinct values.
// if we calculate stats _first_, we can avoid the extra iteration by ensuring max and min match for the column.
// however, stats are optional and can be added later with `dataChange` false log entries, and it may be more appropriate to add stats _later_ to speed up the initial write.
// a happy middle-road might be to compute stats for partition columns only on the initial write since we should validate partition values anyway, and compute additional stats later (at checkpoint time perhaps?).
// also this does not currently support nested partition columns and many other data types.
fn stringified_partition_value(arr: &Arc<dyn Array>) -> Result<String, DeltaWriterError> {
    let data_type = arr.data_type();

    let s = match data_type {
        DataType::Int8 => as_primitive_array::<Int8Type>(arr).value(0).to_string(),
        DataType::Int16 => as_primitive_array::<Int16Type>(arr).value(0).to_string(),
        DataType::Int32 => as_primitive_array::<Int32Type>(arr).value(0).to_string(),
        DataType::Int64 => as_primitive_array::<Int64Type>(arr).value(0).to_string(),
        DataType::UInt8 => as_primitive_array::<UInt8Type>(arr).value(0).to_string(),
        DataType::UInt16 => as_primitive_array::<UInt16Type>(arr).value(0).to_string(),
        DataType::UInt32 => as_primitive_array::<UInt32Type>(arr).value(0).to_string(),
        DataType::UInt64 => as_primitive_array::<UInt64Type>(arr).value(0).to_string(),
        DataType::Utf8 => {
            let data = arrow::array::as_string_array(arr);

            data.value(0).to_string()
        }
        // TODO: handle more types
        _ => {
            unimplemented!("Unimplemented data type: {:?}", data_type);
        }
    };

    Ok(s)
}