use arrow::{
    array::{Array, as_primitive_array}, 
    datatypes::*,
    error::ArrowError,
    datatypes::Schema as ArrowSchema,
    json::reader::Decoder,
    record_batch::RecordBatch,
};
use deltalake::{DeltaTableError, DeltaTableMetaData, Schema, StorageBackend, StorageError, UriError, 
    action::{Add, Stats}
};
use log::{info, warn};
use parquet::{arrow::ArrowWriter, basic::Compression, errors::ParquetError, file::{properties::WriterProperties, writer::{InMemoryWriteableCursor}}};
use serde_json::Value;
use std::{collections::HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

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

pub struct DeltaParquetWriter {
    table_path: String,
    storage: Box<dyn StorageBackend>,
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    writer_properties: WriterProperties,
    cursor: InMemoryWriteableCursor,
    arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    partition_values: HashMap<String, String>,
    num_records: i64,
}

/// A writer that writes record batches in parquet format to a table location.
/// This should be used along side a DeltaTransaction wrapping the same DeltaTable instance.
impl DeltaParquetWriter {
    /// Initialize the writer from the given table path and delta schema
    pub async fn for_table_path_with_schema(table_path: String, schema: &Schema) -> Result<DeltaParquetWriter, DeltaWriterError> {
        // Initialize storage backend for the given table path
        let storage = deltalake::get_backend_for_uri(table_path.as_str())?;

        // Initialize an arrow schema ref from the delta table schema
        let arrow_schema = <ArrowSchema as From<&Schema>>::from(schema);
        let arrow_schema_ref = Arc::new(arrow_schema);

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // TODO: Extract config/env for writer properties and set more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        let cursor = InMemoryWriteableCursor::default();
        // TODO: Handle error
        let arrow_writer = ArrowWriter::try_new(cursor.clone(), arrow_schema_ref.clone(), Some(writer_properties.clone())).unwrap();

        Ok(Self {
            table_path,
            storage,
            arrow_schema_ref,
            writer_properties,
            cursor,
            arrow_writer,
            partition_values: HashMap::new(),
            num_records: 0,
        })
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many record batches and flushed after the appropriate number of bytes has been written.
    pub async fn write_record_batch(&mut self, partition_cols: &Vec<String>, record_batch: &RecordBatch) -> Result<(), DeltaWriterError> {
        let partition_values = extract_partition_values(partition_cols, record_batch)?;

        // TODO: Verify that this RecordBatch contains partition values that agree with previous partition values (if any) for the current writer context
        self.partition_values = partition_values;

        // TODO: collect/update column stats

        // write the record batch to the held arrow writer
        self.arrow_writer.write(record_batch)?;

        // increment num records (for the entire file) by the number of records in the handled batch
        self.num_records += record_batch.num_rows() as i64;

        Ok(())
    }

    /// Returns the current byte length of the in memory buffer.
    /// This should be used by the caller to decide when to call `complete_file`.
    pub fn buffer_len(&self) -> Result<usize, DeltaWriterError> {
        Ok(self.cursor.data().len())
    }
    
    /// Writes the existing parquet bytes to storage and resets internal state to handle another round.
    /// Returns an `Add` action representing the added file to be appended to the delta log.
    pub async fn complete_file(&mut self, partition_cols: &Vec<String>) -> Result<Add, DeltaWriterError> {
        let path = self.next_data_path(partition_cols, &self.partition_values).unwrap();

        let obj_bytes = self.cursor.data();
        let file_size = obj_bytes.len() as i64;

        let storage_path = format!("{}/{}", self.table_path, path);

        self.storage.put_obj(storage_path.as_str(), obj_bytes.as_slice()).await?;

        // Close the arrow writer to flush remaining bytes and write the parquet footer
        self.arrow_writer.close()?;

        let num_records = self.num_records;
 
        // Re-initialize state to handle another file
        self.reset();

        // Create and return the "add" action associated with the completed file
        let add = create_add(&self.partition_values, path, file_size, num_records);

        add
    }

    fn reset(&mut self) {
        self.cursor = InMemoryWriteableCursor::default();
        // TODO: Handle error
        self.arrow_writer = ArrowWriter::try_new(self.cursor.clone(), self.arrow_schema_ref.clone(), Some(self.writer_properties.clone())).unwrap();
        self.num_records = 0;
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(&self, partition_cols: &Vec<String>, partition_values: &HashMap<String, String>) -> Result<String, DeltaWriterError> {
        // TODO: what does 00000 mean?
        let first_part = "00000";
        let uuid_part = Uuid::new_v4();
        // TODO: what does c000 mean?
        let last_part = "c000";

        let file_name = format!("part-{}-{}-{}.parquet", first_part, uuid_part, last_part);

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

struct InMemValueIter<'a> {
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

fn extract_partition_values(partition_cols: &Vec<String>, record_batch: &RecordBatch) -> Result<HashMap<String, String>, DeltaWriterError> {
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

fn create_add(partition_values: &HashMap<String, String>, path: String, size: i64, num_records: i64) -> Result<Add, DeltaWriterError> {
    let stats = Stats {
        numRecords: num_records,
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