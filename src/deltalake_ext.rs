use arrow::{
    array::{as_primitive_array, Array},
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    error::ArrowError,
    json::reader::Decoder,
    record_batch::RecordBatch,
};
use deltalake::{
    action::{Add, Stats},
    DeltaTableError, Schema, StorageBackend, StorageError, UriError,
};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    errors::ParquetError,
    file::{properties::WriterProperties, writer::InMemoryWriteableCursor},
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum DeltaWriterError {
    #[error("Missing partition column: {col_name}")]
    MissingPartitionColumn { col_name: String },

    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        record_batch_schema: SchemaRef,
        expected_schema: Arc<arrow::datatypes::Schema>,
    },

    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    // TODO: derive Debug for Stats in delta-rs
    #[error("Serialization of delta log statistics failed")]
    StatsSerializationFailed { stats: Stats },

    #[error("Invalid table path: {}", .source)]
    UriError {
        #[from]
        source: UriError,
    },

    #[error("Storage interaction failed: {source}")]
    Storage {
        #[from]
        source: StorageError,
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Arrow interaction failed: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Parquet write failed: {source}")]
    Parquet {
        #[from]
        source: ParquetError,
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
    pub async fn for_table_path_with_schema(
        table_path: String,
        schema: &Schema,
    ) -> Result<DeltaParquetWriter, DeltaWriterError> {
        // Initialize storage backend for the given table path
        let storage = deltalake::get_backend_for_uri(table_path.as_str())?;

        // Initialize an arrow schema ref from the delta table schema
        let arrow_schema = <ArrowSchema as From<&Schema>>::from(schema);
        let arrow_schema_ref = Arc::new(arrow_schema);

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        let cursor = InMemoryWriteableCursor::default();
        let arrow_writer = ArrowWriter::try_new(
            cursor.clone(),
            arrow_schema_ref.clone(),
            Some(writer_properties.clone()),
        )?;

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
    pub async fn write_record_batch(
        &mut self,
        partition_cols: &Vec<String>,
        record_batch: &RecordBatch,
    ) -> Result<(), DeltaWriterError> {
        let partition_values = extract_partition_values(partition_cols, record_batch)?;

        // Verify record batch schema matches the expected schema
        let schemas_match = record_batch.schema() == self.arrow_schema_ref;

        if !schemas_match {
            return Err(DeltaWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: self.arrow_schema_ref.clone(),
            });
        }

        // TODO: Verify that this RecordBatch contains partition values that agree with previous partition values (if any) for the current writer context
        self.partition_values = partition_values;

        // TODO: collect/update column stats
        // @nevi-me makes a fantastic recommendation to expose stats from the parquet crate if possible in https://github.com/delta-io/kafka-delta-ingest/pull/1#discussion_r585878860

        // write the record batch to the held arrow writer
        self.arrow_writer.write(record_batch)?;

        // increment num records (for the entire file) by the number of records in the handled batch
        self.num_records += record_batch.num_rows() as i64;

        Ok(())
    }

    /// Returns the current byte length of the in memory buffer.
    /// This should be used by the caller to decide when to call `complete_file`.
    pub fn buffer_len(&self) -> usize {
        self.cursor.data().len()
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    /// Returns an `Add` action representing the added file to be appended to the delta log.
    pub async fn complete_file(
        &mut self,
        partition_cols: &Vec<String>,
    ) -> Result<Add, DeltaWriterError> {
        // Close the arrow writer to flush remaining bytes and write the parquet footer
        self.arrow_writer.close()?;

        let path = self.next_data_path(partition_cols, &self.partition_values)?;

        let obj_bytes = self.cursor.data();
        let file_size = obj_bytes.len() as i64;

        let storage_path = self
            .storage
            .join_path(self.table_path.as_str(), path.as_str());

        //
        // TODO: Wrap in retry loop to handle temporary network errors
        //

        self.storage
            .put_obj(storage_path.as_str(), obj_bytes.as_slice())
            .await?;

        let num_records = self.num_records;

        // Re-initialize internal state to handle another file
        self.reset()?;

        // Create and return the "add" action associated with the completed file
        let add = create_add(&self.partition_values, path, file_size, num_records);

        add
    }

    fn reset(&mut self) -> Result<(), DeltaWriterError> {
        // Reset the internal cursor for the next file
        self.cursor = InMemoryWriteableCursor::default();
        // Reset the internal arrow writer for the next file
        self.arrow_writer = ArrowWriter::try_new(
            self.cursor.clone(),
            self.arrow_schema_ref.clone(),
            Some(self.writer_properties.clone()),
        )?;
        // Reset the record count to start from zero for the next file
        self.num_records = 0;

        Ok(())
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &Vec<String>,
        partition_values: &HashMap<String, String>,
    ) -> Result<String, DeltaWriterError> {
        // TODO: what does 00000 mean?
        let first_part = "00000";
        let uuid_part = Uuid::new_v4();
        // TODO: what does c000 mean?
        let last_part = "c000";

        // NOTE: If we add a non-snappy option, file name must change
        let file_name = format!(
            "part-{}-{}-{}.snappy.parquet",
            first_part, uuid_part, last_part
        );

        let data_path = if partition_cols.len() > 0 {
            let mut path_parts = vec![];

            for k in partition_cols.iter() {
                let partition_value =
                    partition_values
                        .get(k)
                        .ok_or(DeltaWriterError::MissingPartitionColumn {
                            col_name: k.to_string(),
                        })?;

                let part = format!("{}={}", k, partition_value);

                path_parts.push(part);
            }
            path_parts.push(file_name);
            path_parts.join("/")
        } else {
            file_name
        };

        Ok(data_path)
    }
}

struct InMemValueIter<'a> {
    buffer: &'a [Value],
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    fn from_vec(v: &'a [Value]) -> Self {
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

/// Creates an Arrow RecordBatch from the passed JSON buffer.
pub fn record_batch_from_json_buffer(
    arrow_schema_ref: Arc<ArrowSchema>,
    json_buffer: &[Value],
) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = json_buffer.len();
    let mut value_iter = InMemValueIter::from_vec(json_buffer);
    let decoder = Decoder::new(arrow_schema_ref.clone(), row_count, None);

    decoder
        .next_batch(&mut value_iter)?
        .ok_or(DeltaWriterError::EmptyRecordBatch)
}

fn extract_partition_values(
    partition_cols: &Vec<String>,
    record_batch: &RecordBatch,
) -> Result<HashMap<String, String>, DeltaWriterError> {
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

fn create_add(
    partition_values: &HashMap<String, String>,
    path: String,
    size: i64,
    num_records: i64,
) -> Result<Add, DeltaWriterError> {
    // Initialize a delta_rs stats object
    let stats = Stats {
        numRecords: num_records,
        // TODO: calculate additional stats
        // look at https://github.com/apache/arrow/blob/master/rust/arrow/src/compute/kernels/aggregate.rs for pulling these stats
        minValues: HashMap::new(),
        maxValues: HashMap::new(),
        nullCount: HashMap::new(),
    };
    // Derive a stats string to include in the add action
    let stats_string = serde_json::to_string(&stats)
        .or(Err(DeltaWriterError::StatsSerializationFailed { stats }))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,

        partitionValues: partition_values.to_owned(),
        partitionValues_parsed: None,

        modificationTime: modification_time,
        dataChange: true,

        // TODO: calculate additional stats
        stats: Some(stats_string),
        // TODO: implement stats_parsed for better performance gain
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-format
        stats_parsed: None,
        // ?
        tags: None,
    })
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
