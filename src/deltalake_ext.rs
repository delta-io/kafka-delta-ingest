use arrow::{
    array::{
        as_boolean_array, as_primitive_array, as_struct_array, make_array, Array, ArrayData,
        StructArray,
    },
    buffer::MutableBuffer,
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    error::ArrowError,
    json::reader::Decoder,
    record_batch::*,
};
use chrono::prelude::*;
use deltalake::{
    action::{Add, ColumnCountStat, ColumnValueStat, Stats},
    DeltaDataTypeLong, DeltaDataTypeVersion, DeltaTable, DeltaTableError, DeltaTransactionError,
    Schema, StorageBackend, StorageError, UriError,
};
use log::debug;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, LogicalType},
    errors::ParquetError,
    file::{
        metadata::RowGroupMetaData, properties::WriterProperties, statistics::Statistics,
        writer::InMemoryWriteableCursor,
    },
    schema::types::{ColumnDescriptor, SchemaDescriptor},
};
use parquet_format::FileMetaData;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const NANOSECONDS: i64 = 1_000_000_000;

type MinAndMaxValues = (
    HashMap<String, ColumnValueStat>,
    HashMap<String, ColumnValueStat>,
);

type NullCounts = HashMap<String, ColumnCountStat>;

#[derive(thiserror::Error, Debug)]
pub enum DeltaWriterError {
    #[error("Missing partition column: {0}")]
    MissingPartitionColumn(String),

    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        record_batch_schema: SchemaRef,
        expected_schema: Arc<arrow::datatypes::Schema>,
    },

    #[error("{0}")]
    MissingMetadata(String),

    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    #[error("Record {0} is not a JSON object")]
    InvalidRecord(String),

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

    #[error("Delta transaction commit failed: {source}")]
    DeltaTransactionError {
        #[from]
        source: DeltaTransactionError,
    },
}

pub struct DeltaWriter {
    pub table: DeltaTable,
    storage: Box<dyn StorageBackend>,
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DeltaArrowWriter>,
}

pub struct DeltaArrowWriter {
    cursor: InMemoryWriteableCursor,
    arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    partition_values: HashMap<String, String>,
    null_counts: NullCounts,
    buffered_record_batch_count: usize,
}

impl DeltaArrowWriter {
    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many record batches and flushed after the appropriate number of bytes has been written.
    async fn write_record_batch(
        &mut self,
        partition_columns: &[String],
        record_batch: RecordBatch,
    ) -> Result<(), DeltaWriterError> {
        if self.partition_values.is_empty() {
            let partition_values = extract_partition_values(partition_columns, &record_batch)?;
            self.partition_values = partition_values;
        }

        self.arrow_writer.write(&record_batch)?;
        self.buffered_record_batch_count += 1;

        apply_null_counts(&record_batch.into(), &mut self.null_counts);
        Ok(())
    }
}

impl DeltaWriter {
    /// Initialize the writer from the given table path and delta schema
    pub async fn for_table_path(table_path: &str) -> Result<DeltaWriter, DeltaWriterError> {
        let table = deltalake::open_table(table_path).await?;
        let storage = deltalake::get_backend_for_uri(table_path)?;

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.get_metadata()?;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            table,
            storage,
            arrow_schema_ref,
            writer_properties,
            partition_columns,
            arrow_writers: HashMap::new(),
        })
    }

    /// Returns the last transaction version for the given app id recorded in the wrapped delta table.
    pub fn last_transaction_version(&self, app_id: &str) -> Option<DeltaDataTypeVersion> {
        let tx_versions = self.table.get_app_transaction_version();

        let v = tx_versions.get(app_id).map(|v| v.to_owned());

        debug!("Transaction version is {:?} for {}", v, app_id);

        v
    }

    /// Updates the wrapped delta table to load new delta log entries.
    pub async fn update_table(&mut self) -> Result<(), DeltaWriterError> {
        self.table.update().await?;
        Ok(())
    }

    /// Retrieves the latest schema from table, compares to the current and updates if changed.
    /// When schema is updated then `true` is returned which signals the caller that parquet
    /// created file or arrow batch should be revisited.
    pub fn update_schema(&mut self) -> Result<bool, DeltaWriterError> {
        let metadata = self.table.get_metadata()?;
        let schema: ArrowSchema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;

        let schema_updated = self.arrow_schema_ref.as_ref() != &schema
            || &self.partition_columns != &metadata.partition_columns;

        if schema_updated {
            let _ = std::mem::replace(&mut self.arrow_schema_ref, Arc::new(schema));
            let _ = std::mem::replace(
                &mut self.partition_columns,
                metadata.partition_columns.clone(),
            );
        }

        Ok(schema_updated)
    }

    /// Returns the table version of the wrapped delta table.
    pub fn table_version(&self) -> DeltaDataTypeVersion {
        self.table.version
    }

    pub async fn write(&mut self, values: Vec<Value>) -> Result<(), DeltaWriterError> {
        for (key, values) in self.divide_by_partition_values(values)? {
            let record_batch = record_batch_from_json(self.arrow_schema(), values)?;

            if record_batch.schema() != self.arrow_schema_ref {
                return Err(DeltaWriterError::SchemaMismatch {
                    record_batch_schema: record_batch.schema(),
                    expected_schema: self.arrow_schema_ref.clone(),
                });
            }

            match self.arrow_writers.get_mut(&key) {
                Some(writer) => {
                    writer
                        .write_record_batch(&self.partition_columns, record_batch)
                        .await?
                }
                None => {
                    let cursor = InMemoryWriteableCursor::default();
                    let arrow_writer = ArrowWriter::try_new(
                        cursor.clone(),
                        self.arrow_schema_ref.clone(),
                        Some(self.writer_properties.clone()),
                    )?;

                    let mut writer = DeltaArrowWriter {
                        cursor,
                        arrow_writer,
                        partition_values: HashMap::new(),
                        null_counts: NullCounts::new(),
                        buffered_record_batch_count: 0,
                    };

                    writer
                        .write_record_batch(&self.partition_columns, record_batch)
                        .await?;
                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        Ok(())
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    /// TODO https://github.com/delta-io/kafka-delta-ingest/issues/38
    pub fn buffer_len(&self) -> usize {
        self.arrow_writers
            .values()
            .map(|w| w.cursor.data().len())
            .sum()
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    pub async fn write_parquet_files(&mut self) -> Result<Vec<Add>, DeltaWriterError> {
        debug!("Writing parquet files.");

        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, mut writer) in writers {
            let metadata = writer.arrow_writer.close()?;

            let path = self.next_data_path(&self.partition_columns, &writer.partition_values)?;

            let obj_bytes = writer.cursor.data();
            let file_size = obj_bytes.len() as i64;

            let storage_path = self
                .storage
                .join_path(self.table.table_uri.as_str(), path.as_str());

            //
            // TODO: Wrap in retry loop to handle temporary network errors
            //

            self.storage
                .put_obj(&storage_path, obj_bytes.as_slice())
                .await?;

            debug!("Parquet file {} written.", &storage_path);

            // Replace self null_counts with an empty map. Use the other for stats.
            let null_counts = std::mem::take(&mut writer.null_counts);

            actions.push(create_add(
                &writer.partition_values,
                null_counts,
                path,
                file_size,
                &metadata,
            )?);
        }
        Ok(actions)
    }

    /// Returns the number of records held in the current buffer.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.arrow_writers
            .values()
            .map(|w| w.buffered_record_batch_count)
            .sum()
    }

    /// Resets internal state.
    pub fn reset(&mut self) {
        self.arrow_writers.clear();
    }

    /// Returns the arrow schema representation of the delta table schema defined for the wrapped
    /// table.
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.arrow_schema_ref.clone()
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &[String],
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
                let partition_value = partition_values
                    .get(k)
                    .ok_or(DeltaWriterError::MissingPartitionColumn(k.to_string()))?;

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

    fn divide_by_partition_values(
        &self,
        values: Vec<Value>,
    ) -> Result<HashMap<String, Vec<Value>>, DeltaWriterError> {
        let mut partition_values: HashMap<String, Vec<Value>> = HashMap::new();

        for value in values {
            let key = self.json_to_partition_keys(&value)?;
            match partition_values.get_mut(&key) {
                Some(vec) => vec.push(value),
                None => {
                    partition_values.insert(key, vec![value]);
                }
            };
        }

        Ok(partition_values)
    }

    fn json_to_partition_keys(&self, value: &Value) -> Result<String, DeltaWriterError> {
        if let Some(obj) = value.as_object() {
            let key: Vec<String> = self
                .partition_columns
                .iter()
                .map(|c| obj.get(c).unwrap_or(&Value::Null).to_string())
                .collect();
            return Ok(key.join("/"));
        }

        Err(DeltaWriterError::InvalidRecord(value.to_string()))
    }
}

/// Creates an Arrow RecordBatch from the passed JSON buffer.
pub fn record_batch_from_json(
    arrow_schema_ref: Arc<ArrowSchema>,
    json_buffer: Vec<Value>,
) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = json_buffer.len();
    let mut value_iter = json_buffer.iter().map(|j| Ok(j.to_owned()));
    let decoder = Decoder::new(arrow_schema_ref.clone(), row_count, None);
    decoder
        .next_batch(&mut value_iter)?
        .ok_or(DeltaWriterError::EmptyRecordBatch)
}

fn min_max_values_from_file_metadata(
    file_metadata: &FileMetaData,
) -> Result<MinAndMaxValues, ParquetError> {
    let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
    let schema_descriptor = type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))?;

    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();

    let row_group_metadata: Result<Vec<RowGroupMetaData>, ParquetError> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
        .collect();
    let row_group_metadata = row_group_metadata?;

    for i in 0..schema_descriptor.num_columns() {
        let column_descr = schema_descriptor.column(i);

        // If max rep level is > 0, this is an array element or a struct element of an array or something downstream of an array.
        // delta/databricks only computes null counts for arrays - not min max.
        // null counts are tracked at the record batch level, so skip any column with max_rep_level
        // > 0
        if column_descr.max_rep_level() > 0 {
            continue;
        }

        let statistics: Vec<&Statistics> = row_group_metadata
            .iter()
            .filter_map(|g| g.column(i).statistics())
            .collect();

        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        let _ = apply_min_max_for_column(
            statistics.as_slice(),
            column_descr.clone(),
            column_path_parts,
            &mut min_values,
            &mut max_values,
        )?;
    }

    Ok((min_values, max_values))
}

fn apply_null_counts(array: &StructArray, null_counts: &mut HashMap<String, ColumnCountStat>) {
    let fields = match array.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };

    array
        .columns()
        .iter()
        .zip(fields)
        .for_each(|(column, field)| {
            let key = field.name().to_owned();

            match column.data_type() {
                // Recursive case
                DataType::Struct(_) => {
                    let col_struct = null_counts
                        .entry(key)
                        .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

                    match col_struct {
                        ColumnCountStat::Column(map) => {
                            apply_null_counts(as_struct_array(column), map);
                        }
                        _ => unreachable!(),
                    }
                }
                // Base case
                _ => {
                    let col_struct = null_counts
                        .entry(key.clone())
                        .or_insert_with(|| ColumnCountStat::Value(0));

                    match col_struct {
                        ColumnCountStat::Value(n) => {
                            let null_count = column.null_count() as DeltaDataTypeLong;
                            let n = null_count + *n;
                            null_counts.insert(key, ColumnCountStat::Value(n));
                        }
                        _ => unreachable!(),
                    }
                }
            }
        });
}

fn apply_min_max_for_column(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
) -> Result<(), ParquetError> {
    match (column_path_parts.len(), column_path_parts.first()) {
        // Base case - we are at the leaf struct level in the path
        (1, _) => {
            let (min, max) = min_and_max_from_parquet_statistics(statistics, column_descr.clone())?;

            if let Some(min) = min {
                let min = ColumnValueStat::Value(min);
                min_values.insert(column_descr.name().to_string(), min);
            }

            if let Some(max) = max {
                let max = ColumnValueStat::Value(max);
                max_values.insert(column_descr.name().to_string(), max);
            }

            Ok(())
        }
        // Recurse to load value at the appropriate level of HashMap
        (_, Some(key)) => {
            let child_min_values = min_values
                .entry(key.to_owned())
                .or_insert(ColumnValueStat::Column(HashMap::new()));
            let child_max_values = max_values
                .entry(key.to_owned())
                .or_insert(ColumnValueStat::Column(HashMap::new()));

            match (child_min_values, child_max_values) {
                (ColumnValueStat::Column(mins), ColumnValueStat::Column(maxes)) => {
                    let remaining_parts: Vec<String> = column_path_parts
                        .iter()
                        .skip(1)
                        .map(|s| s.to_string())
                        .collect();

                    apply_min_max_for_column(
                        statistics,
                        column_descr,
                        remaining_parts.as_slice(),
                        mins,
                        maxes,
                    )?;

                    Ok(())
                }
                _ => {
                    unreachable!();
                }
            }
        }
        // column path parts will always have at least one element.
        (_, None) => {
            unreachable!();
        }
    }
}

fn min_and_max_from_parquet_statistics(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
) -> Result<(Option<Value>, Option<Value>), ParquetError> {
    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .map(|s| *s)
        .collect();

    if stats_with_min_max.len() == 0 {
        return Ok((None, None));
    }

    let (data_size, data_type) = match stats_with_min_max.first() {
        Some(Statistics::Boolean(_)) => (std::mem::size_of::<bool>(), DataType::Boolean),
        Some(Statistics::Int32(_)) => (std::mem::size_of::<i32>(), DataType::Int32),
        Some(Statistics::Int64(_)) => (std::mem::size_of::<i64>(), DataType::Int64),
        Some(Statistics::Float(_)) => (std::mem::size_of::<f32>(), DataType::Float32),
        Some(Statistics::Double(_)) => (std::mem::size_of::<f64>(), DataType::Float64),
        Some(Statistics::ByteArray(_)) if is_utf8(column_descr.logical_type()) => {
            (0, DataType::Utf8)
        }
        _ => {
            // NOTE: Skips
            // Statistics::Int96(_)
            // Statistics::ByteArray(_)
            // Statistics::FixedLenByteArray(_)

            return Ok((None, None));
        }
    };

    if data_type == DataType::Utf8 {
        return Ok(min_max_strings_from_stats(&stats_with_min_max));
    }

    let arrow_buffer_capacity = stats_with_min_max.len() * data_size;

    let min_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.min_bytes()).collect(),
    );

    let max_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.max_bytes()).collect(),
    );

    match data_type {
        DataType::Boolean => {
            let min = arrow::compute::min_boolean(as_boolean_array(&min_array));
            let min = min.map(|b| Value::Bool(b));

            let max = arrow::compute::max_boolean(as_boolean_array(&max_array));
            let max = max.map(|b| Value::Bool(b));

            Ok((min, max))
        }
        DataType::Int32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            Ok((min, max))
        }
        DataType::Int64 if is_timestamp(column_descr.logical_type()) => {
            let min_array = as_primitive_array::<arrow::datatypes::Int64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| timestamp_ns_to_json_value(i)).flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Int64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| timestamp_ns_to_json_value(i)).flatten();
            Ok((min, max))
        }
        DataType::Int64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            Ok((min, max))
        }
        DataType::Float32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min
                .map(|f| Number::from_f64(f as f64).map(|n| Value::Number(n)))
                .flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Float32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max
                .map(|f| Number::from_f64(f as f64).map(|n| Value::Number(n)))
                .flatten();

            Ok((min, max))
        }
        DataType::Float64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min
                .map(|f| Number::from_f64(f).map(|n| Value::Number(n)))
                .flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Float64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max
                .map(|f| Number::from_f64(f).map(|n| Value::Number(n)))
                .flatten();

            Ok((min, max))
        }
        _ => Ok((None, None)),
    }
}

#[inline]
fn is_utf8(opt: Option<LogicalType>) -> bool {
    match opt.as_ref() {
        Some(LogicalType::STRING(_)) => true,
        _ => false,
    }
}

#[inline]
fn is_timestamp(opt: Option<LogicalType>) -> bool {
    match opt.as_ref() {
        Some(LogicalType::TIMESTAMP(_)) => true,
        _ => false,
    }
}

fn min_max_strings_from_stats(
    stats_with_min_max: &Vec<&Statistics>,
) -> (Option<Value>, Option<Value>) {
    let min_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.min_bytes()).ok());

    let min_value = min_string_candidates
        .min()
        .map(|s| Value::String(s.to_string()));

    let max_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.max_bytes()).ok());

    let max_value = max_string_candidates
        .max()
        .map(|s| Value::String(s.to_string()));

    return (min_value, max_value);
}

#[inline]
pub fn timestamp_ns_to_json_value(ns: i64) -> Option<Value> {
    match Utc.timestamp_opt(ns / NANOSECONDS, (ns % NANOSECONDS) as u32) {
        chrono::offset::LocalResult::Single(dt) => Some(Value::String(format!("{:?}", dt))),
        _ => None,
    }
}

fn arrow_array_from_bytes(
    data_type: DataType,
    capacity: usize,
    byte_arrays: Vec<&[u8]>,
) -> Arc<dyn Array> {
    let mut buffer = MutableBuffer::new(capacity);

    for arr in byte_arrays.iter() {
        buffer.extend_from_slice(arr);
    }

    let builder = ArrayData::builder(data_type)
        .len(byte_arrays.len())
        .add_buffer(buffer.into());

    let data = builder.build();

    make_array(data)
}

fn create_add(
    partition_values: &HashMap<String, String>,
    null_counts: NullCounts,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
) -> Result<Add, DeltaWriterError> {
    let (min_values, max_values) = min_max_values_from_file_metadata(file_metadata)?;

    let stats = Stats {
        num_records: file_metadata.num_rows,
        min_values,
        max_values,
        null_count: null_counts,
    };

    let stats_string = serde_json::to_string(&stats)
        .or(Err(DeltaWriterError::StatsSerializationFailed { stats }))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,
        partition_values: partition_values.to_owned(),
        partition_values_parsed: None,
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        stats_parsed: None,
        tags: None,
    })
}

fn extract_partition_values(
    partition_cols: &[String],
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    #[tokio::test]
    async fn delta_stats_test() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path();
        create_temp_table(table_path);

        let mut writer = DeltaWriter::for_table_path(table_path.to_str().unwrap())
            .await
            .unwrap();

        writer.write(JSON_ROWS.clone()).await.unwrap();
        let add = writer.write_parquet_files().await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        let min_max_keys = vec!["meta", "some_int", "some_string", "some_bool", "date"];
        let mut null_count_keys = vec!["some_list", "some_nested_list"];
        null_count_keys.extend_from_slice(min_max_keys.as_slice());

        assert_eq!(min_max_keys.len(), stats.min_values.len());
        assert_eq!(min_max_keys.len(), stats.max_values.len());
        assert_eq!(null_count_keys.len(), stats.null_count.len());

        // assert on min values
        for (k, v) in stats.min_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(302, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert_eq!(false, v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("GET", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                _ => assert!(false, "Key should not be present"),
            }
        }

        // assert on max values
        for (k, v) in stats.max_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(1, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(400, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert_eq!(true, v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("PUT", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                _ => assert!(false, "Key should not be present"),
            }
        }

        // assert on null count
        for (k, v) in stats.null_count.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnCountStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition);

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!(0, timestamp);
                }
                ("some_int", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_bool", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_string", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_list", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_nested_list", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                ("date", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                _ => assert!(false, "Key should not be present"),
            }
        }
    }

    fn create_temp_table(table_path: &Path) {
        let log_path = table_path.join("_delta_log");

        let _ = std::fs::create_dir(log_path.as_path()).unwrap();
        let _ = std::fs::write(
            log_path.join("00000000000000000000.json"),
            V0_COMMIT.as_str(),
        )
        .unwrap();
    }

    lazy_static! {
        static ref SCHEMA: Value = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "meta",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "kafka",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "topic",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "partition",
                                            "type": "integer",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "offset",
                                            "type": "long",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "producer",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "timestamp",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            }
                        ]
                    },
                    "nullable": true, "metadata": {}
                },
                { "name": "some_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "some_int", "type": "integer", "nullable": true, "metadata": {} },
                { "name": "some_bool", "type": "boolean", "nullable": true, "metadata": {} },
                {
                    "name": "some_list",
                    "type": {
                        "type": "array",
                        "elementType": "string",
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "some_nested_list",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "array",
                            "elementType": "integer",
                            "containsNull": true
                        },
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
               },
               { "name": "date", "type": "string", "nullable": true, "metadata": {} },
            ]
        });
        static ref V0_COMMIT: String = {
            let schema_string = serde_json::to_string(&SCHEMA.clone()).unwrap();
            let jsons = [
                json!({
                    "protocol":{"minReaderVersion":1,"minWriterVersion":2}
                }),
                json!({
                    "metaData": {
                        "id": "22ef18ba-191c-4c36-a606-3dad5cdf3830",
                        "format": {
                            "provider": "parquet", "options": {}
                        },
                        "schemaString": schema_string,
                        "partitionColumns": ["date"], "configuration": {}, "createdTime": 1564524294376i64
                    }
                }),
            ];

            jsons
                .iter()
                .map(|j| serde_json::to_string(j).unwrap())
                .collect::<Vec<String>>()
                .join("\n")
                .to_string()
        };
        static ref JSON_ROWS: Vec<Value> = {
            std::iter::repeat(json!({
                "meta": {
                    "kafka": {
                        "offset": 0,
                        "partition": 0,
                        "topic": "some_topic"
                    },
                    "producer": {
                        "timestamp": "2021-06-22"
                    },
                },
                "some_string": "GET",
                "some_int": 302,
                "some_bool": true,
                "some_list": ["a", "b", "c"],
                "some_nested_list": [[42], [84]],
                "date": "2021-06-22",
            }))
            .take(100)
            .chain(
                std::iter::repeat(json!({
                    "meta": {
                        "kafka": {
                            "offset": 100,
                            "partition": 1,
                            "topic": "another_topic"
                        },
                        "producer": {
                            "timestamp": "2021-06-22"
                        },
                    },
                    "some_string": "PUT",
                    "some_int": 400,
                    "some_bool": false,
                    "some_list": ["x", "y", "z"],
                    "some_nested_list": [[42], [84]],
                    "date": "2021-06-22",
                }))
                .take(100),
            )
            .chain(
                std::iter::repeat(json!({
                    "meta": {
                        "kafka": {
                            "offset": 0,
                            "partition": 0,
                            "topic": "some_topic"
                        },
                        "producer": {
                            "timestamp": "2021-06-22"
                        },
                    },
                    "some_nested_list": [[42], null],
                    "date": "2021-06-22",
                }))
                .take(100),
            )
            .collect()
        };
    }
}
