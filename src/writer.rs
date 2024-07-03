//! High-level writer implementations for [`deltalake`].
#[allow(deprecated)]
use deltalake_core::arrow::{
    array::{
        as_boolean_array, as_primitive_array, as_struct_array, make_array, Array, ArrayData,
        StructArray,
    },
    buffer::MutableBuffer,
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    error::ArrowError,
    json::reader::ReaderBuilder,
    record_batch::*,
};
use deltalake_core::parquet::{
    arrow::ArrowWriter,
    basic::{Compression, LogicalType},
    errors::ParquetError,
    file::{metadata::RowGroupMetaData, properties::WriterProperties, statistics::Statistics},
    format::TimeUnit,
    schema::types::{ColumnDescriptor, SchemaDescriptor},
};
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{
    kernel::{Action, Add, Schema},
    protocol::{ColumnCountStat, ColumnValueStat, Stats},
    storage::ObjectStoreRef,
    DeltaTable, DeltaTableError, ObjectStoreError,
};
use deltalake_core::{operations::transaction::TableReference, parquet::format::FileMetaData};
use log::{error, info, warn};
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::cursor::InMemoryWriteableCursor;

const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

type MinAndMaxValues = (
    HashMap<String, ColumnValueStat>,
    HashMap<String, ColumnValueStat>,
);

type NullCounts = HashMap<String, ColumnCountStat>;

/// Enum representing an error when calling [`DataWriter`].
#[derive(thiserror::Error, Debug)]
pub enum DataWriterError {
    /// Partition column is missing in a record written to delta.
    #[error("Missing partition column: {0}")]
    MissingPartitionColumn(String),

    /// The Arrow RecordBatch schema does not match the expected schema.
    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        /// The record batch schema.
        record_batch_schema: SchemaRef,
        /// The schema of the target delta table.
        expected_schema: Arc<ArrowSchema>,
    },

    /// An Arrow RecordBatch could not be created from the JSON buffer.
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    /// A record was written that was not a JSON object.
    #[error("Record {0} is not a JSON object")]
    InvalidRecord(String),

    /// Indicates that a partial write was performed and error records were discarded.
    #[error("Failed to write some values to parquet. Sample error: {sample_error}.")]
    PartialParquetWrite {
        /// Vec of tuples where the first element of each tuple is the skipped value and the second element is the [`ParquetError`] associated with it.
        skipped_values: Vec<(Value, ParquetError)>,
        /// A sample [`ParquetError`] representing the overall partial write.
        sample_error: ParquetError,
    },

    // TODO: derive Debug for Stats in delta-rs
    /// Serialization of delta log statistics failed.
    #[error("Serialization of delta log statistics failed")]
    StatsSerializationFailed {
        /// The stats object that failed serialization.
        stats: Stats,
    },

    /// Invalid table paths was specified for the delta table.
    #[error("Invalid table path: {}", .source)]
    UriError {
        /// The wrapped [`url::ParseError`].
        #[from]
        source: url::ParseError,
    },

    /// deltalake storage backend returned an error.
    #[error("Storage interaction failed: {source}")]
    Storage {
        /// The wrapped [`ObjectStoreError`]
        #[from]
        source: ObjectStoreError,
    },

    /// DeltaTable returned an error.
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// The wrapped [`DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Arrow returned an error.
    #[error("Arrow interaction failed: {source}")]
    Arrow {
        /// The wrapped [`ArrowError`]
        #[from]
        source: ArrowError,
    },

    /// Parquet write failed.
    #[error("Parquet write failed: {source}")]
    Parquet {
        /// The wrapped [`ParquetError`]
        #[from]
        source: ParquetError,
    },

    /// Error returned from std::io
    #[error("std::io::Error: {source}")]
    Io {
        /// The wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },
}

impl From<ParquetError> for Box<DataWriterError> {
    fn from(value: ParquetError) -> Self {
        Box::new(DataWriterError::Parquet { source: value })
    }
}

impl From<ObjectStoreError> for Box<DataWriterError> {
    fn from(value: ObjectStoreError) -> Self {
        Box::new(DataWriterError::Storage { source: value })
    }
}

impl From<ArrowError> for Box<DataWriterError> {
    fn from(value: ArrowError) -> Self {
        Box::new(DataWriterError::Arrow { source: value })
    }
}

impl From<std::io::Error> for Box<DataWriterError> {
    fn from(value: std::io::Error) -> Self {
        Box::new(DataWriterError::Io { source: value })
    }
}

impl From<DeltaTableError> for Box<DataWriterError> {
    fn from(value: DeltaTableError) -> Self {
        Box::new(DataWriterError::DeltaTable { source: value })
    }
}

/// Writes messages to a delta lake table.
pub struct DataWriter {
    storage: ObjectStoreRef,
    arrow_schema_ref: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DataArrowWriter>,
}

/// Writes messages to an underlying arrow buffer.
pub(crate) struct DataArrowWriter {
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    cursor: InMemoryWriteableCursor,
    arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    partition_values: HashMap<String, Option<String>>,
    null_counts: NullCounts,
    buffered_record_batch_count: usize,
}

impl DataArrowWriter {
    /// Writes the given JSON buffer and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many json buffers and flushed after the appropriate number of bytes has been written.
    async fn write_values(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        json_buffer: Vec<Value>,
    ) -> Result<(), Box<DataWriterError>> {
        let record_batch = record_batch_from_json(arrow_schema.clone(), json_buffer.as_slice())?;

        if record_batch.schema() != arrow_schema {
            return Err(Box::new(DataWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: arrow_schema,
            }));
        }

        let result = self
            .write_record_batch(partition_columns, record_batch)
            .await;

        match result {
            Err(e) => match *e {
                DataWriterError::Parquet { source } => {
                    self.write_partial(partition_columns, arrow_schema, json_buffer, source)
                        .await
                }
                _ => Err(e),
            },
            Ok(_) => result,
        }
    }

    async fn write_partial(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        json_buffer: Vec<Value>,
        parquet_error: ParquetError,
    ) -> Result<(), Box<DataWriterError>> {
        warn!("Failed with parquet error while writing record batch. Attempting quarantine of bad records.");
        let (good, bad) = quarantine_failed_parquet_rows(arrow_schema.clone(), json_buffer)?;
        let record_batch = record_batch_from_json(arrow_schema, good.as_slice())?;
        self.write_record_batch(partition_columns, record_batch)
            .await?;
        info!(
            "Wrote {} good records to record batch and quarantined {} bad records.",
            good.len(),
            bad.len()
        );
        Err(Box::new(DataWriterError::PartialParquetWrite {
            skipped_values: bad,
            sample_error: parquet_error,
        }))
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many record batches and flushed after the appropriate number of bytes has been written.
    async fn write_record_batch(
        &mut self,
        partition_columns: &[String],
        record_batch: RecordBatch,
    ) -> Result<(), Box<DataWriterError>> {
        if self.partition_values.is_empty() {
            let partition_values = extract_partition_values(partition_columns, &record_batch)?;
            self.partition_values = partition_values;
        }

        // Copy current cursor bytes so we can recover from failures
        let current_cursor_bytes = self.cursor.data();

        let result = self.arrow_writer.write(&record_batch);
        self.arrow_writer.flush()?;

        match result {
            Ok(_) => {
                self.buffered_record_batch_count += 1;

                apply_null_counts(
                    partition_columns,
                    &record_batch.into(),
                    &mut self.null_counts,
                    0,
                );
                Ok(())
            }
            // If a write fails we need to reset the state of the DeltaArrowWriter
            Err(e) => {
                let new_cursor = Self::cursor_from_bytes(current_cursor_bytes.as_slice())?;
                let _ = std::mem::replace(&mut self.cursor, new_cursor.clone());
                let arrow_writer = Self::new_underlying_writer(
                    new_cursor,
                    self.arrow_schema.clone(),
                    self.writer_properties.clone(),
                )?;
                let _ = std::mem::replace(&mut self.arrow_writer, arrow_writer);
                self.partition_values.clear();

                Err(e.into())
            }
        }
    }

    fn new(
        arrow_schema: Arc<ArrowSchema>,
        writer_properties: WriterProperties,
    ) -> Result<Self, ParquetError> {
        let cursor = InMemoryWriteableCursor::default();
        let arrow_writer = Self::new_underlying_writer(
            cursor.clone(),
            arrow_schema.clone(),
            writer_properties.clone(),
        )?;

        let partition_values = HashMap::new();
        let null_counts = NullCounts::new();
        let buffered_record_batch_count = 0;

        Ok(Self {
            arrow_schema,
            writer_properties,
            cursor,
            arrow_writer,
            partition_values,
            null_counts,
            buffered_record_batch_count,
        })
    }

    fn cursor_from_bytes(bytes: &[u8]) -> Result<InMemoryWriteableCursor, std::io::Error> {
        let mut cursor = InMemoryWriteableCursor::default();
        cursor.write_all(bytes)?;
        Ok(cursor)
    }

    fn new_underlying_writer(
        cursor: InMemoryWriteableCursor,
        arrow_schema: Arc<ArrowSchema>,
        writer_properties: WriterProperties,
    ) -> Result<ArrowWriter<InMemoryWriteableCursor>, ParquetError> {
        ArrowWriter::try_new(cursor, arrow_schema, Some(writer_properties))
    }
}

impl DataWriter {
    /// Creates a DataWriter to write to the given table
    pub fn for_table(
        table: &DeltaTable,
        _options: HashMap<String, String>, // XXX: figure out if this is necessary
    ) -> Result<DataWriter, Box<DataWriterError>> {
        let storage = table.object_store();

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.metadata()?;
        let arrow_schema = ArrowSchema::try_from(table.schema().unwrap())?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage,
            arrow_schema_ref,
            writer_properties,
            partition_columns,
            arrow_writers: HashMap::new(),
        })
    }

    /// Retrieves the latest schema from table, compares to the current and updates if changed.
    /// When schema is updated then `true` is returned which signals the caller that parquet
    /// created file or arrow batch should be revisited.
    pub fn update_schema(&mut self, table: &DeltaTable) -> Result<bool, Box<DataWriterError>> {
        let metadata = table.metadata().unwrap();
        let schema: ArrowSchema =
            <ArrowSchema as TryFrom<&Schema>>::try_from(table.schema().unwrap())?;

        let schema_updated = self.arrow_schema_ref.as_ref() != &schema
            || self.partition_columns != metadata.partition_columns;

        if schema_updated {
            let _ = std::mem::replace(&mut self.arrow_schema_ref, Arc::new(schema));
            let _ = std::mem::replace(
                &mut self.partition_columns,
                metadata.partition_columns.clone(),
            );
        }

        Ok(schema_updated)
    }

    /// Writes the given values to internal parquet buffers for each represented partition.
    pub async fn write(&mut self, values: Vec<Value>) -> Result<(), Box<DataWriterError>> {
        let mut partial_writes: Vec<(Value, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, values) in self.divide_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => collect_partial_write_failure(
                    &mut partial_writes,
                    writer
                        .write_values(&self.partition_columns, arrow_schema.clone(), values)
                        .await,
                )?,
                None => {
                    let mut writer =
                        DataArrowWriter::new(arrow_schema.clone(), self.writer_properties.clone())?;

                    collect_partial_write_failure(
                        &mut partial_writes,
                        writer
                            .write_values(&self.partition_columns, self.arrow_schema(), values)
                            .await,
                    )?;

                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            let sample = partial_writes[0].1.to_string();
            return Err(Box::new(DataWriterError::PartialParquetWrite {
                skipped_values: partial_writes,
                sample_error: ParquetError::General(sample),
            }));
        }

        Ok(())
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.arrow_writers
            .values()
            .map(|w| {
                let l: i64 = w
                    .arrow_writer
                    .flushed_row_groups()
                    .iter()
                    .map(|rg| rg.total_byte_size())
                    .sum();
                l as usize
            })
            .sum()
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    pub async fn write_parquet_files(&mut self, _: &str) -> Result<Vec<Add>, Box<DataWriterError>> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, mut writer) in writers {
            let metadata = writer.arrow_writer.close()?;

            let path = self.next_data_path(&self.partition_columns, &writer.partition_values)?;

            let obj_bytes = writer.cursor.data();
            let file_size = obj_bytes.len() as i64;

            //
            // TODO: Wrap in retry loop to handle temporary network errors
            //

            self.storage
                .put(
                    &deltalake_core::Path::parse(&path).unwrap(),
                    bytes::Bytes::copy_from_slice(obj_bytes.as_slice()).into(),
                )
                .await?;

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
    pub fn arrow_schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema_ref.clone()
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &[String],
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<String, Box<DataWriterError>> {
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

        let data_path = if !partition_cols.is_empty() {
            let mut path_parts = vec![];

            for k in partition_cols.iter() {
                let partition_value = partition_values
                    .get(k)
                    .ok_or_else(|| DataWriterError::MissingPartitionColumn(k.to_string()))?;

                let partition_value = partition_value
                    .as_deref()
                    .unwrap_or(NULL_PARTITION_VALUE_DATA_PATH);
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
        records: Vec<Value>,
    ) -> Result<HashMap<String, Vec<Value>>, Box<DataWriterError>> {
        let mut partitioned_records: HashMap<String, Vec<Value>> = HashMap::new();

        for record in records {
            let partition_value = self.json_to_partition_values(&record)?;
            match partitioned_records.get_mut(&partition_value) {
                Some(vec) => vec.push(record),
                None => {
                    partitioned_records.insert(partition_value, vec![record]);
                }
            };
        }

        Ok(partitioned_records)
    }

    fn json_to_partition_values(&self, value: &Value) -> Result<String, Box<DataWriterError>> {
        if let Some(obj) = value.as_object() {
            let key: Vec<String> = self
                .partition_columns
                .iter()
                .map(|c| obj.get(c).unwrap_or(&Value::Null).to_string())
                .collect();
            return Ok(key.join("/"));
        }

        Err(Box::new(DataWriterError::InvalidRecord(value.to_string())))
    }

    /// Inserts the given values immediately into the delta table.
    // TODO: Re-using existing methods for now, but this method is batch oriented. We may be able to create a more streamlined implementation for batch writes.
    pub async fn insert_all(
        &mut self,
        table: &mut DeltaTable,
        values: Vec<Value>,
    ) -> Result<i64, Box<DataWriterError>> {
        self.write(values).await?;
        let mut adds = self.write_parquet_files(&table.table_uri()).await?;
        let actions = adds.drain(..).map(Action::Add).collect();
        let commit = deltalake_core::operations::transaction::CommitBuilder::default()
            .with_actions(actions)
            .build(
                table.state.as_ref().map(|s| s as &dyn TableReference),
                table.log_store().clone(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: Some(self.partition_columns.clone()),
                    predicate: None,
                },
            )
            .await
            .map_err(DeltaTableError::from)?;
        Ok(commit.version)
    }
}

/// Creates an Arrow RecordBatch from the passed JSON buffer.
pub fn record_batch_from_json(
    arrow_schema: Arc<ArrowSchema>,
    json: &[Value],
) -> Result<RecordBatch, Box<DataWriterError>> {
    let mut decoder = ReaderBuilder::new(arrow_schema).build_decoder()?;
    decoder.serialize(json)?;
    decoder
        .flush()?
        .ok_or(Box::new(DataWriterError::EmptyRecordBatch))
}

type BadValue = (Value, ParquetError);

fn quarantine_failed_parquet_rows(
    arrow_schema: Arc<ArrowSchema>,
    values: Vec<Value>,
) -> Result<(Vec<Value>, Vec<BadValue>), Box<DataWriterError>> {
    let mut good: Vec<Value> = Vec::new();
    let mut bad: Vec<BadValue> = Vec::new();

    for value in values {
        let record_batch = record_batch_from_json(arrow_schema.clone(), &[value.clone()])?;

        let cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(cursor.clone(), arrow_schema.clone(), None)?;

        match writer.write(&record_batch) {
            Ok(_) => good.push(value),
            Err(e) => bad.push((value, e)),
        }
    }

    Ok((good, bad))
}

fn collect_partial_write_failure(
    partial_writes: &mut Vec<(Value, ParquetError)>,
    writer_result: Result<(), Box<DataWriterError>>,
) -> Result<(), Box<DataWriterError>> {
    match writer_result {
        Err(e) => match *e {
            DataWriterError::PartialParquetWrite { skipped_values, .. } => {
                partial_writes.extend(skipped_values);
                Ok(())
            }
            _ => Err(e),
        },
        _ => writer_result,
    }
}

fn min_max_values_from_file_metadata(
    partition_values: &HashMap<String, Option<String>>,
    file_metadata: &FileMetaData,
) -> Result<MinAndMaxValues, ParquetError> {
    let type_ptr =
        deltalake_core::parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
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

        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        // Do not include partition columns in statistics
        if partition_values.contains_key(&column_path_parts[0]) {
            continue;
        }

        let statistics: Vec<&Statistics> = row_group_metadata
            .iter()
            .filter_map(|g| g.column(i).statistics())
            .collect();

        apply_min_max_for_column(
            statistics.as_slice(),
            column_descr.clone(),
            column_path_parts,
            &mut min_values,
            &mut max_values,
        )?;
    }

    Ok((min_values, max_values))
}

fn apply_null_counts(
    partition_columns: &[String],
    array: &StructArray,
    null_counts: &mut HashMap<String, ColumnCountStat>,
    nest_level: i32,
) {
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

            // Do not include partition columns in statistics
            if nest_level == 0 && partition_columns.contains(&key) {
                return;
            }

            apply_null_counts_for_column(
                partition_columns,
                null_counts,
                nest_level,
                &column,
                field,
            );
        });
}

fn apply_null_counts_for_column(
    partition_columns: &[String],
    null_counts: &mut HashMap<String, ColumnCountStat>,
    nest_level: i32,
    column: &&Arc<dyn Array>,
    field: &Field,
) {
    let key = field.name().to_owned();

    match column.data_type() {
        // Recursive case
        DataType::Struct(_) => {
            let col_struct = null_counts
                .entry(key)
                .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

            match col_struct {
                ColumnCountStat::Column(map) => {
                    apply_null_counts(
                        partition_columns,
                        as_struct_array(*column),
                        map,
                        nest_level + 1,
                    );
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
                    let null_count = column.null_count() as i64;
                    let n = null_count + *n;
                    null_counts.insert(key, ColumnCountStat::Value(n));
                }
                _ => unreachable!(),
            }
        }
    }
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
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_max_values = max_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));

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
    use deltalake_core::arrow::compute::*;

    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .copied()
        .collect();

    if stats_with_min_max.is_empty() {
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
    )?;

    let max_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.max_bytes()).collect(),
    )?;

    match data_type {
        DataType::Boolean => {
            let min = min_boolean(as_boolean_array(&min_array));
            let min = min.map(Value::Bool);

            let max = max_boolean(as_boolean_array(&max_array));
            let max = max.map(Value::Bool);

            Ok((min, max))
        }
        DataType::Int32 => {
            let min_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Int32Type>(&min_array);
            let min = min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Int32Type>(&max_array);
            let max = max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            Ok((min, max))
        }
        DataType::Int64 => {
            let min_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Int64Type>(&min_array);
            let min = min(min_array);
            let max_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Int64Type>(&max_array);
            let max = max(max_array);

            match column_descr.logical_type().as_ref() {
                Some(LogicalType::Timestamp { unit, .. }) => {
                    let max = max
                        .and_then(|n| timestamp_to_delta_stats_string(n, unit))
                        .map(Value::String);
                    let min = min
                        .and_then(|n| timestamp_to_delta_stats_string(n, unit))
                        .map(Value::String);
                    Ok((min, max))
                }
                _ => {
                    let min = min.map(|i| Value::Number(Number::from(i)));
                    let max = max.map(|i| Value::Number(Number::from(i)));

                    Ok((min, max))
                }
            }
        }
        DataType::Float32 => {
            let min_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Float32Type>(&min_array);
            let min = min(min_array);
            let min = min.and_then(|f| Number::from_f64(f as f64).map(Value::Number));

            let max_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Float32Type>(&max_array);
            let max = max(max_array);
            let max = max.and_then(|f| Number::from_f64(f as f64).map(Value::Number));

            Ok((min, max))
        }
        DataType::Float64 => {
            let min_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Float64Type>(&min_array);
            let min = min(min_array);
            let min = min.and_then(|f| Number::from_f64(f).map(Value::Number));

            let max_array =
                as_primitive_array::<deltalake_core::arrow::datatypes::Float64Type>(&max_array);
            let max = max(max_array);
            let max = max.and_then(|f| Number::from_f64(f).map(Value::Number));

            Ok((min, max))
        }
        _ => Ok((None, None)),
    }
}

#[inline]
fn is_utf8(opt: Option<LogicalType>) -> bool {
    matches!(opt.as_ref(), Some(LogicalType::String))
}

fn min_max_strings_from_stats(
    stats_with_min_max: &[&Statistics],
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

    (min_value, max_value)
}

fn arrow_array_from_bytes(
    data_type: DataType,
    capacity: usize,
    byte_arrays: Vec<&[u8]>,
) -> Result<Arc<dyn Array>, ArrowError> {
    let mut buffer = MutableBuffer::new(capacity);

    for arr in byte_arrays.iter() {
        buffer.extend_from_slice(arr);
    }

    let builder = ArrayData::builder(data_type)
        .len(byte_arrays.len())
        .add_buffer(buffer.into());

    let data = builder.build()?;

    Ok(make_array(data))
}

fn create_add(
    partition_values: &HashMap<String, Option<String>>,
    null_counts: NullCounts,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
) -> Result<Add, Box<DataWriterError>> {
    let (min_values, max_values) =
        min_max_values_from_file_metadata(partition_values, file_metadata)?;

    let stats = Stats {
        num_records: file_metadata.num_rows,
        min_values,
        max_values,
        null_count: null_counts,
    };

    let stats_string = serde_json::to_string(&stats)
        .or(Err(DataWriterError::StatsSerializationFailed { stats }))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,
        partition_values: partition_values.to_owned(),
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        stats_parsed: None,
        tags: None,
        ..Default::default()
    })
}

fn extract_partition_values(
    partition_cols: &[String],
    record_batch: &RecordBatch,
) -> Result<HashMap<String, Option<String>>, Box<DataWriterError>> {
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
fn stringified_partition_value(
    arr: &Arc<dyn Array>,
) -> Result<Option<String>, Box<DataWriterError>> {
    let data_type = arr.data_type();

    if arr.is_null(0) {
        return Ok(None);
    }

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
            let data = deltalake_core::arrow::array::as_string_array(arr);

            data.value(0).to_string()
        }
        // TODO: handle more types
        _ => {
            unimplemented!("Unimplemented data type: {:?}", data_type);
        }
    };

    Ok(Some(s))
}

/// Vendored from delta-rs since it's no longer a public API
fn timestamp_to_delta_stats_string(n: i64, time_unit: &TimeUnit) -> Option<String> {
    use deltalake_core::arrow::temporal_conversions;

    let dt = match time_unit {
        TimeUnit::MILLIS(_) => temporal_conversions::timestamp_ms_to_datetime(n),
        TimeUnit::MICROS(_) => temporal_conversions::timestamp_us_to_datetime(n),
        TimeUnit::NANOS(_) => temporal_conversions::timestamp_ns_to_datetime(n),
    }?;

    Some(format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ")))
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

        let table = crate::delta_helpers::load_table(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap();
        let mut writer = DataWriter::for_table(&table, HashMap::new()).unwrap();

        writer.write(JSON_ROWS.clone()).await.unwrap();
        let add = writer
            .write_parquet_files(&table.table_uri())
            .await
            .unwrap();
        assert_eq!(add.len(), 1);
        let stats: deltalake_core::protocol::Stats =
            serde_json::from_str(&add[0].stats.as_ref().unwrap()).expect("Failed to parse stats");

        let min_max_keys = vec!["meta", "some_int", "some_string", "some_bool"];
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
                ("some_bool", ColumnValueStat::Value(v)) => assert!(!v.as_bool().unwrap()),
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
                ("some_bool", ColumnValueStat::Value(v)) => assert!(v.as_bool().unwrap()),
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

        std::fs::create_dir(log_path.as_path()).unwrap();
        std::fs::write(
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
