use arrow::datatypes::Schema as ArrowSchema;
use deltalake::Schema;
use kafka_delta_ingest::deltalake_ext::record_batch_from_json;
use parquet::{
    arrow::ArrowWriter,
    basic::LogicalType,
    errors::ParquetError,
    file::{
        metadata::{ColumnChunkMetaData, RowGroupMetaData},
        statistics::Statistics,
        writer::InMemoryWriteableCursor,
    },
    schema::types::{ColumnDescriptor, SchemaDescriptor},
};
use parquet_format::FileMetaData;
use serde_json::{json, Number, Value};
use std::convert::TryFrom;
use std::sync::Arc;

#[tokio::test]
async fn inspect_parquet() {
    let delta_schema = create_test_schema().unwrap();
    let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&delta_schema).unwrap();
    let arrow_schema_ref = Arc::new(arrow_schema);

    let cursor = InMemoryWriteableCursor::default();
    let mut arrow_writer =
        ArrowWriter::try_new(cursor.clone(), arrow_schema_ref.clone(), None).unwrap();

    let data = some_data();
    let middle = data.len() / 2;

    let batch1: Vec<Value> = data.iter().take(middle).map(|v| v.to_owned()).collect();
    let batch2: Vec<Value> = data.iter().skip(middle).map(|v| v.to_owned()).collect();

    let batch1 = record_batch_from_json(arrow_schema_ref.clone(), batch1.as_slice()).unwrap();
    let batch2 = record_batch_from_json(arrow_schema_ref.clone(), batch2.as_slice()).unwrap();

    let _ = arrow_writer.write(&batch1).unwrap();
    let _ = arrow_writer.write(&batch2).unwrap();

    let file_metadata = arrow_writer.close().unwrap();

    handle_file_metadata(&file_metadata);
}

fn handle_file_metadata(file_metadata: &FileMetaData) {
    let schema_descriptor = schema_descriptor_from_file_metadata(file_metadata).unwrap();
    let leaves = schema_descriptor.columns();

    for row_group in file_metadata.row_groups.iter() {
        let rg =
            RowGroupMetaData::from_thrift(schema_descriptor.clone(), row_group.clone()).unwrap();

        for i in 0..leaves.len() {
            let column_metadata = rg.column(i);
            let column_descr_ptr = leaves[i].clone();

            let column = &row_group.columns[i];

            if let Some(statistics) = column_metadata.statistics() {
                handle_column(column_metadata, column_descr_ptr);
            }
        }

        break;
    }
}

fn handle_column(column_metadata: &ColumnChunkMetaData, column_descr: Arc<ColumnDescriptor>) {
    println!(
        "Column path: {}, Physical type: {}, Logical type: {:?}",
        column_metadata.column_path(),
        column_descr.physical_type(),
        column_descr.logical_type(),
    );

    if let Some(statistics) = column_metadata.statistics() {
        if statistics.has_min_max_set() {
            let (min, max) = stats_as_tuple(statistics, column_descr.clone());

            println!("min: {}, max: {}", min, max);
        }

        println!("null count: {}", statistics.null_count());
    }
}
fn stats_as_tuple(statistics: &Statistics, column_descr: Arc<ColumnDescriptor>) -> (Value, Value) {
    match statistics {
        Statistics::Int32(typed_stats) => {
            let min = Value::Number(Number::from(*typed_stats.min()));
            let max = Value::Number(Number::from(*typed_stats.max()));

            (min, max)
        }
        Statistics::Int64(typed_stats) => {
            let min = Value::Number(Number::from(*typed_stats.min()));
            let max = Value::Number(Number::from(*typed_stats.max()));
            (min, max)
        }
        Statistics::Int96(typed_stats) => {
            let min = Value::Number(Number::from(typed_stats.min().to_i64()));
            let max = Value::Number(Number::from(typed_stats.max().to_i64()));
            (min, max)
        }
        Statistics::Float(typed_stats) => {
            let min = Number::from_f64(*typed_stats.min() as f64)
                .map(|n| Value::Number(n))
                .unwrap();
            let max = Number::from_f64(*typed_stats.max() as f64)
                .map(|n| Value::Number(n))
                .unwrap();
            (min, max)
        }
        Statistics::Double(typed_stats) => {
            let min = Number::from_f64(*typed_stats.min() as f64)
                .map(|n| Value::Number(n))
                .unwrap();
            let max = Number::from_f64(*typed_stats.max() as f64)
                .map(|n| Value::Number(n))
                .unwrap();
            (min, max)
        }
        Statistics::Boolean(typed_stats) => {
            let min = Value::Bool(*typed_stats.min());
            let max = Value::Bool(*typed_stats.max());
            (min, max)
        }
        Statistics::ByteArray(typed_stats) if is_utf8(column_descr.logical_type()) => {
            let min = std::str::from_utf8(typed_stats.min_bytes())
                .map(|s| Value::String(s.to_string()))
                .unwrap();
            let max = std::str::from_utf8(typed_stats.max_bytes())
                .map(|s| Value::String(s.to_string()))
                .unwrap();
            (min, max)
        }
        Statistics::ByteArray(typed_stats) => (Value::Null, Value::Null),
        Statistics::FixedLenByteArray(typed_stats) => (Value::Null, Value::Null),
        _ => (Value::Null, Value::Null),
    }
}

fn is_utf8(opt: Option<LogicalType>) -> bool {
    match opt.as_ref() {
        Some(LogicalType::STRING(_)) => true,
        _ => false,
    }
}

fn schema_descriptor_from_file_metadata(
    file_metadata: &FileMetaData,
) -> Result<Arc<SchemaDescriptor>, ParquetError> {
    let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());

    type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))
}

fn create_test_schema() -> Result<Schema, serde_json::error::Error> {
    let schema_string = serde_json::to_string(&some_schema()).unwrap();

    serde_json::from_str(&schema_string)
}

fn some_schema() -> Value {
    json!({
      "type": "struct",
      "fields": [
        {
          "name": "some_object",
          "type": {
            "type": "struct",
            "fields": [
              {
                "name": "kafka",
                "type": {
                  "type": "struct",
                  "fields": [
                    {
                      "name": "offset",
                      "type": "long",
                      "nullable": true,
                      "metadata": {}
                    },
                    {
                      "name": "topic",
                      "type": "string",
                      "nullable": true,
                      "metadata": {}
                    },
                    {
                      "name": "partition",
                      "type": "integer",
                      "nullable": true,
                      "metadata": {}
                    }
                  ]
                },
                "nullable": true,
                "metadata": {}
              }
            ]
          },
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_int_array",
          "type": {
            "type": "array",
            "elementType": "integer",
            "containsNull": false
          },
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_struct_array",
          "type": {
            "type": "array",
            "elementType": {
              "type": "struct",
              "fields": [
                {
                  "name": "id",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "value",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                }
              ]
            },
            "containsNull": false
          },
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_string",
          "type": "string",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_int",
          "type": "integer",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_long",
          "type": "long",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_float",
          "type": "float",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_double",
          "type": "double",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "some_bool",
          "type": "boolean",
          "nullable": true,
          "metadata": {}
        },
      ]
    })
}

fn some_data() -> Vec<Value> {
    vec![
        json!({
            "meta": {
                "kafka": { "offset": 1, "partition": 0i64, "topic": "A" }
            },
            "some_int_array": [0, 1, 4],
            "some_struct_array": [{
                "id": "xyz",
                "value": "abc",
            }],
            "some_string": "hello world",
            "some_int": 11,
            "some_long": 99i64,
            "some_float": 3.141f32,
            "some_double": 3.141f64,
            "some_bool": true,
        }),
        json!({
            "meta": {
                "kafka": { "topic": "A" }
            },
            "some_struct_array": [{
                "id": "pqr",
                "value": "tuv",
            }],
            "some_string": "hello world",
            "some_int": 42,
            "some_long": 101i64,
            "some_float": 3.141f32,
            "some_double": 3.141f64,
            "some_bool": false,
        }),
        json!({
            "meta": {
                "kafka": { "offset": 2, "partition": 3i64, "topic": "A" }
            },
            "some_int_array": [0, 1],
            "some_struct_array": [{
                "id": "pqr",
                "value": "tuv",
            }],
            "some_string": "hello world",
            "some_int": 42,
            "some_long": 99i64,
            "some_float": 3.141f32,
            "some_double": 3.141f64,
            "some_bool": true,
        }),
        json!({
            "meta": {
                "kafka": { "offset": 2, "partition": 0i64, "topic": "A" }
            },
            "some_int_array": [0, 1, 2, 3],
            "some_struct_array": [{
                "id": "xyz",
                "value": "abc",
            }],
            "some_string": "hello world",
            "some_int": 11,
            "some_long": 99i64,
            "some_float": 3.141f32,
            "some_double": 3.141f64,
            "some_bool": true,
        }),
    ]
}
