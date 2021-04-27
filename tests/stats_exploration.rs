use arrow::{
    array::{
        as_boolean_array, as_primitive_array, make_array, ArrayData, ArrayRef, BooleanArray,
        BooleanBufferBuilder, StringBuilder,
    },
    buffer::MutableBuffer,
    datatypes::{DataType, Field, Schema as ArrowSchema},
};
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
    schema::types::{ColumnDescriptor, ColumnPath, SchemaDescriptor},
};
use parquet_format::FileMetaData;
use serde_json::{json, Number, Value};
use std::collections::HashMap;
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

    // handle_file_metadata(&file_metadata);
    handle_file_metadata2(&file_metadata);
}

fn handle_file_metadata2(file_metadata: &FileMetaData) {
    let schema_descriptor = schema_descriptor_from_file_metadata(file_metadata).unwrap();
    let row_group_metadata: Vec<RowGroupMetaData> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()).unwrap())
        .collect();
    for i in 0..schema_descriptor.num_columns() {
        let column_descr = schema_descriptor.column(i);
        let statistics: Vec<&Statistics> = row_group_metadata
            .iter()
            .filter_map(|g| g.column(i).statistics())
            .collect();

        // let (min, max) = stats_min_and_max(&statistics, column_descr.clone());
        let (min, max) = stats_min_and_max2(&statistics, column_descr.clone());

        let null_count: u64 = statistics.iter().map(|s| s.null_count()).sum();

        println!("Stats for column {:?}", column_descr.path());
        println!("Min: {:?}, Max: {:?}", min, max);
        println!("Null count {}", null_count);
    }
}

fn stats_min_and_max2(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
) -> (Option<Value>, Option<Value>) {
    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .map(|s| *s)
        .collect();

    if stats_with_min_max.len() == 0 {
        return (None, None);
    }

    match stats_with_min_max.first() {
        Some(Statistics::ByteArray(_)) if is_utf8(column_descr.logical_type()) => {
            min_and_max_for_string(&stats_with_min_max)
        }
        Some(Statistics::ByteArray(_)) => {
            panic!();
        }
        Some(Statistics::FixedLenByteArray(_)) => {
            panic!();
        }
        Some(stats) => min_and_max_for_primitive(*stats, &stats_with_min_max),
        _ => {
            panic!();
        }
    }
}

fn min_and_max_for_string(stats_with_min_max: &Vec<&Statistics>) -> (Option<Value>, Option<Value>) {
    let min_strings = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.min_bytes()).ok());

    let min_string = min_strings.min();
    let min_value = min_string.map(|s| Value::String(s.to_string()));

    let max_strings = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.max_bytes()).ok());

    let max_string = max_strings.max();
    let max_value = max_string.map(|s| Value::String(s.to_string()));

    return (min_value, max_value);
}

fn min_and_max_for_primitive(
    type_sample: &Statistics,
    stats_with_min_max: &Vec<&Statistics>,
) -> (Option<Value>, Option<Value>) {
    match type_sample {
        Statistics::Int32(_) => {
            let typed = stats_with_min_max.iter().map(|s| match s {
                Statistics::Int32(typed) => (Some(typed.min()), Some(typed.max())),
                _ => (None, None),
            });
            let min = typed
                .clone()
                .filter_map(|(min, _)| min)
                .min()
                .map(|min| Value::Number(Number::from(*min)));

            let max = typed
                .filter_map(|(_, max)| max)
                .max()
                .map(|max| Value::Number(Number::from(*max)));

            (min, max)
        }
        Statistics::Int64(_) => {
            let typed = stats_with_min_max.iter().map(|s| match s {
                Statistics::Int64(typed) => (Some(typed.min()), Some(typed.max())),
                _ => (None, None),
            });
            let min = typed
                .clone()
                .filter_map(|(min, _)| min)
                .min()
                .map(|min| Value::Number(Number::from(*min)));

            let max = typed
                .filter_map(|(_, max)| max)
                .max()
                .map(|max| Value::Number(Number::from(*max)));

            (min, max)
        }
        Statistics::Int96(_) => {
            panic!()
        }
        Statistics::Float(_) => {
            let typed = stats_with_min_max.iter().map(|s| match s {
                Statistics::Float(typed) => (Some(typed.min()), Some(typed.max())),
                _ => (None, None),
            });
            let min = typed
                .clone()
                .filter_map(|(min, _)| min)
                .fold(f32::INFINITY, |a, &b| a.min(b));
            let min = Number::from_f64(min as f64).map(|n| Value::Number(n));

            let max = typed
                .filter_map(|(_, max)| max)
                .fold(f32::NEG_INFINITY, |a, &b| a.max(b));
            let max = Number::from_f64(max as f64).map(|n| Value::Number(n));

            (min, max)
        }
        Statistics::Double(_) => {
            let typed = stats_with_min_max.iter().map(|s| match s {
                Statistics::Double(typed) => (Some(typed.min()), Some(typed.max())),
                _ => (None, None),
            });
            let min = typed
                .clone()
                .filter_map(|(min, _)| min)
                .fold(f64::INFINITY, |a, &b| a.min(b));
            let min = Number::from_f64(min).map(|n| Value::Number(n));

            let max = typed
                .clone()
                .filter_map(|(_, max)| max)
                .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let max = Number::from_f64(max).map(|n| Value::Number(n));

            (min, max)
        }
        Statistics::Boolean(_) => {
            let typed = stats_with_min_max.iter().map(|s| match s {
                Statistics::Boolean(typed) => (Some(typed.min()), Some(typed.max())),
                _ => (None, None),
            });
            let min = typed
                .clone()
                .filter_map(|(min, _)| min)
                .min()
                .map(|min| Value::Bool(*min));

            let max = typed
                .filter_map(|(_, max)| max)
                .max()
                .map(|max| Value::Bool(*max));

            (min, max)
        }
        _ => {
            panic!();
        }
    }
}

fn stats_min_and_max(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
) -> (Option<Value>, Option<Value>) {
    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .map(|s| *s)
        .collect();

    if stats_with_min_max.len() == 0 {
        return (None, None);
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
        Some(Statistics::ByteArray(_)) => {
            panic!();
        }
        Some(Statistics::FixedLenByteArray(_)) => {
            panic!();
        }
        Some(Statistics::Int96(_)) => {
            panic!();
        }
        None => {
            panic!();
        }
    };

    if data_type == DataType::Utf8 {
        return min_and_max_for_string(&stats_with_min_max);
    }

    let mut min_buffer = MutableBuffer::new(stats_with_min_max.len() * data_size);
    let mut max_buffer = MutableBuffer::new(stats_with_min_max.len() * data_size);

    for (min_bytes, max_bytes) in stats_with_min_max
        .iter()
        .map(|s| (s.min_bytes(), s.max_bytes()))
    {
        min_buffer.extend_from_slice(min_bytes);
        max_buffer.extend_from_slice(max_bytes);
    }

    let mut min_builder = ArrayData::builder(data_type.clone())
        .len(stats_with_min_max.len())
        .add_buffer(min_buffer.into());
    let min_data = min_builder.build();
    let min_array = make_array(min_data);

    let mut max_builder = ArrayData::builder(data_type.clone())
        .len(stats_with_min_max.len())
        .add_buffer(max_buffer.into());
    let max_data = max_builder.build();
    let max_array = make_array(max_data);

    if data_type == DataType::Boolean {
        let min_array = as_boolean_array(&min_array);
        let min = arrow::compute::min_boolean(min_array);
        let min = min.map(|b| Value::Bool(b));

        let max_array = as_boolean_array(&max_array);
        let max = arrow::compute::max_boolean(max_array);
        let max = max.map(|b| Value::Bool(b));

        return (min, max);
    }

    match data_type {
        DataType::Int32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            (min, max)
        }
        DataType::Int64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            (min, max)
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

            (min, max)
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

            (min, max)
        }
        _ => {
            panic!();
        }
    }

    // let stats_min_bytes = stats_with_min_max.iter().map(|s| s.min_bytes());

    // let stats_max_bytes = stats_with_min_max.iter().map(|s| s.min_bytes());

    // todo!()
    // arrow::compute::min(
    // arrow::compute::min_string(
    // arrow::compute::min_boolean(
}

fn handle_file_metadata(file_metadata: &FileMetaData) {
    let schema_descriptor = schema_descriptor_from_file_metadata(file_metadata).unwrap();
    let leaves = schema_descriptor.columns();

    let mut null_counts: HashMap<ColumnPath, u64> = HashMap::new();
    let mut max_values: HashMap<ColumnPath, Value> = HashMap::new();
    let mut min_values: HashMap<ColumnPath, Value> = HashMap::new();

    for row_group in file_metadata.row_groups.iter() {
        let rg =
            RowGroupMetaData::from_thrift(schema_descriptor.clone(), row_group.clone()).unwrap();

        for i in 0..leaves.len() {
            let column_metadata = rg.column(i);
            let column_descr_ptr = leaves[i].clone();

            println!(
                "Column path: {}, Physical type: {}, Logical type: {:?}, Max rep level: {}, max def level: {}",
                column_metadata.column_path(),
                column_descr_ptr.physical_type(),
                column_descr_ptr.logical_type(),
                column_descr_ptr.max_rep_level(),
                column_descr_ptr.max_def_level(),
            );

            if let Some(statistics) = column_metadata.statistics() {
                let column_path = column_metadata.column_path();

                if statistics.has_min_max_set() {
                    let (min, max) = stats_as_tuple(statistics, column_descr_ptr.clone());

                    println!("min: {}, max: {}", min, max);

                    if let Some(v) = max_values.get_mut(column_path) {
                        match lhs_greater(&max, v) {
                            Ok(maybe) if maybe => {
                                *v = max;
                            }
                            Err(s) => {
                                println!("{}", s);
                            }
                            _ => { /* noop */ }
                        }
                    } else {
                        max_values.insert(column_path.clone(), max);
                    }

                    if let Some(v) = min_values.get_mut(column_path) {
                        match lhs_greater(v, &min) {
                            Ok(maybe) if maybe => {
                                *v = min;
                            }
                            Err(s) => {
                                println!("{}", s);
                            }
                            _ => { /* noop */ }
                        }
                    } else {
                        min_values.insert(column_path.clone(), min);
                    }

                    if let Some(v) = null_counts.get_mut(column_path) {
                        *v += statistics.null_count();
                    } else {
                        null_counts.insert(column_path.clone(), statistics.null_count());
                    }
                }

                println!("null count: {}", statistics.null_count());
            }
        }
    }

    println!("Max values:");
    for (k, v) in max_values.iter() {
        println!("{} -> {:?}", k, v);
    }

    println!("Min values:");
    for (k, v) in min_values.iter() {
        println!("{} -> {:?}", k, v);
    }

    println!("Null counts:");
    for (k, v) in null_counts.iter() {
        println!("{} -> {:?}", k, v);
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

fn lhs_greater(lhs: &Value, rhs: &Value) -> Result<bool, String> {
    match (lhs, rhs) {
        (Value::Null, Value::Null) => Ok(false),
        (Value::String(lhs), Value::String(rhs)) => Ok(lhs > rhs),
        (Value::Number(lhs), Value::Number(rhs)) if lhs.is_u64() && rhs.is_u64() => {
            Ok(lhs.as_u64() > rhs.as_u64())
        }
        (Value::Number(lhs), Value::Number(rhs)) if lhs.is_i64() && rhs.is_i64() => {
            Ok(lhs.as_i64() > rhs.as_i64())
        }
        (Value::Number(lhs), Value::Number(rhs)) if lhs.is_f64() && rhs.is_f64() => {
            Ok(lhs.as_f64() > rhs.as_f64())
        }
        (Value::Bool(lhs), Value::Bool(rhs)) => Ok(lhs > rhs),
        _ => Err("Could not compare values".to_string()),
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
