use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, json::reader::Decoder};
use deltalake::Schema as DeltaSchema;
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::RowGroupMetaData, properties::WriterProperties, writer::InMemoryWriteableCursor,
    },
    schema::types::SchemaDescriptor,
};
use serde_json::{json, Value};
use std::sync::Arc;

#[test]
fn inspect_parquet_footer() {
    // Deserialize an arrow schema from a JSON representation.
    let arrow_schema = example_arrow_schema_from_json();

    let cursor = InMemoryWriteableCursor::default();

    // stats enabled is defaulted to true, but just to be explicit...
    let writer_properties = WriterProperties::builder()
        .set_statistics_enabled(true)
        .build();

    let mut arrow_writer = ArrowWriter::try_new(
        cursor.clone(),
        arrow_schema.clone(),
        Some(writer_properties),
    )
    .unwrap();

    let json_data = example_data();
    let mut json_iter = InMemValueIter::from_vec(json_data.as_slice());
    let decoder = Decoder::new(arrow_schema.clone(), json_data.len(), None);
    let batch = decoder.next_batch(&mut json_iter).unwrap().unwrap();

    arrow_writer.write(&batch).unwrap();

    let file_metadata = arrow_writer.close().unwrap();

    //
    // write out file for external inspection with other tools
    //

    std::fs::write("../parquet_footer_test.parquet", cursor.data()).unwrap();

    //
    // Check min/max stats in memory...
    //
    // when checking FileMetaData in memory, I can see min/max stats for everything but the list type fields.
    // however, when reading the file back in with spark-shell, parquet-tools or pyarrow, there is bigger trouble
    // 1. has min max is false for _all_ columns according to pyarrow (weird...? i saw they were
    //    set in memory for non-list)
    // 2. the parquet file appears to be corrupted. on a `show` - both spark-shell and parquet-tools throw exception for index of max level and is unable to present any data.

    println!("## Thrift meta");

    for rg in file_metadata.row_groups.iter() {
        for c in rg.columns.iter() {
            let metadata = c.meta_data.as_ref().unwrap();
            let stats = metadata.statistics.as_ref().unwrap();

            println!("Path: {:?}", metadata.path_in_schema);

            if let None = stats.min_value.as_ref() {
                println!("NO min/max");
            } else {
                println!("Has min/max");
            }
        }
    }

    let type_ = parquet::schema::types::from_thrift(file_metadata.schema.as_slice()).unwrap();
    let schema_descriptor = Arc::new(SchemaDescriptor::new(type_));

    println!();

    println!("## Parquet meta");

    let rg_metas = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()).unwrap());

    for rg_meta in rg_metas {
        for c in rg_meta.columns() {
            println!("Path: {:?}", c.column_path());
            let stats = c.statistics().unwrap();

            if stats.has_min_max_set() {
                println!("Has min/max");
            } else {
                println!("NO min/max");
            }
        }
    }
}

fn example_arrow_schema_from_json() -> Arc<ArrowSchema> {
    let schema_json = json!(
    {
      "fields": [
        {
          "name": "some_nested_object",
          "nullable": true,
          "type": {
            "name": "struct"
          },
          "children": [
            {
              "name": "kafka",
              "nullable": true,
              "type": {
                "name": "struct"
              },
              "children": [
                {
                  "name": "offset",
                  "nullable": true,
                  "type": {
                    "name": "int",
                    "bitWidth": 64,
                    "isSigned": true
                  },
                  "children": []
                },
                {
                  "name": "topic",
                  "nullable": true,
                  "type": {
                    "name": "utf8"
                  },
                  "children": []
                },
                {
                  "name": "partition",
                  "nullable": true,
                  "type": {
                    "name": "int",
                    "bitWidth": 32,
                    "isSigned": true
                  },
                  "children": []
                }
              ]
            }
          ]
        },
        {
          "name": "some_int_array",
          "nullable": true,
          "type": {
            "name": "list"
          },
          "children": [
            {
              "name": "element",
              "nullable": false,
              "type": {
                "name": "int",
                "bitWidth": 32,
                "isSigned": true
              },
              "children": []
            }
          ]
        },
        {
          "name": "some_struct_array",
          "nullable": true,
          "type": {
            "name": "list"
          },
          "children": [
            {
              "name": "element",
              "nullable": false,
              "type": {
                "name": "struct"
              },
              "children": [
                {
                  "name": "id",
                  "nullable": true,
                  "type": {
                    "name": "utf8"
                  },
                  "children": []
                },
                {
                  "name": "value",
                  "nullable": true,
                  "type": {
                    "name": "utf8"
                  },
                  "children": []
                }
              ]
            }
          ]
        },
        {
          "name": "some_string",
          "nullable": true,
          "type": {
            "name": "utf8"
          },
          "children": []
        },
        {
          "name": "some_int",
          "nullable": true,
          "type": {
            "name": "int",
            "bitWidth": 32,
            "isSigned": true
          },
          "children": []
        }
      ],
      "metadata": {}
    }
    );

    Arc::new(ArrowSchema::from(&schema_json).unwrap())
}

fn example_data() -> Vec<Value> {
    vec![
        json!({
            "some_nested_object": {
                "kafka": { "offset": 1, "partition": 0i64, "topic": "A" }
            },
            "some_int_array": [0, 1, 4],
            "some_struct_array": [{
                "id": "xyz",
                "value": "abc",
            }],
            "some_string": "hello world",
            "some_int": 11,
        }),
        json!({
            "some_nested_object": {
                "kafka": { "topic": "A" }
            },
            "some_struct_array": [{
                "id": "pqr",
                "value": "tuv",
            }],
            "some_string": "hello world",
            "some_int": 42,
        }),
        json!({
            "some_nested_object": {
                "kafka": { "offset": 2, "partition": 3i64, "topic": "A" }
            },
            "some_int_array": [0, 1],
            "some_struct_array": [{
                "id": "pqr",
                "value": "tuv",
            }],
            "some_string": "hello world",
        }),
        json!({
            "some_nested_object": {
                "kafka": { "offset": 2, "partition": 0i64, "topic": "A" }
            },
            "some_int_array": [0, 1, 2, 3],
            "some_struct_array": [{
                "id": "xyz",
                "value": "abc",
            }],
            "some_string": "hello world",
            "some_int": 11,
        }),
    ]
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
