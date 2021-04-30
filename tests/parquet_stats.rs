use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, json::reader::Decoder};
use deltalake::Schema as DeltaSchema;
use parquet::{arrow::ArrowWriter, file::writer::InMemoryWriteableCursor};
use serde_json::{json, Value};
use std::convert::TryFrom;
use std::sync::Arc;

#[test]
fn inspect_parquet_footer() {
    // let arrow_schema = example_arrow_schema_from_delta();
    let arrow_schema = example_arrow_schema_from_json();

    let cursor = InMemoryWriteableCursor::default();

    let mut arrow_writer =
        ArrowWriter::try_new(cursor.clone(), arrow_schema.clone(), None).unwrap();

    let json_data = example_data();
    let mut json_iter = InMemValueIter::from_vec(json_data.as_slice());
    let decoder = Decoder::new(arrow_schema.clone(), json_data.len(), None);
    let batch = decoder.next_batch(&mut json_iter).unwrap().unwrap();

    arrow_writer.write(&batch).unwrap();

    let file_metadata = arrow_writer.close().unwrap();

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
}

fn example_arrow_schema_from_delta() -> Arc<ArrowSchema> {
    let delta_schema = example_delta_schema();
    let arrow_schema = <ArrowSchema as TryFrom<&DeltaSchema>>::try_from(&delta_schema).unwrap();

    Arc::new(arrow_schema)
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

fn example_delta_schema() -> DeltaSchema {
    let schema_json = json!({
      "type": "struct",
      "fields": [
        {
          "name": "some_nested_object",
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
      ]
    });

    let schema_string = serde_json::to_string(&schema_json).unwrap();
    serde_json::from_str(&&schema_string).unwrap()
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
