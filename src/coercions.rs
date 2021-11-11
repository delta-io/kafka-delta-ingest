use deltalake::{
    Schema as DeltaSchema, SchemaDataType as DeltaDataType, SchemaField as DeltaField,
};

use chrono::prelude::*;
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug)]
enum CoercionNode {
    Coercion(Coercion),
    Tree(CoercionTree),
}

#[derive(Debug)]
enum Coercion {
    ToString,
    ToTimestamp,
}

#[derive(Debug)]
pub(crate) struct CoercionTree {
    root: HashMap<String, CoercionNode>,
}

/// Returns a [`CoercionTree`] so the schema can be walked efficiently level by level when performing conversions.
pub(crate) fn create_coercion_tree(schema: DeltaSchema) -> CoercionTree {
    let mut root = HashMap::new();

    for field in schema.get_fields() {
        append_coercion(&mut root, field)
    }

    CoercionTree { root }
}

fn append_coercion(context: &mut HashMap<String, CoercionNode>, field: &DeltaField) {
    match field.get_type() {
        DeltaDataType::primitive(primitive_type) if primitive_type == "string" => {
            context.insert(
                field.get_name().to_string(),
                CoercionNode::Coercion(Coercion::ToString),
            );
        }
        DeltaDataType::primitive(primitive_type) if primitive_type == "timestamp" => {
            context.insert(
                field.get_name().to_string(),
                CoercionNode::Coercion(Coercion::ToTimestamp),
            );
        }
        DeltaDataType::r#struct(schema) => {
            let mut nested_context = HashMap::new();
            for nested_field in schema.get_fields() {
                append_coercion(&mut nested_context, nested_field);
            }
            if !nested_context.is_empty() {
                let tree = CoercionTree {
                    root: nested_context,
                };
                context.insert(field.get_name().to_string(), CoercionNode::Tree(tree));
            }
        }
        _ => {
            // noop for now
            // add more data type coercions as necessary
        }
    }
}

/// Applies all data coercions specified by the [`CoercionTree`] to the [`Value`].
/// Though it does not currently, this function should approximate or improve on the coercions applied by [Spark's `from_json`](https://spark.apache.org/docs/latest/api/sql/index.html#from_json)
pub(crate) fn coerce(value: &mut Value, coercion_tree: &CoercionTree) {
    if let Some(context) = value.as_object_mut() {
        for (field_name, coercion) in coercion_tree.root.iter() {
            apply_coercion(context, field_name, coercion);
        }
    }
}

fn apply_coercion(context: &mut Map<String, Value>, field_name: &str, node: &CoercionNode) {
    let opt = context.get_mut(field_name);

    if let Some(value) = opt {
        match node {
            CoercionNode::Coercion(Coercion::ToString) => {
                let replacement = if value.is_string() {
                    None
                } else {
                    Some(value.to_string())
                };

                if let Some(coerced) = replacement {
                    context.insert(field_name.to_string(), Value::String(coerced));
                }
            }
            CoercionNode::Coercion(Coercion::ToTimestamp) => {
                let replacement: Option<i64> = if let Some(s) = value.as_str() {
                    // The delta timestamp data type must be set as microseconds since epoch.
                    // If we have a string, try to convert it.
                    // If conversion fails, leave it alone. It'll come out null in delta.
                    //
                    // TODO: `from_str` doesn't work with all date formats.
                    // It may be worthwhile to do some format sniffing and use more specific parse functions.
                    //
                    DateTime::from_str(s).map_or_else(
                        |e| {
                            log::error!(
                                "Error coercing timestamp from string. String: {}. Error: {}",
                                s,
                                e
                            );
                            None
                        },
                        |dt: DateTime<Utc>| Some(dt.timestamp_nanos() / 1000),
                    )
                } else {
                    None
                };

                if let Some(coerced) = replacement {
                    context.insert(field_name.to_string(), Value::Number(Number::from(coerced)));
                }
            }
            CoercionNode::Tree(tree) => {
                for (k, v) in tree.root.iter() {
                    let new_context = value.as_object_mut();
                    if let Some(new_context) = new_context {
                        apply_coercion(new_context, k, v);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde_json::json;

    lazy_static! {
        static ref SCHEMA: Value = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "level2",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "level2_string",
                                "type": "string",
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "level2_int",
                                "type": "integer",
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "level2_timestamp",
                                "type": "timestamp",
                                "nullable": true, "metadata": {}
                            }]
                    },
                    "nullable": true, "metadata": {}
                },
                { "name": "level1_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "level1_integer", "type": "integer", "nullable": true, "metadata": {} },
                { "name": "level1_timestamp", "type": "timestamp", "nullable": true, "metadata": {} },
            ]
        });
    }

    #[test]
    fn test_value_coercion_tree() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();

        let coercion_tree = create_coercion_tree(delta_schema);

        let mut messages = vec![
            json!({
                "level1_string": "a",
                "level1_integer": 0,
                "level1_timestamp": 1636668665000000i64,
                "level2": {
                    "level2_string": { "x": "x", "y": "y" },
                    "level2_timestamp": "2021-11-11T22:11:58Z"
                }
            }),
            json!({
                "level1_string": { "a": "a", "b": "b"},
                "level1_integer": 42,
                "level1_timestamp": "2021-11-11T22:11:58Z"
            }),
            json!({
                "level1_integer": 99,
            }),
            json!({
                "level1_timestamp": "2021-11-11 22:11:58",
            }),
            json!({
                "level1_timestamp": "This definitely is not a timestamp",
            }),
            json!({
                "level1_timestamp": "1636668665000000",
            }),
        ];

        for message in messages.iter_mut() {
            coerce(message, &coercion_tree);
        }

        println!("{:#?}", coercion_tree);
        println!("{:#?}", messages);
    }
}
