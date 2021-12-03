use deltalake::{Schema as DeltaSchema, SchemaDataType as DeltaDataType};

use chrono::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
#[allow(unused)]
enum CoercionNode {
    Coercion(Coercion),
    Tree(CoercionTree),
    ArrayTree(CoercionTree),
    ArrayPrimitive(Coercion),
}

#[derive(Debug, Clone, PartialEq)]
enum Coercion {
    ToString,
    ToTimestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoercionTree {
    root: HashMap<String, CoercionNode>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoercionArray {
    element: CoercionNode,
}

/// Returns a [`CoercionTree`] so the schema can be walked efficiently level by level when performing conversions.
pub(crate) fn create_coercion_tree(schema: &DeltaSchema) -> CoercionTree {
    let mut root = HashMap::new();

    for field in schema.get_fields() {
        if let Some(node) = build_coercion_node(field.get_type()) {
            root.insert(field.get_name().to_string(), node);
        }
    }

    CoercionTree { root }
}

fn build_coercion_node(r#type: &DeltaDataType) -> Option<CoercionNode> {
    match r#type {
        DeltaDataType::primitive(r#type) if r#type == "string" => {
            Some(CoercionNode::Coercion(Coercion::ToString))
        }
        DeltaDataType::primitive(r#type) if r#type == "timestamp" => {
            Some(CoercionNode::Coercion(Coercion::ToTimestamp))
        }
        DeltaDataType::r#struct(schema) => {
            let nested_context = create_coercion_tree(schema);
            if !nested_context.root.is_empty() {
                Some(CoercionNode::Tree(nested_context))
            } else {
                None
            }
        }
        DeltaDataType::array(schema) => {
            build_coercion_node(schema.get_element_type()).and_then(|node| match node {
                CoercionNode::Coercion(c) => Some(CoercionNode::ArrayPrimitive(c)),
                CoercionNode::Tree(t) => Some(CoercionNode::ArrayTree(t)),
                _ => None,
            })
        }
        _ => None,
    }
}

/// Applies all data coercions specified by the [`CoercionTree`] to the [`Value`].
/// Though it does not currently, this function should approximate or improve on the coercions applied by [Spark's `from_json`](https://spark.apache.org/docs/latest/api/sql/index.html#from_json)
pub(crate) fn coerce(value: &mut Value, coercion_tree: &CoercionTree) {
    if let Some(context) = value.as_object_mut() {
        for (field_name, coercion) in coercion_tree.root.iter() {
            if let Some(value) = context.get_mut(field_name) {
                apply_coercion(value, coercion);
            }
        }
    }
}

fn apply_coercion(value: &mut Value, node: &CoercionNode) {
    match node {
        CoercionNode::Coercion(Coercion::ToString) => {
            if !value.is_string() {
                *value = Value::String(value.to_string());
            }
        }
        CoercionNode::Coercion(Coercion::ToTimestamp) => {
            if let Some(as_str) = value.as_str() {
                if let Some(parsed) = string_to_timestamp(as_str) {
                    *value = parsed
                }
            }
        }
        CoercionNode::Tree(tree) => {
            for (name, node) in tree.root.iter() {
                let fields = value.as_object_mut();
                if let Some(fields) = fields {
                    if let Some(value) = fields.get_mut(name) {
                        apply_coercion(value, node);
                    }
                }
            }
        }
        CoercionNode::ArrayPrimitive(coercion) => {
            let values = value.as_array_mut();
            if let Some(values) = values {
                let node = CoercionNode::Coercion(coercion.clone());
                for value in values {
                    apply_coercion(value, &node);
                }
            }
        }
        CoercionNode::ArrayTree(tree) => {
            let values = value.as_array_mut();
            if let Some(values) = values {
                let node = CoercionNode::Tree(tree.clone());
                for value in values {
                    apply_coercion(value, &node);
                }
            }
        }
    }
}

fn string_to_timestamp(string: &str) -> Option<Value> {
    let parsed = DateTime::from_str(string);
    if let Err(e) = parsed {
        log::error!(
            "Error coercing timestamp from string. String: {}. Error: {}",
            string,
            e
        )
    }
    parsed
        .ok()
        .map(|dt: DateTime<Utc>| Value::Number((dt.timestamp_nanos() / 1000).into()))
}

#[cfg(test)]
mod tests {

    use super::*;
    // use maplit::hashmap;
    use serde_json::json;

    lazy_static! {
        static ref SCHEMA: Value = json!({
            "type": "struct",
            "fields": [
                { "name": "level1_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "level1_integer", "type": "integer", "nullable": true, "metadata": {} },
                { "name": "level1_timestamp", "type": "timestamp", "nullable": true, "metadata": {} },
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
                {
                    "name": "array_timestamp",
                    "type": {
                        "type": "array",
                        "containsNull": true,
                        "elementType": "timestamp",
                    },
                    "nullable": true, "metadata": {},
                },
                {
                    "name": "array_string",
                    "type": {
                        "type": "array",
                        "containsNull": true,
                        "elementType": "string",
                    },
                    "nullable": true, "metadata": {},
                },
                {
                    "name": "array_int",
                    "type": {
                        "type": "array",
                        "containsNull": true,
                        "elementType": "integer",
                    },
                    "nullable": true, "metadata": {},
                },
                {
                    "name": "array_struct",
                    "type": {
                        "type": "array",
                        "containsNull": true,
                        "elementType": {
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
                                },
                            ],
                        },
                    },
                    "nullable": true, "metadata": {},
                }
            ]
        });
    }

    #[test]
    fn test_coercion_tree() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();

        let tree = create_coercion_tree(&delta_schema);

        let mut top_level_keys: Vec<&String> = tree.root.keys().collect();
        top_level_keys.sort();

        let level2 = tree.root.get("level2");
        let level2_root = match level2 {
            Some(CoercionNode::Tree(tree)) => tree.root.clone(),
            _ => unreachable!(""),
        };
        let mut level2_keys: Vec<&String> = level2_root.keys().collect();
        level2_keys.sort();

        let array_struct = tree.root.get("array_struct");
        let array_struct_root = match array_struct {
            Some(CoercionNode::ArrayTree(tree)) => tree.root.clone(),
            _ => unreachable!(""),
        };

        assert_eq!(
            vec![
                "array_string",
                "array_struct",
                "array_timestamp",
                "level1_string",
                "level1_timestamp",
                "level2"
            ],
            top_level_keys
        );

        assert_eq!(vec!["level2_string", "level2_timestamp"], level2_keys);

        assert_eq!(
            CoercionNode::Coercion(Coercion::ToString),
            tree.root.get("level1_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToTimestamp),
            tree.root.get("level1_timestamp").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToString),
            level2_root.get("level2_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToTimestamp),
            level2_root.get("level2_timestamp").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::ArrayPrimitive(Coercion::ToString),
            tree.root.get("array_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::ArrayPrimitive(Coercion::ToTimestamp),
            tree.root.get("array_timestamp").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToString),
            array_struct_root.get("level2_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToTimestamp),
            array_struct_root
                .get("level2_timestamp")
                .unwrap()
                .to_owned()
        );
    }

    #[test]
    fn test_coercions() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();

        let coercion_tree = create_coercion_tree(&delta_schema);

        let mut messages = vec![
            json!({
                "level1_string": "a",
                "level1_integer": 0,
                // Timestamp passed in as an i64. We won't coerce it, but it will work anyway.
                "level1_timestamp": 1636668718000000i64,
                "level2": {
                    "level2_string": { "x": "x", "y": "y" },
                    "level2_timestamp": "2021-11-11T22:11:58Z"
                },
                "array_timestamp": ["2021-11-17T01:02:03Z", "2021-11-17T02:03:04Z"],
                "array_string": ["a", "b", {"a": 1}],
                "array_int": [1, 2, 3],
                "array_struct": [
                    {
                        "level2_string": r#"{"a":1}"#,
                        "level2_int": 1,
                        "level2_timestamp": "2021-11-17T00:00:01Z"
                    },
                    {
                        "level2_string": { "a": 2 },
                        "level2_int": 2,
                        "level2_timestamp": 1637107202000000i64
                    },
                ]
            }),
            json!({
                "level1_string": { "a": "a", "b": "b"},
                "level1_integer": 42,
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": "2021-11-11T22:11:58Z"
            }),
            json!({
                "level1_integer": 99,
            }),
            json!({
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": "2021-11-11T22:11:58+00:00",
            }),
            json!({
                // RFC 3339 but not ISO 8601. We WILL coerce it.
                "level1_timestamp": "2021-11-11T22:11:58-00:00",
            }),
            json!({
                // ISO 8601 but not RFC 3339. We WON'T coerce it.
                "level1_timestamp": "20211111T22115800Z",
            }),
            json!({
                // This is a Java date style timestamp. We WON'T coerce it.
                "level1_timestamp": "2021-11-11 22:11:58",
            }),
            json!({
                "level1_timestamp": "This definitely is not a timestamp",
            }),
            json!({
                // This is valid epoch micros, but typed as a string on the way in. We WON'T coerce it.
                "level1_timestamp": "1636668718000000",
            }),
        ];

        for message in messages.iter_mut() {
            coerce(message, &coercion_tree);
        }

        let expected = vec![
            json!({
                "level1_string": "a",
                "level1_integer": 0,
                // Timestamp passed in as an i64. We won't coerce it, but it will work anyway.
                "level1_timestamp": 1636668718000000i64,
                "level2": {
                    "level2_string": r#"{"x":"x","y":"y"}"#,
                    "level2_timestamp": 1636668718000000i64
                },
                "array_timestamp": [1637110923000000i64, 1637114584000000i64],
                "array_string": ["a", "b", r#"{"a":1}"#],
                "array_int": [1, 2, 3],
                "array_struct": [
                    {
                        "level2_string": "{\"a\":1}",
                        "level2_int": 1,
                        "level2_timestamp": 1637107201000000i64
                    },
                    {
                        "level2_string": r#"{"a":2}"#,
                        "level2_int": 2,
                        "level2_timestamp": 1637107202000000i64
                    },
                ]
            }),
            json!({
                "level1_string": r#"{"a":"a","b":"b"}"#,
                "level1_integer": 42,
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": 1636668718000000i64
            }),
            json!({
                "level1_integer": 99,
            }),
            json!({
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": 1636668718000000i64
            }),
            json!({
                // RFC 3339 but not ISO 8601. We WILL coerce it.
                "level1_timestamp": 1636668718000000i64
            }),
            json!({
                // ISO 8601 but not RFC 3339. We WON'T coerce it.
                "level1_timestamp": "20211111T22115800Z",
            }),
            json!({
                // This is a Java date style timestamp. We WON'T coerce it.
                "level1_timestamp": "2021-11-11 22:11:58",
            }),
            json!({
                "level1_timestamp": "This definitely is not a timestamp",
            }),
            json!({
                // This is valid epoch micros, but typed as a string on the way in. We WON'T coerce it.
                "level1_timestamp": "1636668718000000",
            }),
        ];

        for i in 0..messages.len() {
            assert_eq!(messages[i], expected[i]);
        }
    }
}
