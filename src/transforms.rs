use chrono::prelude::*;
use jmespatch::{
    Context, ErrorReason, Expression, JmespathError, Rcvar, Runtime, RuntimeError, Variable,
    functions::{ArgumentType, CustomFunction, Signature},
};
use rdkafka::Message;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

/// Error thrown by [`Transformer`].
#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    /// The value to transform is not a JSON object.
    #[error("Unable to mutate non-object value {value}")]
    ValueNotAnObject { value: Value },

    /// JMESPath query expression failed when querying the source value.
    #[error("JmespathError: {source}")]
    JmesPath {
        #[from]
        source: JmespathError,
    },

    /// A serde json error occurred when processing the transform.
    #[error("serde_json::Error: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },
}

// Error thrown from custom functions registered in the jmespath Runtime
struct InvalidTypeError {
    expression: String,
    offset: usize,
    expected: String,
    actual: String,
    position: usize,
}

impl InvalidTypeError {
    fn new(context: &Context, expected: &str, actual: String, position: usize) -> Self {
        Self {
            expression: context.expression.to_owned(),
            offset: context.offset,
            expected: expected.to_owned(),
            actual,
            position,
        }
    }
}

// From impl for InvalidTypeError so we can return these from within custom functions
impl From<InvalidTypeError> for JmespathError {
    fn from(err: InvalidTypeError) -> Self {
        JmespathError::new(
            err.expression.as_str(),
            err.offset,
            ErrorReason::Runtime(RuntimeError::InvalidType {
                expected: err.expected,
                actual: err.actual,
                position: err.position,
            }),
        )
    }
}

lazy_static! {
    static ref TRANSFORM_RUNTIME: Runtime = {
        let mut runtime = Runtime::new();
        runtime.register_builtin_functions();
        runtime.register_function("substr", Box::new(create_substr_fn()));
        runtime.register_function(
            "epoch_seconds_to_iso8601",
            Box::new(create_epoch_seconds_to_iso8601_fn()),
        );
        runtime.register_function(
            "epoch_millis_to_iso8601",
            Box::new(create_epoch_millis_to_iso8601_fn()),
        );
        runtime.register_function(
            "epoch_micros_to_iso8601",
            Box::new(create_epoch_micros_to_iso8601_fn()),
        );
        runtime.register_function(
            "epoch_millis_to_micro",
            Box::new(create_epoch_millis_to_micro_fn()),
        );
        runtime
    };
}

fn compile_transforms(
    definitions: &HashMap<String, String>,
) -> Result<Vec<(ValuePath, MessageTransform)>, TransformError> {
    let mut transforms = Vec::new();

    for (k, v) in definitions.iter() {
        let p = ValuePath::from_str(k);

        let t = match v.as_str() {
            "kafka.partition" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Partition),
            "kafka.offset" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Offset),
            "kafka.topic" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Topic),
            "kafka.timestamp" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Timestamp),
            "kafka.timestamp_type" => {
                MessageTransform::KafkaMetaTransform(KafkaMetaProperty::TimestampType)
            }
            _ => {
                let expression = TRANSFORM_RUNTIME.compile(v.as_str())?;

                MessageTransform::ExpressionTransform(expression)
            }
        };

        transforms.push((p, t));
    }

    Ok(transforms)
}

// Returns a Jmespath CustomFunction for selecting substrings from a string.
// This function can be registered and used within a Jmespath runtime.
//
// Logically the function signature in Rust would be:
//
// ```
// fn substr(path: Expression, skip: i32, take: i32) -> Value;
// ```
//
// For example given the object:
//
// ```
// {
//   "name": "William"
// }
// ```
//
// and the expression:
//
// ```
// substr(name,`0`,`4`)
// ```
//
// the value returned will be "Will"
//
fn create_substr_fn() -> CustomFunction {
    CustomFunction::new(
        Signature::new(
            vec![
                ArgumentType::String,
                ArgumentType::Number,
                ArgumentType::Number,
            ],
            None,
        ),
        Box::new(substr),
    )
}

// Returns a Jmespath CustomFunction for converting an epoch timestamp number to an ISO 8601
// formatted string.
//
// For example given the object:
//
// ```
// {
//   "ts": 1626823098
// }
// ```
//
// and the expression:
//
// ```
// epoch_seconds_to_iso8601(ts)
// ```
//
// the value returned will be "2021-07-20T23:18:18Z"
//
// Since functions can be nested and built-in functions are also available, an expression like below (where `ts` is a string on the original message like `"ts": "1626823098"`) works as well:
//
// ```
// epoch_seconds_to_iso8601(to_number(ts))
// ```
fn create_epoch_seconds_to_iso8601_fn() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::Number], None),
        Box::new(jmespath_epoch_seconds_to_iso8601),
    )
}

// TODO: Consolidate these custom function factories
fn create_epoch_millis_to_iso8601_fn() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::Number], None),
        Box::new(jmespath_epoch_millis_to_iso8601),
    )
}

fn create_epoch_micros_to_iso8601_fn() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::Number], None),
        Box::new(jmespath_epoch_micros_to_iso8601),
    )
}

fn create_epoch_millis_to_micro_fn() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::Number], None),
        Box::new(jmespath_epoch_millis_to_micro),
    )
}

fn substr(args: &[Rcvar], context: &mut Context) -> Result<Rcvar, JmespathError> {
    let s = args[0].as_string().ok_or_else(|| {
        InvalidTypeError::new(context, "string", args[0].get_type().to_string(), 0)
    })?;

    let start = args[1].as_number().ok_or_else(|| {
        InvalidTypeError::new(context, "number", args[0].get_type().to_string(), 1)
    })? as usize;
    let end = args[2].as_number().ok_or_else(|| {
        InvalidTypeError::new(context, "number", args[0].get_type().to_string(), 2)
    })? as usize;

    let s: String = s.chars().skip(start).take(end).collect();

    let val = serde_json::Value::String(s);

    let var = Variable::try_from(val)?;

    Ok(Arc::new(var))
}

enum EpochUnit {
    Seconds(i64),
    Milliseconds(i64),
    Microseconds(i64),
}

fn iso8601_from_epoch(epoch_unit: EpochUnit) -> String {
    let dt = match epoch_unit {
        EpochUnit::Seconds(s) => Utc.timestamp_nanos(s * 1_000_000_000),
        EpochUnit::Milliseconds(ms) => Utc.timestamp_nanos(ms * 1_000_000),
        EpochUnit::Microseconds(u) => Utc.timestamp_nanos(u * 1000),
    };

    format!("{:?}", dt)
}

fn jmespath_epoch_seconds_to_iso8601(
    args: &[Rcvar],
    context: &mut Context,
) -> Result<Rcvar, JmespathError> {
    let seconds = i64_from_args(args, context, 0)?;
    let value = serde_json::Value::String(iso8601_from_epoch(EpochUnit::Seconds(seconds)));
    let variable = Variable::try_from(value)?;
    Ok(Arc::new(variable))
}

fn jmespath_epoch_millis_to_iso8601(
    args: &[Rcvar],
    context: &mut Context,
) -> Result<Rcvar, JmespathError> {
    let millis = i64_from_args(args, context, 0)?;
    let value = serde_json::Value::String(iso8601_from_epoch(EpochUnit::Milliseconds(millis)));
    let variable = Variable::try_from(value)?;
    Ok(Arc::new(variable))
}

fn jmespath_epoch_micros_to_iso8601(
    args: &[Rcvar],
    context: &mut Context,
) -> Result<Rcvar, JmespathError> {
    let micros = i64_from_args(args, context, 0)?;
    let value = serde_json::Value::String(iso8601_from_epoch(EpochUnit::Microseconds(micros)));
    let variable = Variable::try_from(value)?;
    Ok(Arc::new(variable))
}
fn jmespath_epoch_millis_to_micro(
    args: &[Rcvar],
    context: &mut Context,
) -> Result<Rcvar, JmespathError> {
    let millis = i64_from_args(args, context, 0)?;
    let variable = Variable::Number((millis * 1000).into());
    Ok(Arc::new(variable))
}

fn i64_from_args(
    args: &[Rcvar],
    context: &mut Context,
    position: usize,
) -> Result<i64, JmespathError> {
    let n = args[0].as_number().ok_or_else(|| {
        InvalidTypeError::new(
            context,
            "number",
            args[position].get_type().to_string(),
            position,
        )
    })?;

    let n = n as i64;

    Ok(n)
}

enum KafkaMetaProperty {
    Partition,
    Offset,
    Topic,
    Timestamp,
    TimestampType,
}

enum MessageTransform {
    KafkaMetaTransform(KafkaMetaProperty),
    ExpressionTransform(Expression<'static>),
}

struct ValuePath {
    parts: Vec<String>,
}

impl ValuePath {
    fn from_str(path: &str) -> Self {
        let parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();

        ValuePath { parts }
    }

    fn part_at(&self, index: usize) -> Option<&str> {
        self.parts.get(index).map(|s| s.as_ref())
    }

    fn len(&self) -> usize {
        self.parts.len()
    }
}

fn set_value(object: &mut Map<String, Value>, path: &ValuePath, path_index: usize, value: Value) {
    match value {
        // Don't set if the extracted value is null.
        Value::Null => { /* noop */ }
        _ => {
            let part = path.part_at(path_index);

            if let Some(property) = part {
                if path_index == path.len() - 1 {
                    // this is the leaf property - set value on the current object in context.
                    object.insert(property.to_string(), value);
                } else if let Some(next_o) =
                    object.get_mut(property).and_then(|v| v.as_object_mut())
                {
                    // the next object already exists on the object. recurse.
                    set_value(next_o, path, path_index + 1, value);
                } else {
                    // this is not the leaf property and the parent object does not exist yet.
                    // create an object, then recurse.
                    let mut next_o = Map::new();
                    set_value(&mut next_o, path, path_index + 1, value);
                    object.insert(property.to_string(), Value::Object(next_o));
                }
            }

            // recursion ends when we run out of path parts.
        }
    }
}

/// Transforms JSON values deserialized from a Kafka topic.
pub(crate) struct Transformer {
    transforms: Vec<(ValuePath, MessageTransform)>,
}

impl Transformer {
    /// Creates a new transformer which executes each provided transform on the passed JSON value.
    ///
    /// Transforms should be provided as a HashMap where the key is the property the transformed value should be assigned to
    /// and the value is the JMESPath query expression or well known Kafka metadata property to assign.
    pub fn from_transforms(transforms: &HashMap<String, String>) -> Result<Self, TransformError> {
        let transforms = compile_transforms(transforms)?;

        Ok(Self { transforms })
    }

    /// Transforms a [`Value`] according to the list of transforms used to create the [`Transformer`].
    /// The optional `kafka_message` must be provided to include well known Kafka properties in the value.
    pub(crate) fn transform<M>(
        &self,
        value: &mut Value,
        kafka_message: Option<&M>,
    ) -> Result<(), TransformError>
    where
        M: Message,
    {
        let data = Variable::try_from(value.clone())?;

        match value.as_object_mut() {
            Some(map) => {
                for (value_path, message_transform) in self.transforms.iter() {
                    match message_transform {
                        MessageTransform::ExpressionTransform(expression) => {
                            apply_expression_transform(map, &data, value_path, expression)?;
                        }
                        MessageTransform::KafkaMetaTransform(meta_property) => {
                            if let Some(kafka_message) = kafka_message {
                                apply_kafka_meta_transform(
                                    map,
                                    value_path,
                                    kafka_message,
                                    meta_property,
                                )?;
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Err(TransformError::ValueNotAnObject {
                value: value.to_owned(),
            }),
        }
    }
}

fn apply_expression_transform(
    object_mut: &mut Map<String, Value>,
    message_variable: &Variable,
    value_path: &ValuePath,
    expression: &Expression,
) -> Result<(), TransformError> {
    let variable = expression.search(message_variable)?;
    let v = serde_json::to_value(variable)?;
    set_value(object_mut, value_path, 0, v);
    Ok(())
}

fn apply_kafka_meta_transform<M>(
    object_mut: &mut Map<String, Value>,
    value_path: &ValuePath,
    kafka_message: &M,
    meta_property: &KafkaMetaProperty,
) -> Result<(), TransformError>
where
    M: Message,
{
    let v = match meta_property {
        KafkaMetaProperty::Partition => serde_json::to_value(kafka_message.partition())?,
        KafkaMetaProperty::Offset => serde_json::to_value(kafka_message.offset())?,
        KafkaMetaProperty::Topic => serde_json::to_value(kafka_message.topic())?,
        KafkaMetaProperty::Timestamp => timestamp_value_from_kafka(kafka_message.timestamp())?,
        // For enum int value definitions, see:
        // https://github.com/apache/kafka/blob/fd36e5a8b657b0858dbfef4ae9706bf714db4ca7/clients/src/main/java/org/apache/kafka/common/record/TimestampType.java#L24-L46
        KafkaMetaProperty::TimestampType => match kafka_message.timestamp() {
            rdkafka::Timestamp::NotAvailable => serde_json::to_value(-1)?,
            rdkafka::Timestamp::CreateTime(_) => serde_json::to_value(0)?,
            rdkafka::Timestamp::LogAppendTime(_) => serde_json::to_value(1)?,
        },
    };
    set_value(object_mut, value_path, 0, v);
    Ok(())
}

fn timestamp_value_from_kafka(
    kafka_timestamp: rdkafka::Timestamp,
) -> Result<Value, serde_json::Error> {
    match kafka_timestamp {
        rdkafka::Timestamp::NotAvailable => serde_json::to_value(None as Option<String>),
        // Convert to milliseconds to microseconds for delta format
        rdkafka::Timestamp::CreateTime(ms) => serde_json::to_value(ms * 1000),
        rdkafka::Timestamp::LogAppendTime(ms) => serde_json::to_value(ms * 1000),
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::message::OwnedMessage;

    use super::*;
    use serde_json::json;

    #[test]
    fn substr_returns_will_from_william() {
        let args = &[
            Arc::new(Variable::String("William".to_owned())),
            Arc::new(Variable::Number(
                serde_json::Number::from_f64(0f64).unwrap(),
            )),
            Arc::new(Variable::Number(
                serde_json::Number::from_f64(4f64).unwrap(),
            )),
        ];

        let runtime = Runtime::new();
        let mut context = Context::new("X", &runtime);

        let s = substr(args, &mut context).unwrap();
        let s = s.as_string().unwrap().as_str();

        assert_eq!("Will", s);
    }

    #[test]
    fn substr_returns_liam_from_william() {
        let args = &[
            Arc::new(Variable::String("William".to_owned())),
            Arc::new(Variable::Number(
                serde_json::Number::from_f64(3f64).unwrap(),
            )),
            Arc::new(Variable::Number(
                serde_json::Number::from_f64(4f64).unwrap(),
            )),
        ];

        let runtime = Runtime::new();
        let mut context = Context::new("X", &runtime);

        let s = substr(args, &mut context).unwrap();
        let s = s.as_string().unwrap().as_str();

        assert_eq!("liam", s);
    }

    #[test]
    fn set_value_sets_recursively() {
        let mut val = json!(
            {
                "name": {
                    "last": "Doe"
                },
            }
        );

        let new_value_path = ValuePath::from_str("name.first");
        let new_value = Value::String("John".to_string());

        set_value(val.as_object_mut().unwrap(), &new_value_path, 0, new_value);

        assert_eq!(
            json!({
                "name": {
                    "first": "John",
                    "last": "Doe"
                }
            }),
            val
        );
    }

    #[test]
    fn transforms_with_substr() {
        let mut test_value = json!({
            "name": "A",
            "modified": "2021-03-16T14:38:58Z",
        });

        let test_message = OwnedMessage::new(
            Some(test_value.to_string().into_bytes()),
            None,
            "test".to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        let mut transforms = HashMap::new();

        transforms.insert(
            "modified_date".to_string(),
            "substr(modified, `0`, `10`)".to_string(),
        );

        let transformer = Transformer::from_transforms(&transforms).unwrap();

        transformer
            .transform(&mut test_value, Some(&test_message))
            .unwrap();

        let name = test_value.get("name").unwrap().as_str().unwrap();
        let modified = test_value.get("modified").unwrap().as_str().unwrap();
        let modified_date = test_value.get("modified_date").unwrap().as_str().unwrap();

        assert_eq!("A", name);
        assert_eq!("2021-03-16T14:38:58Z", modified);
        assert_eq!("2021-03-16", modified_date);
    }

    #[test]
    fn test_iso8601_from_epoch_seconds_test() {
        let expected_iso = "2021-07-20T23:18:18Z";
        let dt = iso8601_from_epoch(EpochUnit::Seconds(1626823098));

        assert_eq!(expected_iso, dt);
    }

    #[test]
    fn test_iso8601_from_epoch_micros_test() {
        let expected_iso = "2021-07-20T23:18:18Z";
        let dt = iso8601_from_epoch(EpochUnit::Microseconds(1626823098000000));

        assert_eq!(expected_iso, dt);
    }

    #[test]
    fn test_epoch_millis_to_micro() {
        let mut test_value = json!({
            "name": "A",
            "modified": 1732279537028u64,
        });

        let test_message = OwnedMessage::new(
            Some(test_value.to_string().into_bytes()),
            None,
            "test".to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        let mut transforms = HashMap::new();

        transforms.insert(
            "modified_micros".to_string(),
            "epoch_millis_to_micro(modified)".to_string(),
        );

        let transformer = Transformer::from_transforms(&transforms).unwrap();

        transformer
            .transform(&mut test_value, Some(&test_message))
            .unwrap();

        let modified_date = test_value.get("modified_micros").unwrap().as_u64().unwrap();

        assert_eq!(1732279537028000u64, modified_date);
    }

    #[test]
    fn test_transforms_with_epoch_seconds_to_iso8601() {
        let expected_iso = "2021-07-20T23:18:18Z";

        let mut test_value = json!({
            "name": "A",
            "epoch_seconds_float": 1626823098.51995,
            "epoch_seconds_int": 1626823098,
            "epoch_seconds_float_string": "1626823098.51995",
            "epoch_seconds_int_string": "1626823098",
        });

        let test_message = OwnedMessage::new(
            Some(test_value.to_string().into_bytes()),
            None,
            "test".to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        let mut transforms = HashMap::new();
        transforms.insert(
            "iso8601_from_float".to_string(),
            "epoch_seconds_to_iso8601(epoch_seconds_float)".to_string(),
        );
        transforms.insert(
            "iso8601_from_int".to_string(),
            "epoch_seconds_to_iso8601(epoch_seconds_int)".to_string(),
        );
        transforms.insert(
            "iso8601_from_float_string".to_string(),
            "epoch_seconds_to_iso8601(to_number(epoch_seconds_float_string))".to_string(),
        );
        transforms.insert(
            "iso8601_from_int_string".to_string(),
            "epoch_seconds_to_iso8601(to_number(epoch_seconds_int_string))".to_string(),
        );

        let transformer = Transformer::from_transforms(&transforms).unwrap();

        transformer
            .transform(&mut test_value, Some(&test_message))
            .unwrap();

        let name = test_value.get("name").unwrap().as_str().unwrap();
        let iso8601_from_float = test_value
            .get("iso8601_from_float")
            .unwrap()
            .as_str()
            .unwrap();
        let iso8601_from_int = test_value
            .get("iso8601_from_int")
            .unwrap()
            .as_str()
            .unwrap();
        let iso8601_from_float_string = test_value
            .get("iso8601_from_float_string")
            .unwrap()
            .as_str()
            .unwrap();
        let iso8601_from_int_string = test_value
            .get("iso8601_from_int_string")
            .unwrap()
            .as_str()
            .unwrap();

        assert_eq!("A", name);
        assert_eq!(expected_iso, iso8601_from_float);
        assert_eq!(expected_iso, iso8601_from_int);
        assert_eq!(expected_iso, iso8601_from_float_string);
        assert_eq!(expected_iso, iso8601_from_int_string);
    }

    #[test]
    fn test_transforms_with_kafka_meta() {
        let mut test_value = json!({
            "name": "A",
            "modified": "2021-03-16T14:38:58Z",
        });

        let test_message = OwnedMessage::new(
            Some(test_value.to_string().into_bytes()),
            None,
            "test".to_string(),
            rdkafka::Timestamp::CreateTime(1626823098519),
            0,
            0,
            None,
        );

        let mut transforms = HashMap::new();

        transforms.insert("_kafka_offset".to_string(), "kafka.offset".to_string());
        transforms.insert(
            "_kafka_partition".to_string(),
            "kafka.partition".to_string(),
        );
        transforms.insert("_kafka_topic".to_string(), "kafka.topic".to_string());
        transforms.insert(
            "_kafka_timestamp".to_string(),
            "kafka.timestamp".to_string(),
        );
        transforms.insert(
            "_kafka_timestamp_type".to_string(),
            "kafka.timestamp_type".to_string(),
        );

        let transformer = Transformer::from_transforms(&transforms).unwrap();

        transformer
            .transform(&mut test_value, Some(&test_message))
            .unwrap();

        let name = test_value.get("name").unwrap().as_str().unwrap();
        let modified = test_value.get("modified").unwrap().as_str().unwrap();

        let kafka_offset = test_value.get("_kafka_offset").unwrap().as_i64().unwrap();
        let kafka_partition = test_value
            .get("_kafka_partition")
            .unwrap()
            .as_i64()
            .unwrap();
        let kafka_topic = test_value.get("_kafka_topic").unwrap().as_str().unwrap();
        let kafka_timestamp = test_value
            .get("_kafka_timestamp")
            .unwrap()
            .as_i64()
            .unwrap();
        let kafka_timestamp_type = test_value
            .get("_kafka_timestamp_type")
            .unwrap()
            .as_i64()
            .unwrap();

        assert_eq!("A", name);
        assert_eq!("2021-03-16T14:38:58Z", modified);
        assert_eq!(0i64, kafka_offset);
        assert_eq!(0i64, kafka_partition);
        assert_eq!("test", kafka_topic);
        assert_eq!(1626823098519000, kafka_timestamp);
        assert_eq!(0, kafka_timestamp_type);
    }
}
