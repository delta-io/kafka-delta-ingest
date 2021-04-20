use jmespatch::{
    functions::{ArgumentType, CustomFunction, Signature},
    Context, ErrorReason, Expression, JmespathError, Rcvar, Runtime, RuntimeError, Variable,
};
use rdkafka::Message;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error("Unable to mutate non-object value {value}")]
    ValueNotAnObject { value: Value },

    #[error("JmespathError: {source}")]
    JmesPath {
        #[from]
        source: JmespathError,
    },

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

impl InvalidTypeError {
    fn new(context: &Context, expected: &str, actual: String, position: usize) -> Self {
        Self {
            expression: context.expression.to_owned(),
            offset: context.offset,
            expected: expected.to_owned(),
            actual: actual.to_string(),
            position: position,
        }
    }
}

lazy_static! {
    static ref TRANSFORM_RUNTIME: Runtime = {
        let mut runtime = Runtime::new();
        runtime.register_builtin_functions();
        runtime.register_function("substr", Box::new(create_substr_fn()));
        runtime
    };
}

pub fn compile_transforms(
    definitions: &HashMap<String, String>,
) -> Result<HashMap<String, MessageTransform>, TransformError> {
    let mut transforms = HashMap::new();

    for (k, v) in definitions.iter() {
        let t = match v.as_str() {
            "kafka.partition" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Partition),
            "kafka.offset" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Offset),
            "kafka.topic" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Topic),
            "kafka.timestamp" => MessageTransform::KafkaMetaTransform(KafkaMetaProperty::Timestamp),
            _ => {
                let expression = TRANSFORM_RUNTIME.compile(v.as_str())?;

                MessageTransform::ExpressionTransform(expression)
            }
        };

        transforms.insert(k.to_owned(), t);
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

pub enum KafkaMetaProperty {
    Partition,
    Offset,
    Topic,
    Timestamp,
}

pub enum MessageTransform {
    KafkaMetaTransform(KafkaMetaProperty),
    ExpressionTransform(Expression<'static>),
}

pub struct Transformer {
    transforms: HashMap<String, MessageTransform>,
}

impl Transformer {
    pub fn new(transforms: HashMap<String, MessageTransform>) -> Self {
        Self { transforms }
    }

    pub fn from_transforms(transforms: &HashMap<String, String>) -> Result<Self, TransformError> {
        let transforms = compile_transforms(transforms)?;

        Ok(Self { transforms })
    }

    pub fn transform<M>(&self, value: &mut Value, kafka_message: &M) -> Result<(), TransformError>
    where
        M: Message,
    {
        let data = Variable::try_from(value.clone())?;

        match value.as_object_mut() {
            Some(map) => {
                for (property, message_transform) in self.transforms.iter() {
                    let property_value = match message_transform {
                        MessageTransform::ExpressionTransform(expression) => {
                            let variable = expression.search(&data)?;
                            serde_json::to_value(variable)?
                        }
                        MessageTransform::KafkaMetaTransform(meta_property) => {
                            match meta_property {
                                KafkaMetaProperty::Partition => {
                                    serde_json::to_value(kafka_message.partition())?
                                }
                                KafkaMetaProperty::Offset => {
                                    serde_json::to_value(kafka_message.offset())?
                                }
                                KafkaMetaProperty::Topic => {
                                    serde_json::to_value(kafka_message.topic())?
                                }
                                KafkaMetaProperty::Timestamp => {
                                    serde_json::to_value(kafka_message.timestamp().to_millis())?
                                }
                            }
                        }
                    };

                    map.insert(property.to_string(), property_value);
                }
                Ok(())
            }
            _ => Err(TransformError::ValueNotAnObject {
                value: value.to_owned(),
            }),
        }
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

        let expressions = compile_transforms(&transforms).unwrap();

        let transformer = Transformer::new(expressions);

        let _ = transformer
            .transform(&mut test_value, &test_message)
            .unwrap();

        let name = test_value.get("name").unwrap().as_str().unwrap();
        let modified = test_value.get("modified").unwrap().as_str().unwrap();
        let modified_date = test_value.get("modified_date").unwrap().as_str().unwrap();

        assert_eq!("A", name);
        assert_eq!("2021-03-16T14:38:58Z", modified);
        assert_eq!("2021-03-16", modified_date);
    }

    #[test]
    fn transforms_with_kafka_meta() {
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

        transforms.insert("_kafka_offset".to_string(), "kafka.offset".to_string());
        transforms.insert(
            "_kafka_partition".to_string(),
            "kafka.partition".to_string(),
        );
        transforms.insert("_kafka_topic".to_string(), "kafka.topic".to_string());

        let expressions = compile_transforms(&transforms).unwrap();

        let transformer = Transformer::new(expressions);

        let _ = transformer
            .transform(&mut test_value, &test_message)
            .unwrap();

        let name = test_value.get("name").unwrap().as_str().unwrap();
        let modified = test_value.get("modified").unwrap().as_str().unwrap();
        let kafka_topic = test_value.get("_kafka_topic").unwrap().as_str().unwrap();

        let kafka_offset = test_value.get("_kafka_offset").unwrap().as_i64().unwrap();
        let kafka_partition = test_value
            .get("_kafka_partition")
            .unwrap()
            .as_i64()
            .unwrap();

        assert_eq!("A", name);
        assert_eq!("2021-03-16T14:38:58Z", modified);
        assert_eq!(0i64, kafka_offset);
        assert_eq!(0i64, kafka_partition);
        assert_eq!("test", kafka_topic);
    }
}
