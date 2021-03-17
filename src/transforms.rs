use jmespath::{
    functions::{ArgumentType, CustomFunction, Signature},
    Context, Expression, Rcvar, Runtime,
};
use rdkafka::Message;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error("Unable to mutate non-object value {value}")]
    ValueNotAnObject { value: Value },

    #[error("JmespathError: {source}")]
    JmesPath {
        #[from]
        source: jmespath::JmespathError,
    },

    #[error("serde_json::Error: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },
}

lazy_static! {
    pub(crate) static ref TRANSFORM_RUNTIME: Runtime = {
        let mut runtime = Runtime::new();
        runtime.register_builtin_functions();
        runtime.register_function("substr", Box::new(custom_substring()));
        runtime
    };
}

pub(crate) fn compile_transforms(
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
                let expression = expression;

                MessageTransform::ExpressionTransform(expression)
            }
        };

        transforms.insert(k.to_owned(), t);
    }

    Ok(transforms)
}

fn custom_substring() -> CustomFunction {
    CustomFunction::new(
        Signature::new(
            vec![
                ArgumentType::String,
                ArgumentType::String,
                ArgumentType::String,
            ],
            None,
        ),
        Box::new(|args: &[Rcvar], _context: &mut Context| {
            let s = args[0].as_string().unwrap();

            let start = args[1].as_string().unwrap().parse::<usize>().unwrap();
            let end = args[2].as_string().unwrap().parse::<usize>().unwrap();

            let s: String = s.chars().skip(start).take(end).collect();

            let var = jmespath::Variable::from(serde_json::Value::String(s));

            Ok(Arc::new(var))
        }),
    )
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

pub(crate) struct TransformContext {
    transforms: HashMap<String, MessageTransform>,
}

impl TransformContext {
    pub(crate) fn new(transforms: HashMap<String, MessageTransform>) -> Self {
        Self { transforms }
    }

    pub(crate) fn transform<M>(
        &self,
        value: &mut Value,
        kafka_message: &M,
    ) -> Result<(), TransformError>
    where
        M: Message,
    {
        let data = jmespath::Variable::from(value.clone());

        match value.as_object_mut() {
            Some(map) => {
                for (property, message_transform) in self.transforms.iter() {
                    let property_value = match message_transform {
                        MessageTransform::ExpressionTransform(expression) => {
                            let variable = expression.search(data.clone())?;
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
            "substr(modified, '0', '10')".to_string(),
        );

        let expressions = compile_transforms(&transforms).unwrap();

        let transformer = TransformContext::new(expressions);

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

        let transformer = TransformContext::new(expressions);

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
