use std::{borrow::BorrowMut, convert::TryFrom, io::Cursor, path::PathBuf};

use async_trait::async_trait;
use schema_registry_converter::async_impl::{
    easy_avro::EasyAvroDecoder, easy_json::EasyJsonDecoder, schema_registry::SrSettings,
};
use serde_json::Value;

use crate::{dead_letters::DeadLetter, MessageDeserializationError, MessageFormat};

use deltalake_core::arrow::datatypes::Schema as ArrowSchema;

/// Structure which contains the [serde_json::Value] and the inferred schema of the message
///
/// The [ArrowSchema] helps with schema evolution
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeserializedMessage {
    message: Value,
    schema: Option<ArrowSchema>,
}

impl DeserializedMessage {
    pub fn schema(&self) -> &Option<ArrowSchema> {
        &self.schema
    }
    pub fn message(self) -> Value {
        self.message
    }
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.message.get(key)
    }
    pub fn as_object_mut(&mut self) -> Option<&mut serde_json::Map<String, Value>> {
        self.message.as_object_mut()
    }
}

/// Allow for `.into()` on [Value] for ease of use
impl From<Value> for DeserializedMessage {
    fn from(message: Value) -> Self {
        // XXX: This seems wasteful, this function should go away, and the deserializers should
        // infer straight from the buffer stream
        let iter = vec![message.clone()].into_iter().map(|v| Ok(v));
        let schema =
            match deltalake_core::arrow::json::reader::infer_json_schema_from_iterator(iter) {
                Ok(schema) => Some(schema),
                _ => None,
            };
        Self { message, schema }
    }
}

#[async_trait]
pub(crate) trait MessageDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<DeserializedMessage, MessageDeserializationError>;
}

pub(crate) struct MessageDeserializerFactory {}
impl MessageDeserializerFactory {
    pub fn try_build(
        input_format: &MessageFormat,
    ) -> Result<Box<dyn MessageDeserializer + Send>, anyhow::Error> {
        match input_format {
            MessageFormat::Json(data) => match data {
                crate::SchemaSource::None => Ok(Self::json_default()),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    match Self::build_sr_settings(sr).map(JsonDeserializer::from_schema_registry) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(_) => Ok(Self::json_default()),
            },
            MessageFormat::Avro(data) => match data {
                crate::SchemaSource::None => Ok(Box::<AvroSchemaDeserializer>::default()),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    match Self::build_sr_settings(sr).map(AvroDeserializer::from_schema_registry) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(f) => {
                    match AvroSchemaDeserializer::try_from_schema_file(f) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
            },
            _ => Ok(Box::new(DefaultDeserializer {})),
        }
    }

    fn json_default() -> Box<dyn MessageDeserializer + Send> {
        Box::new(DefaultDeserializer {})
    }

    fn build_sr_settings(registry_url: &url::Url) -> Result<SrSettings, anyhow::Error> {
        let mut url_string = registry_url.as_str();
        if url_string.ends_with('/') {
            url_string = &url_string[0..url_string.len() - 1];
        }

        let mut builder = SrSettings::new_builder(url_string.to_owned());
        if let Ok(username) = std::env::var("SCHEMA_REGISTRY_USERNAME") {
            builder.set_basic_authorization(
                username.as_str(),
                std::option_env!("SCHEMA_REGISTRY_PASSWORD"),
            );
        }

        if let Ok(proxy_url) = std::env::var("SCHEMA_REGISTRY_PROXY") {
            builder.set_proxy(proxy_url.as_str());
        }

        match builder.build() {
            Ok(s) => Ok(s),
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct DefaultDeserializer {}

#[async_trait]
impl MessageDeserializer for DefaultDeserializer {
    async fn deserialize(
        &mut self,
        payload: &[u8],
    ) -> Result<DeserializedMessage, MessageDeserializationError> {
        let value: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                return Err(MessageDeserializationError::JsonDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(payload, e.to_string()),
                });
            }
        };

        Ok(value.into())
    }
}

#[cfg(test)]
mod default_tests {
    use super::*;

    #[tokio::test]
    async fn deserialize_with_schema() {
        let mut deser = DefaultDeserializer::default();
        let message = deser
            .deserialize(r#"{"hello" : "world"}"#.as_bytes())
            .await
            .expect("Failed to deserialize trivial JSON");
        assert!(
            message.schema().is_some(),
            "The DeserializedMessage doesn't have a schema!"
        );
    }

    #[tokio::test]
    async fn deserialize_simple_json() {
        #[derive(serde::Deserialize)]
        struct HW {
            hello: String,
        }

        let mut deser = DefaultDeserializer::default();
        let message = deser
            .deserialize(r#"{"hello" : "world"}"#.as_bytes())
            .await
            .expect("Failed to deserialize trivial JSON");
        let value: HW = serde_json::from_value(message.message).expect("Failed to coerce");
        assert_eq!("world", value.hello);
    }
}

struct AvroDeserializer {
    decoder: EasyAvroDecoder,
}

#[derive(Default)]
struct AvroSchemaDeserializer {
    schema: Option<apache_avro::Schema>,
}

struct JsonDeserializer {
    decoder: EasyJsonDecoder,
}

#[async_trait]
impl MessageDeserializer for AvroDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<DeserializedMessage, MessageDeserializationError> {
        match self.decoder.decode_with_schema(Some(message_bytes)).await {
            Ok(drs) => match drs {
                Some(v) => match Value::try_from(v.value) {
                    Ok(v) => Ok(v.into()),
                    Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            e.to_string(),
                        ),
                    }),
                },
                None => return Err(MessageDeserializationError::EmptyPayload),
            },
            Err(e) => {
                return Err(MessageDeserializationError::AvroDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        message_bytes,
                        e.to_string(),
                    ),
                });
            }
        }
    }
}

#[async_trait]
impl MessageDeserializer for AvroSchemaDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<DeserializedMessage, MessageDeserializationError> {
        let reader_result = match &self.schema {
            None => apache_avro::Reader::new(Cursor::new(message_bytes)),
            Some(schema) => apache_avro::Reader::with_schema(schema, Cursor::new(message_bytes)),
        };

        match reader_result {
            Ok(mut reader) => {
                if let Some(r) = reader.next() {
                    let v = match r {
                        Err(_) => return Err(MessageDeserializationError::EmptyPayload),
                        Ok(v) => Value::try_from(v),
                    };

                    return match v {
                        Ok(value) => Ok(value.into()),
                        Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                            dead_letter: DeadLetter::from_failed_deserialization(
                                message_bytes,
                                e.to_string(),
                            ),
                        }),
                    };
                }

                return Err(MessageDeserializationError::EmptyPayload);
                // TODO: Code to return multiple values from avro message
                /*let (values, errors): (Vec<_>, Vec<_>) =
                    reader.into_iter().partition(Result::is_ok);
                if errors.len() > 0 {
                    let error_string = errors
                        .iter()
                        .map(|m| m.err().unwrap().to_string())
                        .fold(String::new(), |current, next| current + "\n" + &next);
                    return Err(MessageDeserializationError::AvroDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            error_string,
                        ),
                    });
                }
                let (transformed, t_errors): (Vec<_>, Vec<_>) = values
                    .into_iter()
                    .map(|v| v.unwrap())
                    .map(Value::try_from)
                    .partition(Result::is_ok);

                if t_errors.len() > 0 {
                    let error_string = t_errors
                        .iter()
                        .map(|m| m.err().unwrap().to_string())
                        .fold(String::new(), |current, next| current + "\n" + &next);
                    return Err(MessageDeserializationError::AvroDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            error_string,
                        ),
                    });
                }

                Ok(transformed.into_iter().map(|m| m.unwrap()).collect())*/
            }
            Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                dead_letter: DeadLetter::from_failed_deserialization(message_bytes, e.to_string()),
            }),
        }
    }
}

#[async_trait]
impl MessageDeserializer for JsonDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<DeserializedMessage, MessageDeserializationError> {
        let decoder = self.decoder.borrow_mut();
        match decoder.decode(Some(message_bytes)).await {
            Ok(drs) => match drs {
                Some(v) => Ok(v.value.into()),
                None => return Err(MessageDeserializationError::EmptyPayload),
            },
            Err(e) => {
                return Err(MessageDeserializationError::AvroDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        message_bytes,
                        e.to_string(),
                    ),
                });
            }
        }
    }
}
impl JsonDeserializer {
    pub(crate) fn from_schema_registry(sr_settings: SrSettings) -> Self {
        JsonDeserializer {
            decoder: EasyJsonDecoder::new(sr_settings),
        }
    }
}

impl AvroSchemaDeserializer {
    pub(crate) fn try_from_schema_file(file: &PathBuf) -> Result<Self, anyhow::Error> {
        match std::fs::read_to_string(file) {
            Ok(content) => match apache_avro::Schema::parse_str(&content) {
                Ok(s) => Ok(AvroSchemaDeserializer { schema: Some(s) }),
                Err(e) => Err(anyhow::format_err!("{}", e.to_string())),
            },
            Err(e) => Err(anyhow::format_err!("{}", e.to_string())),
        }
    }
}

impl AvroDeserializer {
    pub(crate) fn from_schema_registry(sr_settings: SrSettings) -> Self {
        AvroDeserializer {
            decoder: EasyAvroDecoder::new(sr_settings),
        }
    }
}

#[cfg(test)]
mod tests {}
