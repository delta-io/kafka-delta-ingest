use async_trait::async_trait;
use serde_json::Value;

use crate::{dead_letters::DeadLetter, MessageDeserializationError, MessageFormat};

use deltalake_core::arrow::datatypes::Schema as ArrowSchema;

#[cfg(feature = "avro")]
use schema_registry_converter::async_impl::schema_registry::SrSettings;

#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "avro")]
use crate::serialization::avro::*;

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
        let iter = vec![message.clone()].into_iter().map(Ok);
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
        schema_evolution: bool,
    ) -> Result<Box<dyn MessageDeserializer + Send>, anyhow::Error> {
        match input_format {
            #[cfg(feature = "avro")]
            MessageFormat::Json(data) => match data {
                crate::SchemaSource::None => Ok(Self::json_default(schema_evolution)),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    if schema_evolution {
                        warn!("Schema evolution is not currently implemented for Avro enabled topics!");
                    }
                    match Self::build_sr_settings(sr).map(JsonDeserializer::from_schema_registry) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(_) => Ok(Self::json_default(schema_evolution)),
            },
            #[cfg(feature = "avro")]
            MessageFormat::Avro(data) => match data {
                crate::SchemaSource::None => Ok(Box::<AvroSchemaDeserializer>::default()),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    if schema_evolution {
                        warn!("Schema evolution is not currently implemented for Avro enabled topics!");
                    }
                    match Self::build_sr_settings(sr).map(AvroDeserializer::from_schema_registry) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(f) => {
                    if schema_evolution {
                        warn!("Schema evolution is not currently implemented for Avro enabled topics!");
                    }
                    match AvroSchemaDeserializer::try_from_schema_file(f) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
            },
            _ => Ok(Self::json_default(schema_evolution)),
        }
    }

    fn json_default(schema_evolution: bool) -> Box<dyn MessageDeserializer + Send> {
        Box::new(DefaultDeserializer { schema_evolution })
    }

    #[cfg(feature = "avro")]
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

#[allow(unused)]
#[derive(Clone, Debug, Default)]
struct DefaultDeserializer {
    /// Whether the serializer can support schema evolution or not
    schema_evolution: bool,
}

#[allow(unused)]
impl DefaultDeserializer {
    // TODO: This would be good to move into the trait itself
    /// Return true if the serializer provides schemas to enable schema evolution
    pub fn can_evolve_schema(&self) -> bool {
        self.schema_evolution
    }
}

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
    #[test]
    fn deserializer_default_evolution() {
        let deser = DefaultDeserializer::default();
        assert_eq!(false, deser.can_evolve_schema());
    }

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
