use crate::{
    dead_letters::DeadLetter, DeserializedMessage, MessageDeserializationError,
    MessageDeserializer, MessageFormat,
};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use schema_registry_converter::async_impl::{
    easy_avro::EasyAvroDecoder, easy_json::EasyJsonDecoder, schema_registry::SrSettings,
};
use serde_json::Value;
use std::{borrow::BorrowMut, convert::TryFrom, io::Cursor, io::Read, path::PathBuf};

pub(crate) struct AvroDeserializer {
    decoder: EasyAvroDecoder,
}

#[derive(Default)]
pub(crate) struct AvroSchemaDeserializer {
    schema: Option<apache_avro::Schema>,
}

pub(crate) struct JsonDeserializer {
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
