use crate::{MessageDeserializationError, MessageFormat, dead_letters::DeadLetter};
use async_trait::async_trait;
use dashmap::DashMap;
use flate2::read::GzDecoder;
use schema_registry_converter::async_impl::{
    easy_avro::EasyAvroDecoder, easy_json::EasyJsonDecoder, schema_registry::SrSettings,
};
use serde_json::Value;

// use crate::avro_canonical_schema_workaround::parse_into_canonical_form;
use apache_avro::{GenericSingleObjectReader, Schema, rabin::Rabin};
use std::{
    borrow::BorrowMut,
    convert::{TryFrom, TryInto},
    io::{Cursor, Read},
    path::PathBuf,
};

use log::debug;

#[async_trait]
pub(crate) trait MessageDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<Value, MessageDeserializationError>;
}

pub(crate) struct MessageDeserializerFactory {}

impl MessageDeserializerFactory {
    pub fn try_build(
        input_format: &MessageFormat,
        decompress_gzip: bool, // Add this parameter
    ) -> Result<Box<dyn MessageDeserializer + Send>, anyhow::Error> {
        match input_format {
            MessageFormat::Json(data) => match data {
                crate::SchemaSource::None => Ok(Self::json_default(decompress_gzip)),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    match Self::build_sr_settings(sr).map(JsonDeserializer::from_schema_registry) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(_) => Ok(Self::json_default(decompress_gzip)),
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
            MessageFormat::SoeAvro(path) => match SoeAvroDeserializer::try_from_path(path) {
                Ok(s) => Ok(Box::new(s)),
                Err(e) => Err(e),
            },
            _ => Ok(Box::new(DefaultDeserializer::new(decompress_gzip))),
        }
    }

    fn json_default(decompress_gzip: bool) -> Box<dyn MessageDeserializer + Send> {
        Box::new(DefaultDeserializer::new(decompress_gzip))
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

struct DefaultDeserializer {
    decompress_gzip: bool,
}

impl DefaultDeserializer {
    pub fn new(decompress_gzip: bool) -> Self {
        DefaultDeserializer { decompress_gzip }
    }

    fn decompress(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut decoder = GzDecoder::new(bytes);
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data)?;
        Ok(decompressed_data)
    }
}

#[async_trait]
impl MessageDeserializer for DefaultDeserializer {
    async fn deserialize(&mut self, payload: &[u8]) -> Result<Value, MessageDeserializationError> {
        let payload = if self.decompress_gzip {
            Self::decompress(payload).map_err(|e| {
                MessageDeserializationError::JsonDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(payload, e.to_string()),
                }
            })?
        } else {
            payload.to_vec()
        };

        let value: Value = match serde_json::from_slice(&payload) {
            Ok(v) => v,
            Err(e) => {
                return Err(MessageDeserializationError::JsonDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(&payload, e.to_string()),
                });
            }
        };

        Ok(value)
    }
}

struct AvroDeserializer {
    decoder: EasyAvroDecoder,
}

struct SoeAvroDeserializer {
    //Deserializer for avro single object encoding
    decoders: DashMap<i64, GenericSingleObjectReader>,
}

#[derive(Default)]
struct AvroSchemaDeserializer {
    schema: Option<apache_avro::Schema>,
}

struct JsonDeserializer {
    decoder: EasyJsonDecoder,
}

#[async_trait]
impl MessageDeserializer for SoeAvroDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<Value, MessageDeserializationError> {
        let key = Self::extract_message_fingerprint(message_bytes).map_err(|e| {
            MessageDeserializationError::AvroDeserialization {
                dead_letter: DeadLetter::from_failed_deserialization(message_bytes, e.to_string()),
            }
        })?;

        let decoder =
            self.decoders
                .get(&key)
                .ok_or(MessageDeserializationError::AvroDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        message_bytes,
                        format!(
                            "Unkown schema with fingerprint {}",
                            &message_bytes[2..10]
                                .iter()
                                .map(|byte| format!("{:02x}", byte))
                                .collect::<Vec<String>>()
                                .join("")
                        ),
                    ),
                })?;
        let mut reader = Cursor::new(message_bytes);

        match decoder.read_value(&mut reader) {
            Ok(drs) => match Value::try_from(drs) {
                Ok(v) => Ok(v),
                Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        message_bytes,
                        e.to_string(),
                    ),
                }),
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
impl MessageDeserializer for AvroDeserializer {
    async fn deserialize(
        &mut self,
        message_bytes: &[u8],
    ) -> Result<Value, MessageDeserializationError> {
        match self.decoder.decode_with_schema(Some(message_bytes)).await {
            Ok(drs) => match drs {
                Some(v) => match Value::try_from(v.value) {
                    Ok(v) => Ok(v),
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
    ) -> Result<Value, MessageDeserializationError> {
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
                        Ok(value) => Ok(value),
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
    ) -> Result<Value, MessageDeserializationError> {
        let decoder = self.decoder.borrow_mut();
        match decoder.decode(Some(message_bytes)).await {
            Ok(drs) => match drs {
                Some(v) => Ok(v.value),
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
                Err(e) => Err(anyhow::format_err!("{}", e)),
            },
            Err(e) => Err(anyhow::format_err!("{}", e)),
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

impl SoeAvroDeserializer {
    pub(crate) fn try_from_path(path: &PathBuf) -> Result<Self, anyhow::Error> {
        if path.is_file() {
            let (key, seo_reader) = Self::read_single_schema_file(path)?;
            debug!(
                "Loaded schema {:?} with key (i64 rep of fingerprint) {:?}",
                path, key
            );
            let map: DashMap<i64, GenericSingleObjectReader> = DashMap::with_capacity(1);
            map.insert(key, seo_reader);
            Ok(SoeAvroDeserializer { decoders: map })
        } else if path.is_dir() {
            let decoders = path
                .read_dir()?
                .map(|file| {
                    let file_path = file?.path();
                    let value = Self::read_single_schema_file(&file_path)?;
                    Ok(value)
                })
                .collect::<anyhow::Result<DashMap<_, _>>>()?;

            Ok(SoeAvroDeserializer { decoders })
        } else {
            Err(anyhow::format_err!("Path '{:?}' does not exists", path))
        }
    }

    fn read_single_schema_file(
        path: &PathBuf,
    ) -> Result<(i64, GenericSingleObjectReader), anyhow::Error> {
        match std::fs::read_to_string(path) {
            Ok(content) => match Schema::parse_str(&content) {
                Ok(s) => {
                    let fingerprint = s.fingerprint::<Rabin>().bytes;
                    let fingerprint = fingerprint
                        .try_into()
                        .expect("Rabin fingerprints are 8 bytes");
                    let key = Self::fingerprint_to_i64(fingerprint);
                    match GenericSingleObjectReader::new(s) {
                        Ok(decoder) => Ok((key, decoder)),
                        Err(e) => Err(anyhow::format_err!(
                            "Schema file '{:?}'; Error: {}",
                            path,
                            e
                        )),
                    }
                }
                Err(e) => Err(anyhow::format_err!(
                    "Schema file '{:?}'; Error: {}",
                    path,
                    e
                )),
            },
            Err(e) => Err(anyhow::format_err!(
                "Schema file '{:?}'; Error: {}",
                path,
                e
            )),
        }
    }

    fn extract_message_fingerprint(msg: &[u8]) -> Result<i64, anyhow::Error> {
        msg.get(2..10)
            .ok_or(anyhow::anyhow!(
                "Message does not contain a valid fingerprint"
            ))
            .map(|x| Self::fingerprint_to_i64(x.try_into().expect("Slice must be 8 bytes long")))
    }

    fn fingerprint_to_i64(msg: [u8; 8]) -> i64 {
        i64::from_le_bytes(msg)
    }
}

#[cfg(test)]
mod tests {}
