//! Binary used to run a kafka-delta-ingest command.
//!
//! # Summary
//! [`kafka_delta_ingest`] is a tool for writing data from a Kafka topic into a Delta Lake table.
//!
//! # Features
//!
//! * Apply simple transforms (using JMESPath queries or well known properties) to JSON before writing to Delta
//! * Write bad messages to a dead letter queue
//! * Send metrics to a Statsd endpoint
//! * Control the characteristics of output files written to Delta Lake by tuning buffer latency and file size parameters
//! * Automatically write Delta log checkpoints
//! * Passthrough of [librdkafka properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for additional Kafka configuration
//! * Start from explicit partition offsets.
//! * Write to any table URI supported by [`deltalake::storage::StorageBackend`]
//!
//! # Usage
//!
//! ```
//! // Start an ingest process to ingest data from `my-topic` into the Delta Lake table at `/my/table/uri`.
//! kafka-delta-ingest ingest my-topic /my/table/uri
//! ```
//!
//! ```
//! // List the available command line flags available for the `ingest` subcommand
//! kafka-delta-ingest ingest -h
//! ```

#![deny(warnings)]
#![deny(deprecated)]
#![deny(missing_docs)]

use chrono::Local;
use clap::{Arg, ArgAction, ArgGroup, ArgMatches, Command};
use kafka_delta_ingest::{
    start_ingest, AutoOffsetReset, DataTypeOffset, DataTypePartition, IngestOptions, MessageFormat,
    SchemaSource,
};
use log::{error, info, LevelFilter};
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::PathBuf;
use std::str::FromStr;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "sentry-ext")]
    {
        let _guard = std::env::var("SENTRY_DSN").ok().map(|dsn| {
            sentry::init((
                dsn,
                sentry::ClientOptions {
                    release: sentry::release_name!(),
                    ..Default::default()
                },
            ))
        });
    }

    let matches = build_app().get_matches();

    match matches.subcommand() {
        Some(("ingest", ingest_matches)) => {
            let app_id = ingest_matches
                .get_one::<String>("app_id")
                .unwrap()
                .to_string();

            init_logger(app_id.clone());

            let topic = ingest_matches
                .get_one::<String>("topic")
                .unwrap()
                .to_string();
            let table_location = ingest_matches
                .get_one::<String>("table_location")
                .unwrap()
                .to_string();

            let kafka_brokers = ingest_matches
                .get_one::<String>("kafka")
                .unwrap()
                .to_string();
            let consumer_group_id = ingest_matches
                .get_one::<String>("consumer_group")
                .unwrap()
                .to_string();

            let seek_offsets = ingest_matches
                .get_one::<String>("seek_offsets")
                .map(|s| parse_seek_offsets(s));

            let auto_offset_reset = ingest_matches
                .get_one::<String>("auto_offset_reset")
                .unwrap()
                .to_string();

            let auto_offset_reset: AutoOffsetReset = match &auto_offset_reset as &str {
                "earliest" => AutoOffsetReset::Earliest,
                "latest" => AutoOffsetReset::Latest,
                unknown => panic!("Unknown auto_offset_reset {}", unknown),
            };

            let allowed_latency = ingest_matches.get_one::<u64>("allowed_latency").unwrap();
            let max_messages_per_batch = ingest_matches
                .get_one::<usize>("max_messages_per_batch")
                .unwrap();
            let min_bytes_per_file = ingest_matches
                .get_one::<usize>("min_bytes_per_file")
                .unwrap();

            let transforms: HashMap<String, String> = ingest_matches
                .get_many::<String>("transform")
                .expect("Failed to parse transforms")
                .map(|t| parse_transform(t).unwrap())
                .collect();

            let dlq_table_location = ingest_matches
                .get_one::<String>("dlq_table_location")
                .map(|s| s.to_string());

            let dlq_transforms: HashMap<String, String> = ingest_matches
                .get_many::<String>("dlq_transform")
                .map(|dlq| dlq.map(|t| parse_transform(t).unwrap()).collect())
                .unwrap_or(HashMap::new());

            let write_checkpoints = ingest_matches.get_flag("checkpoints");

            let additional_kafka_settings = ingest_matches
                .get_many::<String>("kafka_setting")
                .map(|k| k.map(|s| parse_kafka_property(s).unwrap()).collect());

            let statsd_endpoint = ingest_matches
                .get_one::<String>("statsd_endpoint")
                .unwrap()
                .to_string();

            let end_at_last_offsets = ingest_matches
                .contains_id("end");

            let format = convert_matches_to_message_format(ingest_matches).unwrap();

            let options = IngestOptions {
                kafka_brokers,
                consumer_group_id,
                app_id,
                seek_offsets,
                auto_offset_reset,
                allowed_latency: *allowed_latency,
                max_messages_per_batch: *max_messages_per_batch,
                min_bytes_per_file: *min_bytes_per_file,
                transforms,
                dlq_table_uri: dlq_table_location,
                dlq_transforms,
                write_checkpoints,
                additional_kafka_settings,
                statsd_endpoint,
                input_format: format,
                end_at_last_offsets,
            };

            tokio::spawn(async move {
                let run = start_ingest(
                    topic,
                    table_location,
                    options,
                    std::sync::Arc::new(tokio_util::sync::CancellationToken::new()),
                )
                .await;
                match &run {
                    Ok(_) => info!("Ingest service exited gracefully"),
                    Err(e) => error!("Ingest service exited with error {:?}", e),
                }
                run
            })
            .await
            .unwrap()
            .unwrap();
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn to_schema_source(
    input: Option<&String>,
    disable_files: bool,
) -> Result<SchemaSource, SchemaSourceError> {
    match input {
        None => Ok(SchemaSource::None),
        Some(value) => {
            if value.is_empty() {
                return Ok(SchemaSource::None);
            }

            if !value.starts_with("http") {
                if disable_files {
                    return Ok(SchemaSource::None);
                }

                let p = PathBuf::from_str(value)?;
                if !p.exists() {
                    return Err(SchemaSourceError::FileNotFound {
                        file_name: (*value).clone(),
                    });
                }

                return Ok(SchemaSource::File(p));
            }

            Ok(SchemaSource::SchemaRegistry(url::Url::parse(value)?))
        }
    }
}

fn init_logger(app_id: String) {
    let app_id: &'static str = Box::leak(app_id.into_boxed_str());
    let log_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|l| LevelFilter::from_str(l.as_str()).ok())
        .unwrap_or(log::LevelFilter::Info);

    let _ = env_logger::Builder::new()
        .format(move |buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}: {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                app_id,
                record.args(),
            )
        })
        .filter(None, log_level)
        .try_init();
}

#[derive(thiserror::Error, Debug)]
#[error("'{value}' - Each transform argument must be colon delimited and match the pattern 'PROPERTY: SOURCE'")]
struct TransformSyntaxError {
    value: String,
}

#[derive(thiserror::Error, Debug)]
#[error("'{value}' - Each Kafka setting must be delimited by an '=' and match the pattern 'PROPERTY_NAME=PROPERTY_VALUE'")]
struct KafkaPropertySyntaxError {
    value: String,
}

/// Errors returned by [`to_schema_source`] function.
#[derive(thiserror::Error, Debug)]
enum SchemaSourceError {
    /// Wrapped [`core::convert::Infallible`]
    #[error("Invalid file path: {source}")]
    InvalidPath {
        #[from]
        source: core::convert::Infallible,
    },
    /// Wrapped [`url::ParseError`]
    #[error("Invalid url: {source}")]
    InvalidUrl {
        #[from]
        source: url::ParseError,
    },
    #[error("File not found error: {file_name}")]
    FileNotFound { file_name: String },
}

fn parse_kafka_property(val: &str) -> Result<(String, String), KafkaPropertySyntaxError> {
    parse_tuple(val, "=").map_err(|s| KafkaPropertySyntaxError { value: s })
}

fn parse_transform(val: &str) -> Result<(String, String), TransformSyntaxError> {
    parse_tuple(val, ":").map_err(|s| TransformSyntaxError { value: s })
}

// parse argument as a duple and let clap format the error in case of invalid syntax.
// this function is used both as a validator in the clap config, and to extract the program
// arguments.
fn parse_tuple(val: &str, delimiter: &str) -> Result<(String, String), String> {
    let splits: Vec<&str> = val.splitn(2, delimiter).map(|s| s.trim()).collect();

    match splits.len() {
        2 => {
            let tuple: (String, String) = (splits[0].to_owned(), splits[1].to_owned());
            Ok(tuple)
        }
        _ => Err(val.to_string()),
    }
}

fn parse_seek_offsets(val: &str) -> Vec<(DataTypePartition, DataTypeOffset)> {
    let map: HashMap<String, DataTypeOffset> =
        serde_json::from_str(val).expect("Cannot parse seek offsets");

    let mut list: Vec<(DataTypePartition, DataTypeOffset)> = map
        .iter()
        .map(|(p, o)| (p.parse::<DataTypePartition>().unwrap(), *o))
        .collect();

    list.sort_by(|a, b| a.0.cmp(&b.0));
    list
}

fn build_app() -> Command {
    Command::new("kafka-delta-ingest")
                    .version(env!["CARGO_PKG_VERSION"])
                    .about("Daemon for ingesting messages from Kafka and writing them to a Delta table")
                    .subcommand(
                        Command::new("ingest")
                            .about("Starts a stream that consumes from a Kafka topic and writes to a Delta table")
                            .arg(Arg::new("topic")
                                 .help("The Kafka topic to stream from")
                                .required(true))
                            .arg(Arg::new("table_location")
                                 .help("The Delta table location to write out")
                                 .required(true))
                            .arg(Arg::new("kafka")
                                 .short('k')
                                 .long("kafka")
                                 .help("Kafka broker connection string to use")
                                 .default_value("localhost:9092"))
                            .arg(Arg::new("consumer_group")
                                 .short('g')
                                 .long("consumer_group")
                                 .help("Consumer group to use when subscribing to Kafka topics")
                                 .default_value("kafka_delta_ingest"))
                            .arg(Arg::new("app_id")
                                 .short('a')
                                 .long("app_id")
                                 .help("App ID to use when writing to Delta")
                                 .default_value("kafka_delta_ingest"))
                            .arg(Arg::new("seek_offsets")
                                 .long("seek_offsets")
                                 .help(r#"Only useful when offsets are not already stored in the delta table. A JSON string specifying the partition offset map as the starting point for ingestion. This is *seeking* rather than _starting_ offsets. The first ingested message would be (seek_offset + 1). Ex: {"0":123, "1":321}"#))
                            .arg(Arg::new("auto_offset_reset")
                                 .short('o')
                                 .long("auto_offset_reset")
                                 .help(r#"The default offset reset policy, which is either 'earliest' or 'latest'.
The configuration is applied when offsets are not found in delta table or not specified with 'seek_offsets'. This also overrides the kafka consumer's 'auto.offset.reset' config."#)
                                 .default_value("earliest"))
                            .arg(Arg::new("allowed_latency")
                                 .short('l')
                                 .long("allowed_latency")
                                 .help("The allowed latency (in seconds) from the time a message is consumed to when it should be written to Delta.")
                                 .default_value("300")
                                 .value_parser(clap::value_parser!(u64)))
                            .arg(Arg::new("max_messages_per_batch")
                                 .short('m')
                                 .long("max_messages_per_batch")
                                 .help("The maximum number of rows allowed in a parquet batch. This shoulid be the approximate number of bytes described by MIN_BYTES_PER_FILE")
                                 .default_value("5000")
                                 .value_parser(clap::value_parser!(usize)))
                            .arg(Arg::new("min_bytes_per_file")
                                 .short('b')
                                 .long("min_bytes_per_file")
                                 .help("The target minimum file size (in bytes) for each Delta file. File size may be smaller than this value if ALLOWED_LATENCY does not allow enough time to accumulate the specified number of bytes.")
                                 .default_value("134217728")
                                 .value_parser(clap::value_parser!(usize)))
                            .arg(Arg::new("transform")
                                 .short('t')
                                 .long("transform")
                                 .action(ArgAction::Append)
                                 .help(
r#"A list of transforms to apply to each Kafka message. Each transform should follow the pattern:
"PROPERTY: SOURCE". For example:

... -t 'modified_date: substr(modified,`0`,`10`)' 'kafka_offset: kafka.offset'

Valid values for SOURCE come in two flavors: (1) JMESPath query expressions and (2) well known
Kafka metadata properties. Both are demonstrated in the example above.

The first SOURCE extracts a substring from the "modified" property of the JSON value, skipping 0
characters and taking 10. the transform assigns the result to "modified_date" (the PROPERTY).
You can read about JMESPath syntax in https://jmespath.org/specification.html. In addition to the
built-in JMESPath functions, Kafka Delta Ingest adds the custom `substr` function.

The second SOURCE represents the well-known Kafka "offset" property. Kafka Delta Ingest supports
the following well-known Kafka metadata properties:

* kafka.offset
* kafka.partition
* kafka.topic
* kafka.timestamp
"#))
                            .arg(Arg::new("dlq_table_location")
                                 .long("dlq_table_location")
                                 .required(false)
                                 .help("Optional table to write unprocessable entities to"))
                            .arg(Arg::new("dlq_transform")
                                 .long("dlq_transform")
                                 .required(false)
                                 .action(ArgAction::Append)
                                 .help("Transforms to apply before writing unprocessable entities to the dlq_location"))
                            .arg(Arg::new("checkpoints")
                                 .short('c')
                                 .long("checkpoints")
                                 .action(ArgAction::SetTrue)
                                 .help("If set then kafka-delta-ingest will write checkpoints on every 10th commit"))
                            .arg(Arg::new("kafka_setting")
                                 .short('K')
                                 .long("kafka_setting")
                                 .action(ArgAction::Append)
                                 .help(r#"A list of additional settings to include when creating the Kafka consumer.

This can be used to provide TLS configuration as in:

... -K "security.protocol=SSL" "ssl.certificate.location=kafka.crt" "ssl.key.location=kafka.key""#))
                            .arg(Arg::new("statsd_endpoint")
                                 .short('s')
                                 .long("statsd_endpoint")
                                 .help("Statsd endpoint for sending stats")
                                 .default_value("localhost:8125"))
                            .arg(Arg::new("json")
                                    .required(false)
                                    .long("json")
                                    .help("Schema registry endpoint, local path, or empty string"))
                            .arg(Arg::new("avro")
                                    .long("avro")
                                    .required(false)
                                    .help("Schema registry endpoint, local path, or empty string"))
                            .group(ArgGroup::new("format")
                                    .args(["json", "avro"])
                                    .required(false))
                            .arg(Arg::new("end")
                                .long("ends_at_latest_offsets")
                                .required(false)
                                .num_args(0)
                                .help(""))
                        )
                .arg_required_else_help(true)
}

fn convert_matches_to_message_format(
    ingest_matches: &ArgMatches,
) -> Result<MessageFormat, SchemaSourceError> {
    if !ingest_matches.contains_id("format") {
        return Ok(MessageFormat::DefaultJson);
    }

    if ingest_matches.contains_id("avro") {
        return to_schema_source(ingest_matches.get_one::<String>("avro"), false)
            .map(MessageFormat::Avro);
    }

    return to_schema_source(ingest_matches.get_one::<String>("json"), true)
        .map(MessageFormat::Json);
}

#[cfg(test)]
mod test {
    use kafka_delta_ingest::{MessageFormat, SchemaSource};

    use crate::{
        build_app, convert_matches_to_message_format, parse_seek_offsets, SchemaSourceError,
    };

    const SCHEMA_REGISTRY_ADDRESS: &str = "http://localhost:8081";

    #[test]
    fn parse_seek_offsets_test() {
        let parsed = parse_seek_offsets(r#"{"0":10,"2":12,"1":13}"#);
        assert_eq!(parsed, vec![(0, 10), (1, 13), (2, 12)]);
    }

    #[test]
    fn get_json_argument() {
        let schema_registry_url: url::Url = url::Url::parse(SCHEMA_REGISTRY_ADDRESS).unwrap();
        assert!(matches!(
            get_subcommand_matches(vec!["--json", ""]).unwrap(),
            MessageFormat::Json(SchemaSource::None)
        ));
        assert!(matches!(
            get_subcommand_matches(vec!["--json", "test"]).unwrap(),
            MessageFormat::Json(SchemaSource::None)
        ));

        match get_subcommand_matches(vec!["--json", SCHEMA_REGISTRY_ADDRESS]).unwrap() {
            MessageFormat::Json(SchemaSource::SchemaRegistry(registry_url)) => {
                assert_eq!(registry_url, schema_registry_url);
            }
            _ => panic!("invalid message format"),
        }

        assert!(matches!(
            get_subcommand_matches(vec!["--json", "http::a//"]),
            Err(SchemaSourceError::InvalidUrl { .. })
        ));
    }

    #[test]
    fn get_avro_argument() {
        let schema_registry_url: url::Url = url::Url::parse(SCHEMA_REGISTRY_ADDRESS).unwrap();
        assert!(matches!(
            get_subcommand_matches(vec!["--avro", ""]).unwrap(),
            MessageFormat::Avro(SchemaSource::None)
        ));

        match get_subcommand_matches(vec!["--avro", "tests/data/default_schema.avro"]).unwrap() {
            MessageFormat::Avro(SchemaSource::File(file_name)) => {
                assert_eq!(
                    file_name.to_str().unwrap(),
                    "tests/data/default_schema.avro"
                );
            }
            _ => panic!("invalid message format"),
        }

        assert!(matches!(
            get_subcommand_matches(vec!["--avro", "this_file_does_not_exist"]),
            Err(SchemaSourceError::FileNotFound { .. })
        ));

        match get_subcommand_matches(vec!["--avro", SCHEMA_REGISTRY_ADDRESS]).unwrap() {
            MessageFormat::Avro(SchemaSource::SchemaRegistry(registry_url)) => {
                assert_eq!(registry_url, schema_registry_url)
                // assert_eq!(registry_url, SCHEMA_REGISTRY_ADDRESS);
            }
            _ => panic!("invalid message format"),
        }

        assert!(matches!(
            get_subcommand_matches(vec!["--avro", "http::a//"]),
            Err(SchemaSourceError::InvalidUrl { .. })
        ));
    }

    fn get_subcommand_matches(args: Vec<&str>) -> Result<MessageFormat, SchemaSourceError> {
        let base_args = vec![
            "app",
            "ingest",
            "web_requests",
            "./tests/data/web_requests",
            "-a",
            "test",
        ];
        let try_matches = build_app().try_get_matches_from([base_args, args].concat());
        let matches = try_matches.unwrap();
        let (_, subcommand) = matches.subcommand().unwrap();
        return convert_matches_to_message_format(subcommand);
    }
}
