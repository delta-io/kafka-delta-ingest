#![deny(warnings)]

#[macro_use]
extern crate clap;

use clap::{AppSettings, Values};
use kafka_delta_ingest::{instrumentation, KafkaJsonToDelta, Options};
use log::{error, info};
use std::collections::HashMap;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let matches = clap_app!(kafka_delta_ingest =>
        (version: env!("CARGO_PKG_VERSION"))
        (about: "Service for ingesting messages from a Kafka topic and writing them to a Delta table")
        (@subcommand ingest =>
            (about: "Starts a stream that consumes from a Kafka topic and and writes to a Delta table")
            (@arg TOPIC: +required "The Kafka topic to stream from")
            (@arg TABLE_LOCATION: +required "The Delta table location to write to")

            (@arg DLQ_TABLE_LOCATION: --dlq_table_location +takes_value "Optional table to write dead letters to")

            (@arg KAFKA_BROKERS: -k --kafka +takes_value default_value("localhost:9092") 
             "The Kafka broker connection string to use when connecting to Kafka.")
            (@arg CONSUMER_GROUP: -g --consumer_group_id +takes_value default_value("kafka_delta_ingest") 
             "The consumer group id to use when subscribing to Kafka.")

            (@arg ADDITIONAL_KAFKA_SETTINGS: -K --Kafka +multiple_occurrences +multiple_values +takes_value validator(parse_kafka_property)
            r#"A list of additional settings to include when creating the Kafka consumer.

            Each additional setting should follow the pattern: "PROPERTY_NAME=PROPERTY_VALUE". For example:

            ... -K "auto.offset.reset=earliest"

            This can be used to provide TLS configuration as in:

            ... -K "security.protocol=SSL" "ssl.certificate.location=kafka.crt" "ssl.key.location=kafka.key"

             "#)

            (@arg APP_ID: -a --app_id +takes_value default_value("kafka_delta_ingest") 
             "The app id to use when writing to Delta.")

            (@arg ALLOWED_LATENCY: -l --allowed_latency +takes_value default_value("300") 
             "The allowed latency (in seconds) from the time a message is consumed to when it should be written to Delta.")
            (@arg MAX_MESSAGES_PER_BATCH: -m --max_messages_per_batch +takes_value default_value("5000") 
             "The maximum number of rows allowed in a parquet row group. This should approximate the number of bytes described by MIN_BYTES_PER_FILE.")
            (@arg MIN_BYTES_PER_FILE: -b --min_bytes_per_file +takes_value default_value("134217728") 
             "The target minimum file size (in bytes) for each Delta file. File size may be smaller than this value if ALLOWED_LATENCY does not allow enough time to accumulate the specified number of bytes.")

            (@arg TRANSFORM: -t --transform +multiple_occurrences +multiple_values +takes_value validator(parse_transform)
            r#"A list of transforms to apply to each Kafka message. 
Each transform should follow the pattern: "PROPERTY: SOURCE". For example: 

... -t 'modified_date: substr(modified,`0`,`10`)' 'kafka_offset: kafka.offset'

Valid values for SOURCE come in two flavors: (1) JMESPath query expressions and (2) well known Kafka metadata properties. Both are demonstrated in the example above.

The first SOURCE extracts a substring from the "modified" property of the JSON value, skipping 0 characters and taking 10. the transform assigns the result to "modified_date" (the PROPERTY).
You can read about JMESPath syntax in https://jmespath.org/specification.html. In addition to the built-in JMESPath functions, Kafka Delta Ingest adds the custom `substr` function.

The second SOURCE represents the well-known Kafka "offset" property. Kafka Delta Ingest supports the following well-known Kafka metadata properties:

* kafka.offset
* kafka.partition
* kafka.topic
* kafka.timestamp
"#)

            (@arg DLQ_TRANSFORM: --dlq_transform +multiple_occurrences +multiple_values +takes_value validator(parse_transform) "Transforms to apply before writing dead letters to delta")

            (@arg STATSD_ENDPOINT: -s --statsd_endpoint +takes_value
             "The statsd endpoint to send statistics to.")

            (@arg CHECKPOINTS: -c --checkpoints
            "If set then Kafka Delta ingest will write checkpoints on each 10th commit.")
        )
    )
    .setting(AppSettings::SubcommandRequiredElseHelp)
    .setting(AppSettings::DisableVersionForSubcommands)
    .get_matches();

    match matches.subcommand() {
        Some(("ingest", ingest_matches)) => {
            let topic = ingest_matches.value_of("TOPIC").unwrap();
            let table_location = ingest_matches.value_of("TABLE_LOCATION").unwrap();

            let dlq_table_location = ingest_matches.value_of("DLQ_TABLE_LOCATION");

            let kafka_brokers = ingest_matches.value_of("KAFKA_BROKERS").unwrap();
            let consumer_group_id = ingest_matches.value_of("CONSUMER_GROUP").unwrap();

            let additional_kafka_properties = ingest_matches
                .values_of("ADDITIONAL_KAFKA_SETTINGS")
                .map(Values::collect)
                .unwrap_or_else(Vec::new);
            let additional_kafka_settings: HashMap<String, String> = additional_kafka_properties
                .iter()
                .map(|p| parse_kafka_property(p).unwrap())
                .collect();

            let app_id = ingest_matches.value_of("APP_ID").unwrap();

            let allowed_latency = ingest_matches.value_of_t::<u64>("ALLOWED_LATENCY").unwrap();
            let max_messages_per_batch = ingest_matches
                .value_of_t::<usize>("MAX_MESSAGES_PER_BATCH")
                .unwrap();
            let min_bytes_per_file = ingest_matches
                .value_of_t::<usize>("MIN_BYTES_PER_FILE")
                .unwrap();

            let transforms: Vec<&str> = ingest_matches
                .values_of("TRANSFORM")
                .map(Values::collect)
                .unwrap_or_else(Vec::new);
            let transforms: HashMap<String, String> = transforms
                .iter()
                .map(|t| parse_transform(t).unwrap())
                .collect();

            let dlq_transforms: Vec<&str> = ingest_matches
                .values_of("DLQ_TRANSFORM")
                .map(Values::collect)
                .unwrap_or_else(Vec::new);
            let dlq_transforms: HashMap<String, String> = dlq_transforms
                .iter()
                .map(|t| parse_transform(t).unwrap())
                .collect();

            let stats_endpoint = ingest_matches
                .value_of("STATSD_ENDPOINT")
                .unwrap_or("localhost:8125");
            let stats_sender = instrumentation::init_stats(stats_endpoint, app_id)?;

            let write_checkpoints = ingest_matches.is_present("CHECKPOINTS");

            let options = Options::new(
                topic.to_string(),
                table_location.to_string(),
                dlq_table_location.map(|s| s.to_owned()),
                dlq_transforms,
                app_id.to_string(),
                allowed_latency,
                max_messages_per_batch,
                min_bytes_per_file,
                write_checkpoints,
            );

            let mut stream = KafkaJsonToDelta::new(
                options,
                kafka_brokers.to_string(),
                consumer_group_id.to_string(),
                Some(additional_kafka_settings),
                transforms,
                stats_sender,
            )?;

            let _ = tokio::spawn(async move {
                match stream.start(None).await {
                    Ok(_) => info!("Stream exited gracefully"),
                    Err(e) => error!("Stream exited with error {:?}", e),
                }
            })
            .await;
        }
        _ => unreachable!(),
    }

    Ok(())
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
