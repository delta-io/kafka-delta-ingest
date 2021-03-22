#[macro_use]
extern crate clap;

use clap::{AppSettings, Values};
use kafka_delta_ingest::KafkaJsonToDelta;
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
            (@arg KAFKA_BROKERS: -k --kafka +required +takes_value default_value("localhost:9092") "The Kafka broker connection string to use when connecting to Kafka.")
            (@arg CONSUMER_GROUP: -g --consumer_group_id +takes_value default_value("kafka_delta_ingest") "The consumer group id to use when subscribing to Kafka.")
            (@arg ALLOWED_LATENCY: -l --allowed_latency +takes_value default_value("300") "The allowed latency (in seconds) from the time a message is consumed to when it should be written to Delta.")
            (@arg MAX_MESSAGES_PER_BATCH: -m --max_messages_per_batch +takes_value default_value("5000") "The maximum number of rows allowed in a parquet row group. This should approximate the number of bytes described by MIN_BYTES_PER_FILE.")
            (@arg MIN_BYTES_PER_FILE: -b --min_bytes_per_file +takes_value default_value("134217728") "The target minimum file size (in bytes) for each Delta file. File size may be smaller than this value if ALLOWED_LATENCY does not allow enough time to accumulate the specified number of bytes.")
            (@arg TRANSFORM: -t --transform +multiple +takes_value validator(parse_transform) r#"A list of transforms to apply to each Kafka message. 
Each transform should follow the pattern: "PROPERTY:SOURCE". For example: 

... -t 'modified_date:substr(modified,`0`,`10`)' 'kafka_offset:kafka.offset'

Valid values for SOURCE come in two flavors: (1) JMESPath query expressions and (2) well known Kafka metadata properties. Both are demonstrated in the example above.

The first SOURCE extracts a substring from the "modified" property of the JSON value, skipping 0 characters and taking 10. the transform assigns the result to "modified_date" (the PROPERTY).
You can read about JMESPath syntax in https://jmespath.org/specification.html. In addition to the built-in JMESPath functions, Kafka Delta Ingest adds the custom `substr` function.

The second SOURCE represents the well-known Kafka "offset" property. Kafka Delta Ingest supports the following well-known Kafka metadata properties:

* kafka.offset
* kafka.partition
* kafka.topic
* kafka.timestamp
"#)
        )
    )
    .setting(AppSettings::SubcommandRequiredElseHelp)
    .setting(AppSettings::VersionlessSubcommands)
    .get_matches();

    match matches.subcommand() {
        Some(("ingest", ingest_matches)) => {
            let topic = ingest_matches.value_of("TOPIC").unwrap();
            let table_location = ingest_matches.value_of("TABLE_LOCATION").unwrap();
            let kafka_brokers = ingest_matches.value_of("KAFKA_BROKERS").unwrap();
            let consumer_group_id = ingest_matches.value_of("CONSUMER_GROUP").unwrap();

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
                .unwrap_or_else(|| vec![]);
            let transforms: HashMap<String, String> = transforms
                .iter()
                .map(|t| parse_transform(t).unwrap())
                .collect();

            let mut stream = KafkaJsonToDelta::new(
                topic.to_string(),
                table_location.to_string(),
                kafka_brokers.to_string(),
                consumer_group_id.to_string(),
                // TODO: Collect and pass additional kafka settings (SSL, etc)
                None,
                allowed_latency,
                max_messages_per_batch,
                min_bytes_per_file,
                transforms,
            );

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
#[error("'{value}' - Each transform argument must be colon delimited and match the pattern 'PROPERTY_TO_ASSIGN: TRANSFORM'")]
struct InvalidTransformSyntaxError {
    value: String,
}

// parse each transform argument and let clap format the error in case of invalid syntax.
// this function is used both as a validator in the clap config, and to extract the program
// arguments.
fn parse_transform(val: &str) -> Result<(String, String), InvalidTransformSyntaxError> {
    let splits: Vec<&str> = val.splitn(2, ':').map(|s| s.trim()).collect();

    match splits.len() {
        2 => {
            let tuple: (String, String) = (splits[0].to_owned(), splits[1].to_owned());
            Ok(tuple)
        }
        _ => Err(InvalidTransformSyntaxError {
            value: val.to_string(),
        }),
    }
}
