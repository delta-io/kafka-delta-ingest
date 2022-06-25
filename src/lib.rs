//! Implementations supporting the kafka-delta-ingest daemon

//! ## Feature flags
//!
//! - `dynamic-linking`: Use the `dynamic-linking` feature of the `rdkafka` crate and link to the system's version of librdkafka instead of letting the `rdkafka` crate builds its own librdkafka.

#![deny(warnings)]
#![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate strum_macros;

#[cfg(test)]
extern crate serde_json;

use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;

mod coercions;
mod dead_letters;
mod delta_helpers;
mod errors;
mod kafka;
mod metrics;
mod offsets;
mod processing;
mod settings;
mod transforms;
mod util;
mod value_buffers;
pub mod writer;

pub use kafka::{AutoOffsetReset, DataTypeOffset, DataTypePartition};
pub use settings::IngestOptions;

use crate::{
    errors::IngestError, kafka::IngestConsumer, metrics::IngestMetrics, processing::Processor,
    transforms::Transformer, writer::DataWriter,
};

/// Starts an ingestion stream which consumes from the Kafka topic and periodically writes to the
/// delta table.
pub async fn start_ingest(
    topic: String,
    table_uri: String,
    opts: IngestOptions,
    cancellation_token: Arc<CancellationToken>,
) -> Result<(), IngestError> {
    log::info!(
        "Ingesting messages from kafka topic `{}` to delta table `{}`",
        topic,
        table_uri
    );
    log::info!("Using options: [allowed_latency={},max_messages_per_batch={},min_bytes_per_file={},write_checkpoints={}]",
           opts.allowed_latency,
           opts.max_messages_per_batch,
           opts.min_bytes_per_file,
           opts.write_checkpoints);

    let table = delta_helpers::load_table(&table_uri, HashMap::new()).await?;
    let writer = DataWriter::for_table(&table, HashMap::new())?;
    let dlq = dead_letters::dlq_from_opts(&opts).await?;
    let consumer = IngestConsumer::new(kafka::config_from_options(&opts), &topic)?;
    let coercion_tree = coercions::create_coercion_tree(&table.get_metadata()?.schema);
    let transformer = Transformer::from_transforms(&opts.transforms)?;
    let metrics = IngestMetrics::new(&opts.statsd_endpoint)?;

    let mut processor = Processor::new(
        topic,
        consumer,
        table,
        writer,
        dlq,
        transformer,
        coercion_tree,
        metrics,
        cancellation_token,
        &opts,
    );

    let result = processor.start().await;

    if let Err(e) = result {
        log::error!("Processor failed: {:?}", e);
        return Err(e.into());
    }

    Ok(())
}
