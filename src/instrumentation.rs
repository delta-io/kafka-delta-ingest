use async_trait::async_trait;
use deltalake::{DeltaDataTypeVersion, DeltaTableError};
use dipstick::{Input, InputScope, Prefixed, Statsd, StatsdScope};
use log::{debug, error, info, warn};
use parquet::errors::ParquetError;
use rdkafka::message::{BorrowedMessage, Message};
use std::convert::TryInto;
use std::time::Instant;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};

use crate::transforms::TransformError;

/// A tuple where the first element is a variant of [StatTypes] and the second element is the value to record for the statistic.
pub(crate) type Statistic = (StatTypes, i64);

/// Initializes a channel for sending statistics to statsd.
pub(crate) fn init_stats(
    endpoint: &str,
    app_id: &str,
) -> Result<Sender<Statistic>, std::io::Error> {
    let scope = Statsd::send_to(endpoint)?.named(app_id).metrics();

    let mut handler = StatsHandler::new(scope);

    let sender = handler.tx.clone();

    task::spawn(async move {
        handler.run_loop().await;
    });

    Ok(sender)
}

/// Trait that exposes helper methods for writing logs and metrics.
#[async_trait]
pub(crate) trait Instrumentation {
    // messages

    /// Records a Kafka message has been deserialized.
    async fn log_message_deserialized(&self, m: &BorrowedMessage) {
        debug_message("Message deserialized", m);
        self.record_stat((StatTypes::MessageDeserialized, 1)).await;
    }

    /// Records a failure when deserializing a Kafka message.
    async fn log_message_deserialization_failed(&self, m: &BorrowedMessage, e: &serde_json::Error) {
        error_message(
            "Message deserialization failed",
            m,
            e as &dyn std::error::Error,
        );
        self.record_stat((StatTypes::MessageDeserializationFailed, 1))
            .await;
    }

    /// Records the size of the last message received from Kafka.
    async fn log_message_bytes(&self, bytes: usize) {
        self.record_stat((StatTypes::MessageSize, bytes as i64))
            .await;
    }

    /// Records that a Kafka message has been transformed.
    async fn log_message_transformed(&self, m: &BorrowedMessage) {
        debug_message("Message transformed", m);
        self.record_stat((StatTypes::MessageTransformed, 1)).await;
    }

    /// Records that transforming a Kafka message failed.
    async fn log_message_transform_failed(&self, m: &BorrowedMessage, e: &TransformError) {
        error_message("Message transformed failed", m, e as &dyn std::error::Error);
        self.record_stat((StatTypes::MessageTransformFailed, 1))
            .await;
    }

    /// Records that a set of buffered messages was only _partially_ written due to a Parquet error.
    async fn log_partial_parquet_write(
        &self,
        skipped: usize,
        parquet_error: Option<&ParquetError>,
    ) {
        warn!(
            "Partial Parquet Write, skipped {}. ParquetError {:?}",
            skipped, parquet_error
        );
    }

    // record batches

    /// Records that an Arrow RecordBatch has been started from buffered messages.
    async fn log_record_batch_started(&self) {
        debug!("Record batch started");
        self.record_stat((StatTypes::RecordBatchStarted, 1)).await;
    }

    /// Records that an Arrow RecordBatch has been created from buffered messages.
    async fn log_record_batch_completed(
        &self,
        buffered_record_batch_count: usize,
        timer: &Instant,
    ) {
        let duration = timer.elapsed().as_millis() as i64;
        debug!("Record batch completed in {} millis", duration);
        self.record_stat((StatTypes::RecordBatchCompleted, 1)).await;
        self.record_stat((
            StatTypes::BufferedRecordBatches,
            buffered_record_batch_count as i64,
        ))
        .await;
        self.record_stat((StatTypes::RecordBatchWriteDuration, duration))
            .await;
    }

    // delta writes

    /// Records that a delta write has started.
    async fn log_delta_write_started(&self) {
        debug!("Delta write started");
        self.record_stat((StatTypes::DeltaWriteStarted, 1)).await;
    }

    /// Records that a delta write has completed.
    async fn log_delta_write_completed(&self, version: DeltaDataTypeVersion, timer: &Instant) {
        let duration = timer.elapsed().as_millis() as i64;
        info!(
            "Delta write for version {} has completed in {} millis",
            version, duration
        );
        self.record_stat((StatTypes::DeltaWriteCompleted, 1)).await;
        self.record_stat((StatTypes::DeltaWriteDuration, duration))
            .await;
    }

    /// Records that a delta write has failed.
    async fn log_delta_write_failed(&self, e: &DeltaTableError) {
        error!("Delta write failed {}", e);
        self.record_stat((StatTypes::DeltaWriteFailed, 1)).await;
    }

    /// Records the size of a file added to Delta.
    async fn log_delta_add_file_size(&self, size: i64) {
        self.record_stat((StatTypes::DeltaAddFileSize, size)).await
    }

    // delta txn

    /// Records the last txn version for the app id in the delta log.
    async fn log_delta_tx_version_found(&self, app_id: &str, txn_version: i64) {
        info!("Read tx version of {} for app {}", txn_version, app_id);
    }

    /// Records that a delta txn was not found for the app id.
    async fn log_delta_tx_version_not_found(&self, app_id: &str) {
        info!(
            "Delta table does not contain a txn for app id {}. Starting write-ahead-log from 1.",
            app_id
        );
    }

    // control plane

    /// Records that the ingest stream has been cancelled and will terminate.
    async fn log_stream_cancelled(&self, m: &BorrowedMessage) {
        info_message("Found cancellation token set. Stopping run loop.", m);
    }

    // helpers

    /// Records a statistic.
    async fn record_stat(&self, statistic: Statistic) {
        let _ = self.stats_sender().send(statistic).await;
    }

    /// Returns the stat sender channel.
    fn stats_sender(&self) -> Sender<Statistic>;
}

pub(crate) struct StatsHandler {
    metrics: StatsdScope,
    rx: Receiver<Statistic>,
    pub tx: Sender<Statistic>,
}

impl StatsHandler {
    pub(crate) fn new(metrics: StatsdScope) -> StatsHandler {
        let (tx, rx) = channel(1_000_000);

        StatsHandler { metrics, rx, tx }
    }

    pub(crate) async fn run_loop(&mut self) {
        loop {
            if let Some((stat, val)) = self.rx.recv().await {
                match stat {
                    // timers
                    StatTypes::RecordBatchWriteDuration | StatTypes::DeltaWriteDuration => {
                        self.handle_timer(stat, val);
                    }

                    // gauges
                    StatTypes::BufferedRecordBatches
                    | StatTypes::MessageSize
                    | StatTypes::DeltaAddFileSize
                    | StatType::BufferNumPartitions
                    | StatType::BufferLagTotal
                    | StatType::BufferLagMax
                    | StatType::BufferLagMin
                    | StatType::DeltaWriteNumPartitions
                    | StatType::DeltaWriteLagTotal
                    | StatType::DeltaWriteLagMax
                    | StatType::DeltaWriteLagMin => {
                        self.handle_gauge(stat, val);
                    }

                    // counters
                    _ => {
                        self.handle_counter(stat, val);
                    }
                }
            }
        }
    }

    fn handle_timer(&self, stat: StatTypes, duration_us: i64) {
        let stat_string = stat.to_string();

        if let Ok(duration) = duration_us.try_into() {
            self.metrics
                .timer(stat_string.as_str())
                .interval_us(duration);
        } else {
            error!("Failed to report timer to statsd with an i64 that couldn't fit into u64.");
        }
    }

    fn handle_gauge(&self, stat: StatTypes, count: i64) {
        let stat_string = stat.to_string();
        let key = stat_string.as_str();

        self.metrics.gauge(key).value(count);
    }

    fn handle_counter(&self, stat: StatTypes, count: i64) {
        let stat_string = stat.to_string();
        let key = stat_string.as_str();

        let sized_count: usize = count.try_into().expect("Could not convert to usize");

        self.metrics.counter(key).count(sized_count);
    }
}

fn debug_message(description: &str, m: &BorrowedMessage) {
    debug!(
        "{} - partition {} offset {}",
        description,
        m.partition(),
        m.offset()
    );
}

fn info_message(description: &str, m: &BorrowedMessage) {
    info!(
        "{} - partition {} offset {}",
        description,
        m.partition(),
        m.offset()
    );
}

fn error_message(description: &str, m: &BorrowedMessage, e: &dyn std::error::Error) {
    error!(
        "{} - partition {} offset {}, error: {}",
        description,
        m.partition(),
        m.offset(),
        e
    );
}

/// Statistic types handled by [crate::instrumentation].
#[derive(Debug, Display, Hash, PartialEq, Eq)]
pub(crate) enum StatTypes {
    //
    // counters
    //
    /// Counter for a deserialized message.
    #[strum(serialize = "messages.deserialization.completed")]
    MessageDeserialized,
    /// Counter for a message that failed deserialization.
    #[strum(serialize = "messages.deserialization.failed")]
    MessageDeserializationFailed,
    /// Counter for a transformed message.
    #[strum(serialize = "messages.transform.completed")]
    MessageTransformed,
    /// Counter for a message that failed transformation.
    #[strum(serialize = "messages.transform.failed")]
    MessageTransformFailed,
    /// Counter for when a record batch is started.
    #[strum(serialize = "recordbatch.started")]
    RecordBatchStarted,
    /// Counter for when a record batch is completed.
    #[strum(serialize = "recordbatch.completed")]
    RecordBatchCompleted,
    /// Counter for when a delta write is started.
    #[strum(serialize = "delta.write.started")]
    DeltaWriteStarted,
    /// Counter for when a delta write is completed.
    #[strum(serialize = "delta.write.completed")]
    DeltaWriteCompleted,
    /// Counter for failed delta writes.
    #[strum(serialize = "delta.write.failed")]
    DeltaWriteFailed,

    //
    // timers
    //
    /// Timer for record batch write duration.
    #[strum(serialize = "recordbatch.write_duration")]
    RecordBatchWriteDuration,
    /// Timer for delta write duration.
    #[strum(serialize = "delta.write.duration")]
    DeltaWriteDuration,

    //
    // gauges
    //
    /// Gauge for number of Arrow record batches in buffer.
    #[strum(serialize = "buffered.record_batches")]
    BufferedRecordBatches,
    /// Gauge for message size.
    #[strum(serialize = "messages.size")]
    MessageSize,
    /// Gauge for Delta add file size.
    #[strum(serialize = "delta.add.size")]
    DeltaAddFileSize,
    /// Gauge for the number of partitions in buffer.
    #[strum(serialize = "buffer.lag.num_partitions")]
    BufferNumPartitions,
    /// Gauge for total buffer lag across all partitions.
    #[strum(serialize = "buffer.lag.total")]
    BufferLagTotal,
    /// Gauge for max buffer lag across all partitions.
    #[strum(serialize = "buffer.lag.max")]
    BufferLagMax,
    /// Gauge for min buffer lag across all partitions.
    #[strum(serialize = "buffer.lag.min")]
    BufferLagMin,
    /// Gauge for the number of partitions in the last delta write.
    #[strum(serialize = "delta.write.lag.num_partitions")]
    DeltaWriteNumPartitions,
    /// Gauge for total delta write lag across all partitions.
    #[strum(serialize = "delta.write.lag.total")]
    DeltaWriteLagTotal,
    /// Gauge for max delta write lag across all partitions.
    #[strum(serialize = "delta.write.lag.max")]
    DeltaWriteLagMax,
    /// Gauge for min delta write lag across all partitions.
    #[strum(serialize = "delta.write.lag.min")]
    DeltaWriteLagMin,
}
