use async_trait::async_trait;
use deltalake::DeltaDataTypeVersion;
use dipstick::{Input, InputScope, Prefixed, Statsd, StatsdScope};
use log::{debug, error, info, warn};
use rdkafka::message::{BorrowedMessage, Message};
use std::convert::TryInto;
use std::time::Instant;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};

pub type Statistic = (StatTypes, i64);

pub fn init_stats(endpoint: &str, app_id: &str) -> Result<Sender<Statistic>, std::io::Error> {
    let scope = Statsd::send_to(endpoint)?.named(app_id).metrics();

    let mut handler = StatsHandler::new(scope);

    let sender = handler.tx.clone();

    task::spawn(async move {
        handler.run_loop().await;
    });

    Ok(sender)
}

#[async_trait]
pub trait Instrumentation {
    // messages

    async fn log_message_received(&self, m: &BorrowedMessage) {
        debug_message("Message received", m);
        self.record_stat((StatTypes::MessageReceived, 1)).await;
    }

    async fn log_message_deserialized(&self, m: &BorrowedMessage) {
        debug_message("Message deserialized", m);
        self.record_stat((StatTypes::MessageDeserialized, 1)).await;
    }

    async fn log_message_deserialization_failed(&self, m: &BorrowedMessage) {
        debug_message("Message deserialization failed", m);
        self.record_stat((StatTypes::MessageDeserializationFailed, 1))
            .await;
    }

    async fn log_message_bytes(&self, bytes: usize) {
        self.record_stat((StatTypes::MessageSize, bytes as i64))
            .await;
    }

    async fn log_message_transformed(&self, m: &BorrowedMessage) {
        debug_message("Message transformed", m);
        self.record_stat((StatTypes::MessageTransformed, 1)).await;
    }

    async fn log_message_transform_failed(&self, m: &BorrowedMessage) {
        warn_message("Message transformed failed", m);
        self.record_stat((StatTypes::MessageTransformFailed, 1))
            .await;
    }

    async fn log_message_buffered(&self, m: &BorrowedMessage, message_buffer_len: usize) {
        debug_message("Message buffered", m);
        self.record_stat((StatTypes::MessageBuffered, 1)).await;
        self.record_stat((StatTypes::BufferedMessages, message_buffer_len as i64))
            .await;
    }

    // assignments

    async fn log_assignment_untracked_skipped(&self, m: &BorrowedMessage) {
        info_message("Partition is not tracked. Resetting partition assignment and skipping message. Will process the same message again after consumer seek.", m);
    }
    // record batches

    async fn log_record_batch_started(&self) {
        debug!("Record batch started");
        self.record_stat((StatTypes::RecordBatchStarted, 1)).await;
    }

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

    async fn log_delta_write_started(&self) {
        debug!("Delta write started");
        self.record_stat((StatTypes::DeltaWriteStarted, 1)).await;
    }

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

    async fn log_delta_write_failed(&self) {
        warn!("Delta write failed");
        self.record_stat((StatTypes::DeltaWriteFailed, 1)).await;
    }

    async fn log_delta_add_file_size(&self, size: i64) {
        self.record_stat((StatTypes::DeltaAddFileSize, size)).await
    }

    // delta tx

    async fn log_delta_tx_version_found(&self, app_id: &str, txn_version: i64) {
        info!("Read tx version of {} for app {}", txn_version, app_id);
    }

    async fn log_delta_tx_version_not_found(&self, app_id: &str) {
        info!(
            "Delta table does not contain a txn for app id {}. Starting write-ahead-log from 1.",
            app_id
        );
    }

    // control plane

    async fn log_stream_cancelled(&self, m: &BorrowedMessage) {
        info_message("Found cancellation token set. Stopping run loop.", m);
    }

    // helpers

    async fn record_stat(&self, statistic: Statistic) {
        let _ = self.stats_sender().send(statistic).await;
    }

    fn stats_sender(&self) -> Sender<Statistic>;
}

pub struct StatsHandler {
    metrics: StatsdScope,
    rx: Receiver<Statistic>,
    pub tx: Sender<Statistic>,
}

impl StatsHandler {
    pub fn new(metrics: StatsdScope) -> StatsHandler {
        let (tx, rx) = channel(1_000_000);

        StatsHandler { metrics, rx, tx }
    }

    pub async fn run_loop(&mut self) {
        loop {
            if let Some((stat, val)) = self.rx.recv().await {
                match stat {
                    // timers
                    StatTypes::RecordBatchWriteDuration
                    | StatTypes::DeltaWriteDuration
                    | StatTypes::WriteAheadLogEntryDuration => {
                        self.handle_timer(stat, val);
                    }

                    // gauges
                    StatTypes::BufferedMessages
                    | StatTypes::BufferedRecordBatches
                    | StatTypes::MessageSize
                    | StatTypes::DeltaAddFileSize => {
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

fn warn_message(description: &str, m: &BorrowedMessage) {
    warn!(
        "{} - partition {} offset {}",
        description,
        m.partition(),
        m.offset()
    );
}

#[derive(Debug, Display, Hash, PartialEq, Eq)]
pub enum StatTypes {
    // counters
    #[strum(serialize = "messages.received")]
    MessageReceived,
    #[strum(serialize = "messages.deserialization.completed")]
    MessageDeserialized,
    #[strum(serialize = "messages.deserialization.failed")]
    MessageDeserializationFailed,
    #[strum(serialize = "messages.transform.completed")]
    MessageTransformed,
    #[strum(serialize = "messages.transform.failed")]
    MessageTransformFailed,
    #[strum(serialize = "messages.buffered")]
    MessageBuffered,

    #[strum(serialize = "assignments.message_skipped")]
    AssignmentsMessageSkipped,

    #[strum(serialize = "recordbatch.started")]
    RecordBatchStarted,
    #[strum(serialize = "recordbatch.completed")]
    RecordBatchCompleted,

    #[strum(serialize = "wal.entry.prepared")]
    WriteAheadLogEntryPrepared,
    #[strum(serialize = "wal.entry.completed")]
    WriteAheadLogEntryCompleted,
    #[strum(serialize = "wal.entry.aborted")]
    WriteAheadLogEntryAborted,

    #[strum(serialize = "delta.write.started")]
    DeltaWriteStarted,
    #[strum(serialize = "delta.write.completed")]
    DeltaWriteCompleted,
    #[strum(serialize = "delta.write.failed")]
    DeltaWriteFailed,

    // timers
    #[strum(serialize = "recordbatch.write_duration")]
    RecordBatchWriteDuration,
    #[strum(serialize = "wal.entry.duration")]
    WriteAheadLogEntryDuration,
    #[strum(serialize = "delta.write.duration")]
    DeltaWriteDuration,

    // gauges
    #[strum(serialize = "buffered.messages")]
    BufferedMessages,
    #[strum(serialize = "buffered.record_batches")]
    BufferedRecordBatches,
    #[strum(serialize = "messages.size")]
    MessageSize,
    #[strum(serialize = "delta.add.size")]
    DeltaAddFileSize,
}
