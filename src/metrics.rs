use dipstick::*;
use log::error;
use std::convert::TryInto;
use std::time::Instant;

// TODO: This should be a command line parameter so we can adjust the queue size based on expected message rate.
const INPUT_QUEUE_SIZE: usize = 100;

/// Stat types for the various metrics reported by the application
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

/// Wraps a [`dipstick::queue::InputQueueScope`] to provide a higher level API for recording metrics.
#[derive(Clone)]
pub(crate) struct IngestMetrics {
    metrics: InputQueueScope,
}

impl IngestMetrics {
    pub(crate) fn new(endpoint: &str, app_id: &str) -> Self {
        let metrics = create_queue(endpoint, app_id);

        Self { metrics }
    }

    /// increments a counter for message deserialized
    pub fn message_deserialized(&self) {
        self.record_one(StatTypes::MessageDeserialized);
    }

    /// increments a counter for message deserialization failed
    pub fn message_deserialization_failed(&self) {
        self.record_one(StatTypes::MessageDeserializationFailed);
    }

    pub fn message_deserialized_size(&self, size: usize) {
        self.record_stat(StatTypes::MessageSize, size as i64);
    }

    /// increments a counter for message transformed
    pub fn message_transformed(&self) {
        self.record_one(StatTypes::MessageTransformed);
    }

    /// increments a counter for message transform failed
    pub fn message_transform_failed(&self) {
        self.record_one(StatTypes::MessageTransformFailed);
    }

    /// increments a counter for record batch started
    pub fn batch_started(&self) {
        self.record_one(StatTypes::RecordBatchStarted);
    }

    /// increments a counter for record batch completed.
    /// records a guage stat for buffered record batches.
    /// records a timer stat for record batch write duration.
    pub fn batch_completed(&self, buffered_record_batch_count: usize, timer: &Instant) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_one(StatTypes::RecordBatchCompleted);
        self.record_stat(
            StatTypes::BufferedRecordBatches,
            buffered_record_batch_count as i64,
        );
        self.record_stat(StatTypes::RecordBatchWriteDuration, duration);
    }

    /// increments a counter for delta write started
    pub fn delta_write_started(&self) {
        self.record_one(StatTypes::DeltaWriteStarted);
    }

    /// increments a counter for delta write started.
    /// records a timer stat for delta write duration.
    pub fn delta_write_completed(&self, timer: &Instant) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_one(StatTypes::DeltaWriteCompleted);
        self.record_stat(StatTypes::DeltaWriteDuration, duration);
    }

    /// increments a counter for delta write failed.
    pub fn delta_write_failed(&self) {
        self.record_one(StatTypes::DeltaWriteFailed);
    }

    /// records a guage for delta file size.
    pub fn delta_file_size(&self, size: i64) {
        self.record_stat(StatTypes::DeltaAddFileSize, size as i64);
    }

    pub fn buffer_lag(&self, buffer_lags: Vec<i64>) {
        let lag_metrics = self.calculate_lag_metrics(buffer_lags);

        self.record_stat(StatTypes::BufferNumPartitions, lag_metrics.num_partitions);
        self.record_stat(StatTypes::BufferLagTotal, lag_metrics.total);

        if let Some(max) = lag_metrics.max {
            self.record_stat(StatTypes::BufferLagMax, max);
        }

        if let Some(min) = lag_metrics.min {
            self.record_stat(StatTypes::BufferLagMin, min);
        }
    }

    pub fn delta_lag(&self, write_lags: Vec<i64>) {
        let lag_metrics = self.calculate_lag_metrics(write_lags);

        self.record_stat(
            StatTypes::DeltaWriteNumPartitions,
            lag_metrics.num_partitions,
        );
        self.record_stat(StatTypes::DeltaWriteLagTotal, lag_metrics.total);

        if let Some(max) = lag_metrics.max {
            self.record_stat(StatTypes::DeltaWriteLagMax, max);
        }

        if let Some(min) = lag_metrics.min {
            self.record_stat(StatTypes::DeltaWriteLagMin, min);
        }
    }

    fn calculate_lag_metrics(&self, lags: Vec<i64>) -> LagMetrics {
        let total: i64 = lags.iter().sum();
        let max = lags.iter().max().map(|n| *n);
        let min = lags.iter().min().map(|n| *n);
        let num_partitions = lags.len() as i64;

        LagMetrics {
            total,
            max,
            min,
            num_partitions,
        }
    }

    fn record_one(&self, stat_type: StatTypes) {
        self.record_stat(stat_type, 1);
    }

    fn record_stat(&self, stat_type: StatTypes, val: i64) {
        match stat_type {
            // timers
            StatTypes::RecordBatchWriteDuration | StatTypes::DeltaWriteDuration => {
                self.handle_timer(stat_type, val);
            }

            // gauges
            StatTypes::BufferedRecordBatches
            | StatTypes::MessageSize
            | StatTypes::DeltaAddFileSize
            | StatTypes::BufferNumPartitions
            | StatTypes::BufferLagTotal
            | StatTypes::BufferLagMax
            | StatTypes::BufferLagMin
            | StatTypes::DeltaWriteNumPartitions
            | StatTypes::DeltaWriteLagTotal
            | StatTypes::DeltaWriteLagMax
            | StatTypes::DeltaWriteLagMin => {
                self.handle_gauge(stat_type, val);
            }

            // counters
            _ => {
                self.handle_counter(stat_type, val);
            }
        }
    }

    fn handle_timer(&self, stat_type: StatTypes, duration_us: i64) {
        let stat_string = stat_type.to_string();

        if let Ok(duration) = duration_us.try_into() {
            self.metrics
                .timer(stat_string.as_str())
                .interval_us(duration);
        } else {
            error!("Failed to report timer to statsd with an i64 that couldn't fit into u64.");
        }
    }

    fn handle_gauge(&self, stat_type: StatTypes, count: i64) {
        let stat_string = stat_type.to_string();
        let key = stat_string.as_str();

        self.metrics.gauge(key).value(count);
    }

    fn handle_counter(&self, stat_type: StatTypes, count: i64) {
        let stat_string = stat_type.to_string();
        let key = stat_string.as_str();

        let sized_count: usize = count.try_into().expect("Could not convert to usize");

        self.metrics.counter(key).count(sized_count);
    }
}

struct LagMetrics {
    total: i64,
    max: Option<i64>,
    min: Option<i64>,
    num_partitions: i64,
}

fn create_queue(endpoint: &str, app_id: &str) -> InputQueueScope {
    let scope = Statsd::send_to(endpoint)
        .unwrap()
        // don't send stats immediately -
        // wait to trigger on input queue size
        .queued(INPUT_QUEUE_SIZE)
        .named(app_id)
        .metrics();

    scope
}
