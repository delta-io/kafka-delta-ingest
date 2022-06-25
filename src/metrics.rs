use dipstick::*;
use std::{convert::TryInto, time::Instant};

use crate::{
    errors::IngestMetricsError,
    settings::{
        DEFAULT_INPUT_QUEUE_SIZE, METRICS_INPUT_QUEUE_SIZE_VAR_NAME, METRICS_PREFIX_VAR_NAME,
    },
};

/// Wraps a [`dipstick::queue::InputQueueScope`] to provide a higher level API for recording metrics.
#[derive(Clone)]
pub struct IngestMetrics {
    metrics: InputQueueScope,
}

impl IngestMetrics {
    /// Creates an instance of [`IngestMetrics`] for sending metrics to statsd.
    pub fn new(endpoint: &str) -> Result<Self, IngestMetricsError> {
        let metrics = create_queue(endpoint)?;

        Ok(Self { metrics })
    }

    /// increments a counter for message deserialized
    pub fn message_deserialized(&self) {
        self.record_one(StatType::MessageDeserialized);
    }

    /// increments a counter for message deserialization failed
    pub fn message_deserialization_failed(&self) {
        self.record_one(StatType::MessageDeserializationFailed);
    }

    /// records a guage stat for message size
    pub fn message_deserialized_size(&self, size: usize) {
        self.record_stat(StatType::MessageSize, size as i64);
    }

    /// increments a counter for message transformed
    pub fn message_transformed(&self) {
        self.record_one(StatType::MessageTransformed);
    }

    /// increments a counter for message transform failed
    pub fn message_transform_failed(&self) {
        self.record_one(StatType::MessageTransformFailed);
    }

    /// increments a counter for record batch started
    pub fn batch_started(&self) {
        self.record_one(StatType::RecordBatchStarted);
    }

    /// increments a counter for record batch completed.
    /// records a guage stat for buffered record batches.
    /// records a timer stat for record batch write duration.
    pub fn batch_completed(&self, buffered_record_batch_count: usize, timer: &Instant) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_one(StatType::RecordBatchCompleted);
        self.record_stat(
            StatType::BufferedRecordBatches,
            buffered_record_batch_count as i64,
        );
        self.record_stat(StatType::RecordBatchWriteDuration, duration);
    }

    /// increments a counter for delta write started
    pub fn delta_write_started(&self) {
        self.record_one(StatType::DeltaWriteStarted);
    }

    /// increments a counter for delta write started.
    /// records a timer stat for delta write duration.
    pub fn delta_write_completed(&self, timer: &Instant) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_one(StatType::DeltaWriteCompleted);
        self.record_stat(StatType::DeltaWriteDuration, duration);
    }

    /// increments a counter for delta write failed.
    pub fn delta_write_failed(&self) {
        self.record_one(StatType::DeltaWriteFailed);
    }

    /// records a guage for delta file size.
    pub fn delta_file_size(&self, size: i64) {
        self.record_stat(StatType::DeltaAddFileSize, size as i64);
    }

    /// records total, max, and min consumer lag for offsets held in buffer.
    /// also records the number of partitions represented by the buffer lag vector.
    pub fn buffer_lag(&self, buffer_lags: Vec<i64>) {
        let lag_metrics = self.calculate_lag_metrics(buffer_lags);

        self.record_stat(StatType::BufferNumPartitions, lag_metrics.num_partitions);
        self.record_stat(StatType::BufferLagTotal, lag_metrics.total);

        if let Some(max) = lag_metrics.max {
            self.record_stat(StatType::BufferLagMax, max);
        }

        if let Some(min) = lag_metrics.min {
            self.record_stat(StatType::BufferLagMin, min);
        }
    }

    /// records total, max, and min consumer lag for offsets written to delta.
    /// also records the number of partitions represented by the write lag vector.
    pub fn delta_lag(&self, write_lags: Vec<i64>) {
        let lag_metrics = self.calculate_lag_metrics(write_lags);

        self.record_stat(
            StatType::DeltaWriteNumPartitions,
            lag_metrics.num_partitions,
        );
        self.record_stat(StatType::DeltaWriteLagTotal, lag_metrics.total);

        if let Some(max) = lag_metrics.max {
            self.record_stat(StatType::DeltaWriteLagMax, max);
        }

        if let Some(min) = lag_metrics.min {
            self.record_stat(StatType::DeltaWriteLagMin, min);
        }
    }

    /// Calculates total, max, min and num_partitions from the vector of lags.
    fn calculate_lag_metrics(&self, lags: Vec<i64>) -> LagMetrics {
        let total: i64 = lags.iter().sum();
        let max = lags.iter().max().copied();
        let min = lags.iter().min().copied();
        let num_partitions = lags.len() as i64;

        LagMetrics {
            total,
            max,
            min,
            num_partitions,
        }
    }

    /// Records a count of 1 for the metric.
    fn record_one(&self, stat_type: StatType) {
        self.record_stat(stat_type, 1);
    }

    /// Records a metric for the given [`StatType`] with the given value.
    fn record_stat(&self, stat_type: StatType, val: i64) {
        match stat_type {
            // timers
            StatType::RecordBatchWriteDuration | StatType::DeltaWriteDuration => {
                self.handle_timer(stat_type, val);
            }

            // gauges
            StatType::BufferedRecordBatches
            | StatType::MessageSize
            | StatType::DeltaAddFileSize
            | StatType::BufferNumPartitions
            | StatType::BufferLagTotal
            | StatType::BufferLagMax
            | StatType::BufferLagMin
            | StatType::DeltaWriteNumPartitions
            | StatType::DeltaWriteLagTotal
            | StatType::DeltaWriteLagMax
            | StatType::DeltaWriteLagMin => {
                self.handle_gauge(stat_type, val);
            }

            // counters
            _ => {
                self.handle_counter(stat_type, val);
            }
        }
    }

    /// Records a timer metric for the given [`StatType`].
    fn handle_timer(&self, stat_type: StatType, duration_us: i64) {
        let stat_string = stat_type.to_string();

        if let Ok(duration) = duration_us.try_into() {
            self.metrics
                .timer(stat_string.as_str())
                .interval_us(duration);
        } else {
            log::error!("Failed to report timer to statsd with an i64 that couldn't fit into u64.");
        }
    }

    /// Records a gauge metric for the given [`StatType`].
    fn handle_gauge(&self, stat_type: StatType, count: i64) {
        let stat_string = stat_type.to_string();
        let key = stat_string.as_str();

        self.metrics.gauge(key).value(count);
    }

    /// Records a counter metric for the given [`StatType`].
    fn handle_counter(&self, stat_type: StatType, count: i64) {
        let stat_string = stat_type.to_string();
        let key = stat_string.as_str();

        let sized_count: usize = count.try_into().expect("Could not convert to usize");

        self.metrics.counter(key).count(sized_count);
    }
}

/// Stat types for the various metrics reported by the application
#[derive(Display, Hash, PartialEq, Eq)]
enum StatType {
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

/// Struct representing aggregate lag metrics calculated from a vector of partition lags.
struct LagMetrics {
    total: i64,
    max: Option<i64>,
    min: Option<i64>,
    num_partitions: i64,
}

/// Creates a statsd metric scope to send metrics to.
fn create_queue(endpoint: &str) -> Result<InputQueueScope, IngestMetricsError> {
    let input_queue_size = if let Ok(val) = std::env::var(METRICS_INPUT_QUEUE_SIZE_VAR_NAME) {
        val.parse::<usize>()
            .map_err(|_| IngestMetricsError::InvalidMetricsInputQueueSize(val))?
    } else {
        DEFAULT_INPUT_QUEUE_SIZE
    };

    let prefix =
        std::env::var(METRICS_PREFIX_VAR_NAME).unwrap_or_else(|_| "kafka_delta_ingest".to_string());

    let scope = Statsd::send_to(endpoint)
        .unwrap()
        // don't send stats immediately -
        // wait to trigger on input queue size
        .queued(input_queue_size)
        .named(prefix)
        .metrics();

    Ok(scope)
}
