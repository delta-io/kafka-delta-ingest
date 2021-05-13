#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate maplit;

#[macro_use]
extern crate strum_macros;

use deltalake::{action, DeltaTableError, DeltaTransactionError};
use futures::stream::StreamExt;
use log::{error, info, warn};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaError,
    util::Timeout,
    ClientContext, Message, Offset, TopicPartitionList,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc::Sender, Mutex};
use tokio_util::sync::CancellationToken;

pub mod deltalake_ext;
pub mod instrumentation;
mod transforms;
pub mod write_ahead_log;

use crate::transforms::*;
use crate::write_ahead_log::*;
use crate::{
    deltalake_ext::{DeltaWriter, DeltaWriterError},
    instrumentation::{Instrumentation, Statistic},
};
use rdkafka::message::BorrowedMessage;

type DataTypeTransactionId = i64;
type DataTypePartition = i32;
type DataTypeOffset = i64;

#[derive(thiserror::Error, Debug)]
pub enum KafkaJsonToDeltaError {
    #[error("Kafka error: {source}")]
    Kafka {
        #[from]
        source: KafkaError,
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("DeltaTransaction failed: {source}")]
    DeltaTransaction {
        #[from]
        source: DeltaTransactionError,
    },

    #[error("DeltaWriter error: {source}")]
    DeltaWriter {
        #[from]
        source: DeltaWriterError,
    },

    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
    },

    #[error("WriteAheadLogError: {source}")]
    WriteAheadLog {
        #[from]
        source: WriteAheadLogError,
    },

    #[error("ValueBufferError: {source}")]
    ValueBuffer {
        #[from]
        source: ValueBufferError,
    },

    #[error("A message was handled on partition {partition} but this partition is not tracked")]
    UntrackedTopicPartition { partition: i32 },

    #[error("Topic partition offset list is empty")]
    MissingPartitionOffsets,

    #[error("Kafka message payload was empty at partition {partition} offset {offset}")]
    KafkaMessageNoPayload { partition: i32, offset: i64 },

    #[error("Kafka message deserialization failed at partition {partition} offset {offset}")]
    KafkaMessageDeserialization { partition: i32, offset: i64 },
}

/// This error is used in stream run_loop to indicate whether the stream should
/// jump straight to the next message with `Continue` or completely fail with `General` error.
#[derive(thiserror::Error, Debug)]
enum ProcessingError {
    #[error("Continue to the next message")]
    Continue,

    #[error("KafkaJsonToDeltaError: {source}")]
    General {
        #[from]
        source: KafkaJsonToDeltaError,
    },
}

impl From<WriteAheadLogError> for ProcessingError {
    fn from(e: WriteAheadLogError) -> Self {
        ProcessingError::General { source: e.into() }
    }
}
impl From<DeltaWriterError> for ProcessingError {
    fn from(e: DeltaWriterError) -> Self {
        ProcessingError::General { source: e.into() }
    }
}

struct ProcessingState {
    delta_writer: DeltaWriter,
    value_buffers: ValueBuffers,
    transformer: Transformer,
    wal: Arc<dyn WriteAheadLog + Sync + Send>,
    latency_timer: Instant,
}

/// Encapsulates a single topic-to-table ingestion stream.
pub struct KafkaJsonToDelta {
    topic: String,
    table_location: String,
    app_id: String,
    allowed_latency: u64,
    max_messages_per_batch: usize,
    min_bytes_per_file: usize,
    transforms: HashMap<String, String>,
    consumer: StreamConsumer<Context>,
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
    stats_sender: Sender<Statistic>,
}

impl Instrumentation for KafkaJsonToDelta {
    fn stats_sender(&self) -> Sender<Statistic> {
        self.stats_sender.clone()
    }
}

impl KafkaJsonToDelta {
    /// Creates a new instance of KafkaJsonToDelta.
    pub fn new(
        topic: String,
        table_location: String,
        kafka_brokers: String,
        consumer_group_id: String,
        additional_kafka_settings: Option<HashMap<String, String>>,
        app_id: String,
        allowed_latency: u64,
        max_messages_per_batch: usize,
        min_bytes_per_file: usize,
        transforms: HashMap<String, String>,
        stats_sender: Sender<Statistic>,
    ) -> Result<Self, KafkaJsonToDeltaError> {
        let mut kafka_client_config = ClientConfig::new();

        info!("App id is {}", app_id);
        info!("Kafka broker string is {}", kafka_brokers);
        info!("Kafka consumer group id is {}", consumer_group_id);

        kafka_client_config
            .set("bootstrap.servers", kafka_brokers.clone())
            .set("group.id", consumer_group_id)
            .set("enable.auto.commit", "false");

        if let Some(additional) = additional_kafka_settings {
            for (k, v) in additional.iter() {
                info!("Applying additional kafka setting {} = {}", k, v);
                kafka_client_config.set(k, v);
            }
        }

        let partition_assignment = Arc::new(Mutex::new(PartitionAssignment::new()));
        let consumer_context = Context::new(partition_assignment.clone());

        let consumer: StreamConsumer<Context> =
            kafka_client_config.create_with_context(consumer_context)?;

        Ok(Self {
            topic,
            table_location,
            app_id,
            allowed_latency,
            max_messages_per_batch,
            min_bytes_per_file,
            transforms,
            consumer,
            partition_assignment,
            stats_sender,
        })
    }

    /// Starts the topic-to-table ingestion stream.
    pub async fn start(
        &mut self,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        info!("Starting stream");

        self.consumer.subscribe(&[self.topic.as_str()])?;

        let res = self.run_loop(cancellation_token).await;

        if res.is_err() {
            error!("Stream stopped with error result: {:?}", res);
        } else {
            info!("Stream stopped");
        }

        res
    }

    async fn init(&mut self, state: &ProcessingState) -> Result<(), KafkaJsonToDeltaError> {
        info!("Initializing last Delta transaction version before starting run loop.");

        let last_txn_version = state
            .delta_writer
            .last_transaction_version(self.app_id.as_str())
            .await?;

        if let Some(txn_version) = last_txn_version {
            let previous_wal_entry = state.wal.get_entry_by_transaction_id(txn_version).await?;
            self.init_offsets(&previous_wal_entry.partition_offsets)
                .await?;
        }

        Ok(())
    }

    async fn process_message(
        &mut self,
        state: &mut ProcessingState,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), ProcessingError> {
        let mut value = self.deserialize_message(msg).await?;
        self.transform_value(state, &mut value, msg).await?;

        if state.value_buffers.is_tracking(msg.partition()) {
            // This is the 99% case. We are already tracking this partition. Just
            // buffer it.
            state
                .value_buffers
                .add(msg.partition(), msg.offset(), value);
            self.log_message_buffered(msg, state.value_buffers.len())
                .await;
        } else {
            self.handle_non_tracked_partition(state, msg, value).await?;
        }

        if self.should_complete_record_batch(&state.value_buffers, &state.latency_timer) {
            self.finalize_record_batch(state).await?;
        }

        Ok(())
    }

    async fn deserialize_message(
        &self,
        msg: &BorrowedMessage<'_>,
    ) -> Result<Value, ProcessingError> {
        match deserialize_message(msg) {
            Ok(v) => {
                self.log_message_deserialized(msg).await;
                Ok(v)
            }
            Err(KafkaJsonToDeltaError::KafkaMessageNoPayload { .. }) => {
                self.log_message_deserialization_failed(msg).await;
                Err(ProcessingError::Continue)
            }
            Err(KafkaJsonToDeltaError::KafkaMessageDeserialization { .. }) => {
                self.log_message_deserialization_failed(msg).await;
                Err(ProcessingError::Continue)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn transform_value(
        &mut self,
        state: &mut ProcessingState,
        value: &mut Value,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), ProcessingError> {
        // Transform
        // TODO: Add better transform failure handling, ideally send to a dlq
        match state.transformer.transform(value, msg) {
            Err(_) => {
                self.log_message_transform_failed(msg).await;
                Err(ProcessingError::Continue)
            }
            _ => {
                self.log_message_transformed(msg).await;
                Ok(())
            }
        }
    }

    async fn handle_non_tracked_partition(
        &self,
        state: &mut ProcessingState,
        msg: &BorrowedMessage<'_>,
        value: Value,
    ) -> Result<(), ProcessingError> {
        if let Some(tx_version) = state
            .delta_writer
            .last_transaction_version(self.app_id.as_str())
            .await?
        {
            self.log_assignment_untracked_skipped(msg).await;

            // Since this is an untracked partition and a Delta transaction exists for
            // the table already, we need to update partition assignments, seek the
            // consumer and wait the next iteration of the run_loop.
            update_partition_assignment(
                self.partition_assignment.clone(),
                state.wal.clone(),
                Some(tx_version),
                &mut state.value_buffers,
                self.topic.as_str(),
                &self.consumer,
            )
            .await?;
            Err(ProcessingError::Continue)
        } else {
            // Since there is no delta transaction - this should be the first message
            // available on the partition.
            // Update partition assignment and buffer.
            update_partition_assignment(
                self.partition_assignment.clone(),
                state.wal.clone(),
                None,
                &mut state.value_buffers,
                self.topic.as_str(),
                &self.consumer,
            )
            .await?;

            state
                .value_buffers
                .add(msg.partition(), msg.offset(), value);

            self.log_assignment_untracked_buffered(msg, state.value_buffers.len())
                .await;
            Ok(())
        }
    }

    async fn finalize_record_batch(
        &self,
        state: &mut ProcessingState,
    ) -> Result<(), ProcessingError> {
        self.log_record_batch_started().await;

        let (values, partition_offsets) =
            consume_value_buffers(self.partition_assignment.clone(), &mut state.value_buffers)
                .await?;

        let record_batch = deltalake_ext::record_batch_from_json(
            state.delta_writer.arrow_schema(),
            values.as_slice(),
        )?;

        // Since we are writing a record batch to the in-memory parquet file, value buffers must be marked
        // dirty so we know we have to drop all buffers and re-seek if a rebalance occurs before the next file write.
        state.value_buffers.mark_dirty();

        let record_batch_timer = Instant::now();
        state.delta_writer.write_record_batch(&record_batch).await?;

        self.log_record_batch_completed(
            state.delta_writer.buffered_record_batch_count(),
            &record_batch_timer,
        )
        .await;

        // Finalize file and write Delta transaction
        if self.should_complete_file(&state.delta_writer, &state.latency_timer) {
            // Reset the latency timer to track allowed latency for the next file
            state.latency_timer = Instant::now();

            // Lookup the last transaction version from the Delta Log
            let last_txn_version = state
                .delta_writer
                .last_transaction_version(self.app_id.as_str())
                .await?;

            // Prepare the new WAL entry
            let wal_timer = Instant::now();
            let wal_entry = if let Some(txn_version) = last_txn_version {
                self.log_delta_tx_version_found(self.app_id.as_str(), txn_version)
                    .await;

                let last_wal_entry = state.wal.get_entry_by_transaction_id(txn_version).await?;

                last_wal_entry.prepare_next(&partition_offsets)
            } else {
                self.log_delta_tx_version_not_found(self.app_id.as_str())
                    .await;
                WriteAheadLogEntry::new(
                    1,
                    TransactionState::Prepared,
                    None, /*delta table version starts out empty*/
                    partition_offsets.clone(),
                )
            };
            state.wal.put_entry(&wal_entry).await?;
            self.log_write_ahead_log_prepared().await;

            // Complete the delta write
            let delta_write_timer = Instant::now();
            let txn = action::Action::txn(action::Txn {
                appId: self.app_id.clone(),
                version: wal_entry.transaction_id,
                lastUpdated: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            });
            self.log_delta_write_started().await;
            let commit_result = state.delta_writer.write_file(&mut vec![txn]).await;
            match commit_result {
                Ok(version) => {
                    self.log_delta_write_completed(&delta_write_timer).await;
                    state
                        .wal
                        .complete_entry(wal_entry.transaction_id, version)
                        .await?;
                    self.log_write_ahead_log_completed(&wal_timer).await;
                }
                Err(e) => {
                    state.wal.abort_entry(wal_entry.transaction_id).await?;
                    self.log_write_ahead_log_aborted().await;
                    self.log_delta_write_failed().await;
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    async fn run_loop(
        &mut self,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let mut state = ProcessingState {
            delta_writer: DeltaWriter::for_table_path(self.table_location.clone()).await?,
            value_buffers: ValueBuffers::new(),
            transformer: Transformer::from_transforms(&self.transforms)?,
            wal: write_ahead_log::new_write_ahead_log(self.app_id.to_string()).await?,
            latency_timer: Instant::now(),
        };

        self.init(&state).await?;

        let mut stream = self.consumer.stream();
        //state.latency_timer = Instant::now(); TODO

        info!("Starting run loop.");

        while let Some(message) = stream.next().await {
            match message {
                Ok(m) => {
                    match self.process_message(&mut state, &m).await {
                        Ok(_) | Err(ProcessingError::Continue) => {
                            // Exit if the cancellation token is set
                            if let Some(token) = cancellation_token {
                                if token.is_cancelled() {
                                    self.log_stream_cancelled(&m).await;
                                    return Ok(());
                                }
                            }
                        }
                        Err(ProcessingError::General { source }) => return Err(source),
                    }
                }
                Err(e) => {
                    // TODO: What does an error unwrapping the BorrowedMessage mean? Determine if this should stop the stream.
                    error!(
                        "Error getting BorrowedMessage while processing stream {:?}",
                        e
                    );
                }
            }
        }

        Ok(())
    }

    fn should_complete_record_batch(
        &self,
        value_buffers: &ValueBuffers,
        latency_timer: &Instant,
    ) -> bool {
        let should = value_buffers.len() == self.max_messages_per_batch
            || latency_timer.elapsed().as_millis() >= (self.allowed_latency * 1000) as u128;

        info!("Should complete record batch? {}", should);
        info!("Value buffers len is {}", value_buffers.len());

        should
    }

    fn should_complete_file(&self, delta_writer: &DeltaWriter, latency_timer: &Instant) -> bool {
        let should = delta_writer.buffer_len() >= self.min_bytes_per_file
            || latency_timer.elapsed().as_secs() >= self.allowed_latency;

        info!("Should complete file? {}", should);
        info!(
            "Latency timer at {} secs",
            latency_timer.elapsed().as_secs()
        );
        info!("Delta buffer len at {} bytes", delta_writer.buffer_len());

        should
    }

    async fn init_offsets(
        &mut self,
        offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let topic_partition_list = self.consumer.assignment()?;

        let mut partition_assignment = self.partition_assignment.lock().await;
        let partitions = partition_vec_from_topic_partition_list(&topic_partition_list);

        partition_assignment.update_assignment(&partitions);
        partition_assignment.update_offsets(offsets);

        let assignment = partition_assignment.assignment();

        // Seek the consumer to current offsets
        for (k, v) in assignment.iter() {
            match v {
                Some(offset) => {
                    info!(
                        "init_offsets: Seeking consumer to offset {} for partition {}",
                        offset, k
                    );
                    self.consumer.seek(
                        self.topic.as_str(),
                        *k,
                        Offset::Offset(*offset),
                        Timeout::Never,
                    )?;
                }
                _ => {
                    info!(
                        "Partition {} has no recorded offset. Not seeking consumer. Default starting offsets will be applied.",
                        k
                    );
                }
            }
        }

        Ok(())
    }
}

// NOTE: Lifetime constraint for write_ahead_log param is a workaround for
// https://github.com/rust-lang/rust/issues/63033
async fn update_partition_assignment<'a>(
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
    write_ahead_log: Arc<dyn WriteAheadLog + 'a + Send + Sync>,
    transaction_id: Option<DataTypeTransactionId>,
    value_buffers: &mut ValueBuffers,
    topic: &str,
    consumer: &StreamConsumer<Context>,
) -> Result<(), KafkaJsonToDeltaError> {
    let mut partition_assignment = partition_assignment.lock().await;

    // Update offsets from the previous transaction id if one exists
    if let Some(transaction_id) = transaction_id {
        let previous_wal_entry = write_ahead_log
            .get_entry_by_transaction_id(transaction_id)
            .await?;

        partition_assignment.update_offsets(&previous_wal_entry.partition_offsets);
    }

    let assignment = partition_assignment.assignment();
    // Extract the offsets for newly tracked partitions and seek the consumer.
    // Essentially, we are catching up with the last assignment from the most recent
    // rebalance event here and we will start buffering messages correctly on the next
    // iteraton of run_loop.
    // `run_loop` call context must `continue` the run loop without processing further.
    // Basically *drop/do-not-append* the current message since it will not be at the correct offset since
    // the partition was previously untracked and we are only now doing the `consumer.seek` while
    // holding the partition_assignment lock.
    // It would be nice to seek the consumer immediately on rebalance since we acquire a
    // partition_assignment lock there anyway, instead of waiting to
    // seek the consumer within `run_loop` but I haven't been able to figure out how to do that
    // with the available rdkafka API for handling rebalance.
    value_buffers.update_partitions(assignment)?;
    for (k, v) in assignment.iter() {
        match v {
            Some(offset) => {
                info!(
                    "update_partition_assignment: Seeking consumer to offset {} for partition: {}",
                    offset, k
                );
                consumer.seek(topic, *k, Offset::Offset(*offset), Timeout::Never)?;
            }
            _ => {
                info!(
                    "Partition {} has no recorded offset. Not seeking consumer.",
                    k
                );
            }
        }
    }

    Ok(())
}

fn deserialize_message<M>(m: &M) -> Result<serde_json::Value, KafkaJsonToDeltaError>
where
    M: Message,
{
    // Deserialize the rdkafka message into a serde_json::Value
    let message_bytes = match m.payload() {
        Some(bytes) => bytes,
        None => {
            warn!(
                "Payload has no bytes at partition: {} with offset: {}",
                m.partition(),
                m.offset()
            );
            return Err(KafkaJsonToDeltaError::KafkaMessageNoPayload {
                partition: m.partition(),
                offset: m.offset(),
            });
        }
    };

    let value: Value = match serde_json::from_slice(message_bytes) {
        Ok(v) => v,
        Err(e) => {
            // TODO: Add better deserialization error handling
            // Ideally, provide an option to send the message bytes to a dead letter queue
            error!("Error deserializing message {:?}", e);
            return Err(KafkaJsonToDeltaError::KafkaMessageDeserialization {
                partition: m.partition(),
                offset: m.offset(),
            });
        }
    };

    Ok(value)
}

async fn consume_value_buffers(
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
    buffers: &mut ValueBuffers,
) -> Result<(Vec<Value>, HashMap<DataTypePartition, DataTypeOffset>), KafkaJsonToDeltaError> {
    let mut partition_assignment = partition_assignment.lock().await;

    let ConsumedBuffers {
        values,
        partition_offsets,
    } = buffers.consume();

    partition_assignment.update_offsets(&partition_offsets);

    let partition_offsets = partition_assignment.partition_offsets();

    Ok((values, partition_offsets))
}

#[derive(thiserror::Error, Debug)]
pub enum ValueBufferError {
    #[error("Illegal tracked buffer state for partition {partition}.")]
    IllegalTrackedBufferState { partition: DataTypePartition },
}

pub struct ValueBuffers {
    buffers: HashMap<DataTypePartition, ValueBuffer>,
    len: usize,
    partitions: Vec<DataTypePartition>,
    dirty: bool,
}

impl ValueBuffers {
    pub fn new() -> Self {
        Self {
            buffers: HashMap::new(),
            len: 0,
            partitions: Vec::new(),
            dirty: false,
        }
    }

    pub fn for_partitions(partitions: Vec<DataTypePartition>) -> Self {
        Self {
            buffers: HashMap::new(),
            len: 0,
            partitions,
            dirty: false,
        }
    }

    pub fn add(&mut self, partition: DataTypePartition, offset: DataTypeOffset, value: Value) {
        let buffer = self
            .buffers
            .entry(partition)
            .or_insert_with(|| ValueBuffer::new());
        buffer.add(value, offset);
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn consume(&mut self) -> ConsumedBuffers {
        let mut partition_offsets = HashMap::new();

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| match buffer.consume() {
                Some((values, offset)) => {
                    partition_offsets.insert(partition.clone(), offset);
                    Some(values)
                }
                None => None,
            })
            .flatten()
            .collect();

        self.len = 0;

        ConsumedBuffers {
            values,
            partition_offsets,
        }
    }

    pub fn update_partitions(
        &mut self,
        assigned_partitions: &HashMap<DataTypePartition, Option<DataTypeOffset>>,
    ) -> Result<(), ValueBufferError> {
        // Drop all buffers that are no longer assigned
        let partitions = self.partitions.iter();
        for p in partitions {
            if !assigned_partitions.contains_key(&p) {
                let buffer = self.buffers.remove(&p);

                if let Some(b) = buffer {
                    self.len = self.len - b.values.len();
                }
            }
        }

        // If self has been marked dirty (due to a rebalance after a consume),
        // reset all buffers and offsets and mark not dirty.
        if self.dirty {
            for (k, v) in self.buffers.iter_mut() {
                match assigned_partitions.get(k) {
                    Some(offset_option) => {
                        v.reset(*offset_option);
                    }
                    _ => {
                        // This should never happen since we removed all unassigned buffers above.
                        return Err(ValueBufferError::IllegalTrackedBufferState { partition: *k });
                    }
                }
            }
        }

        // Add all partitions that were not previously assigned.
        for (k, _) in assigned_partitions.iter() {
            if !self.is_tracking(*k) {
                self.buffers.insert(*k, ValueBuffer::new());
            }
        }

        // Set the list of currently assigned partitions.
        self.partitions = assigned_partitions.keys().map(|k| *k).collect();

        // Since we have synced up with the currently assigned partitiions and offsets, we can mark
        // ourselves clean.
        self.dirty = false;

        Ok(())
    }

    pub fn is_tracking(&self, partition: DataTypePartition) -> bool {
        self.partitions.contains(&partition)
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }
}

pub struct ConsumedBuffers {
    pub values: Vec<Value>,
    pub partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
}

pub struct ValueBuffer {
    last_offset: Option<DataTypeOffset>,
    values: Vec<Value>,
}

impl ValueBuffer {
    fn new() -> Self {
        Self {
            last_offset: None,
            values: Vec::new(),
        }
    }

    fn add(&mut self, value: Value, offset: DataTypeOffset) {
        self.last_offset = Some(offset);
        self.values.push(value);
    }

    fn reset(&mut self, offset: Option<DataTypeOffset>) {
        self.last_offset = offset;
        self.values.clear();
    }

    fn consume(&mut self) -> Option<(Vec<Value>, DataTypeOffset)> {
        match self.last_offset {
            Some(last_offset) => {
                let consumed = (std::mem::replace(&mut self.values, vec![]), last_offset);
                self.last_offset = None;
                Some(consumed)
            }
            None => None,
        }
    }
}

pub struct Context {
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);

        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("Received new partition assignment list");
                let partitions = partition_vec_from_topic_partition_list(tpl);
                let partitions = Arc::new(partitions);
                on_rebalance_assign(self.partition_assignment.clone(), partitions);
            }
            Rebalance::Revoke => {
                info!("Partition assignments revoked");
                on_rebalance_revoke(self.partition_assignment.clone());
            }
            Rebalance::Error(e) => {
                warn!(
                    "Unexpected Kafka error in post_rebalance invocation {:?}",
                    e
                );
            }
        }
    }
}

fn partition_vec_from_topic_partition_list(
    topic_partition_list: &TopicPartitionList,
) -> Vec<DataTypePartition> {
    topic_partition_list
        .to_topic_map()
        .iter()
        .map(|((_, p), _)| *p)
        .collect()
}

fn on_rebalance_assign(
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
    partitions: Arc<Vec<DataTypePartition>>,
) {
    let _fut = tokio::spawn(async move {
        let mut partition_assignment = partition_assignment.lock().await;
        partition_assignment.update_assignment(&partitions);
    });
}

fn on_rebalance_revoke(partition_assignment: Arc<Mutex<PartitionAssignment>>) {
    let _fut = tokio::spawn(async move {
        let mut partition_assignment = partition_assignment.lock().await;
        partition_assignment.clear();
    });
}

impl Context {
    pub fn new(partition_assignment: Arc<Mutex<PartitionAssignment>>) -> Self {
        Self {
            partition_assignment,
        }
    }
}

pub struct PartitionAssignment {
    assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl PartitionAssignment {
    pub fn new() -> Self {
        Self {
            assignment: HashMap::new(),
        }
    }

    pub fn update_assignment(&mut self, partitions: &Vec<DataTypePartition>) {
        self.assignment.retain(|p, _| partitions.contains(p));
        for p in partitions {
            if !self.assignment.contains_key(p) {
                self.assignment.insert(*p, None);
            }
        }
    }

    pub fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(&k) {
                *entry = Some(*v);
            } else {
                warn!("Partition {} is not part of the assignment.", k);
            }
        }
    }

    pub fn partitions(&self) -> Vec<DataTypePartition> {
        self.assignment.keys().map(|k| *k).collect()
    }

    pub fn assignment(&self) -> &HashMap<DataTypePartition, Option<DataTypeOffset>> {
        &self.assignment
    }

    pub fn partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let partition_offsets = self
            .assignment
            .iter()
            .filter_map(|(k, v)| match v {
                Some(o) => Some((*k, *o)),
                None => None,
            })
            .collect();

        partition_offsets
    }

    pub fn clear(&mut self) {
        self.assignment.clear();
    }
}
