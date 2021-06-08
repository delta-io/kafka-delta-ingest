#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate strum_macros;

use deltalake::{action, DeltaTableError, DeltaTransactionError};
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
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

use crate::transforms::*;
use crate::{
    deltalake_ext::{DeltaWriter, DeltaWriterError},
    instrumentation::{Instrumentation, Statistic},
};
use deltalake::action::{Action, Add};
use rdkafka::message::BorrowedMessage;

type DataTypePartition = i32;
type DataTypeOffset = i64;

const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

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

impl From<DeltaWriterError> for ProcessingError {
    fn from(e: DeltaWriterError) -> Self {
        ProcessingError::General { source: e.into() }
    }
}

/// Application options
pub struct Options {
    /// Source kafka topic to consume messages from
    pub topic: String,
    /// Destination delta table location to produce messages into
    pub table_location: String,
    /// Unique per topic per environment. Must be the same for all processes that are part of a single job.
    pub app_id: String,
    /// Max desired latency from when a message is received to when it is written and
    /// committed to the target delta table (in seconds)
    pub allowed_latency: u64,
    /// Number of messages to buffer before writing a record batch.
    pub max_messages_per_batch: usize,
    /// Desired minimum number of compressed parquet bytes to buffer in memory
    /// before writing to storage and committing a transaction.
    pub min_bytes_per_file: usize,
}

impl Options {
    pub fn new(
        topic: String,
        table_location: String,
        app_id: String,
        allowed_latency: u64,
        max_messages_per_batch: usize,
        min_bytes_per_file: usize,
    ) -> Self {
        Self {
            topic,
            table_location,
            app_id,
            allowed_latency,
            max_messages_per_batch,
            min_bytes_per_file,
        }
    }
}

/// Encapsulates a single topic-to-table ingestion stream.
pub struct KafkaJsonToDelta {
    opts: Options,
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

struct ProcessingState {
    delta_writer: DeltaWriter,
    value_buffers: ValueBuffers,
    transformer: Transformer,
    latency_timer: Instant,
}

impl KafkaJsonToDelta {
    /// Creates a new instance of KafkaJsonToDelta.
    pub fn new(
        opts: Options,
        kafka_brokers: String,
        consumer_group_id: String,
        additional_kafka_settings: Option<HashMap<String, String>>,
        transforms: HashMap<String, String>,
        stats_sender: Sender<Statistic>,
    ) -> Result<Self, KafkaJsonToDeltaError> {
        let mut kafka_client_config = ClientConfig::new();

        info!("App id is {}", opts.app_id);
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
            opts,
            transforms,
            consumer,
            partition_assignment,
            stats_sender,
        })
    }

    pub fn app_id_for_partition(&self, partition: DataTypePartition) -> String {
        format!("{}-{}", self.opts.app_id, partition)
    }

    /// Starts the topic-to-table ingestion stream.
    pub async fn start(
        &mut self,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        info!("Starting stream");

        self.consumer.subscribe(&[self.opts.topic.as_str()])?;

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

        let mut offsets = HashMap::new();

        for p in state.value_buffers.partitions.iter() {
            let partition = *p;

            let last_txn_version = state
                .delta_writer
                .last_transaction_version(&self.app_id_for_partition(partition));

            if let Some(v) = last_txn_version {
                offsets.insert(partition, DataTypeOffset::from(v));
            }
        }

        self.init_offsets(&offsets).await?;

        Ok(())
    }

    async fn process_message(
        &self,
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

        Ok(())
    }

    async fn deserialize_message(
        &self,
        msg: &BorrowedMessage<'_>,
    ) -> Result<Value, ProcessingError> {
        // Deserialize the rdkafka message into a serde_json::Value
        let message_bytes = match msg.payload() {
            Some(bytes) => bytes,
            None => {
                warn!(
                    "Payload has no bytes at partition: {} with offset: {}",
                    msg.partition(),
                    msg.offset()
                );
                self.log_message_deserialization_failed(msg).await;
                return Err(ProcessingError::Continue);
            }
        };

        let value: Value = match serde_json::from_slice(message_bytes) {
            Ok(v) => v,
            Err(e) => {
                // TODO: Add better deserialization error handling
                // Ideally, provide an option to send the message bytes to a dead letter queue
                error!("Error deserializing message {:?}", e);
                self.log_message_deserialization_failed(msg).await;
                return Err(ProcessingError::Continue);
            }
        };

        self.log_message_deserialized(msg).await;
        Ok(value)
    }

    async fn transform_value(
        &self,
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
        let partition = msg.partition();

        if let Some(offset) = state
            .delta_writer
            .last_transaction_version(&self.app_id_for_partition(partition))
        {
            self.log_assignment_untracked_skipped(msg).await;

            // Since this is an untracked partition and a Delta transaction exists for
            // the table already, we need to update partition assignments, seek the
            // consumer and wait the next iteration of the run_loop.
            self.update_partition_assignment(state, Some((partition, offset)))
                .await?;
            Err(ProcessingError::Continue)
        } else {
            // Since there is no delta transaction - this should be the first message
            // available on the partition.
            // Update partition assignment and buffer.
            self.update_partition_assignment(state, None).await?;

            state.value_buffers.add(partition, msg.offset(), value);

            self.log_assignment_untracked_buffered(msg, state.value_buffers.len())
                .await;
            Ok(())
        }
    }

    async fn update_partition_assignment(
        &self,
        state: &mut ProcessingState,
        assign: Option<(DataTypePartition, DataTypeOffset)>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let mut partition_assignment = self.partition_assignment.lock().await;

        // TODO we don't need to update the whole map here, only single affected partition
        // TODO but even if we update all, we need get offsets from delta txn

        // Update offsets from the previous transaction id if one exists
        if let Some((p, o)) = assign {
            let mut offsets = HashMap::new();
            offsets.insert(p, o);
            partition_assignment.update_offsets(&offsets);
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
        state.value_buffers.update_partitions(assignment)?;
        for (k, v) in assignment.iter() {
            match v {
                Some(offset) => {
                    info!(
                        "update_partition_assignment: Seeking consumer to offset {} for partition: {}",
                        offset, k
                    );
                    self.consumer.seek(
                        &self.opts.topic,
                        *k,
                        Offset::Offset(*offset),
                        Timeout::Never,
                    )?;
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

    async fn finalize_record_batch(
        &self,
        state: &mut ProcessingState,
    ) -> Result<(), KafkaJsonToDeltaError> {
        self.log_record_batch_started().await;

        let (values, partition_offsets) = self.consume_value_buffers(state).await?;

        if values.is_empty() {
            return Ok(());
        }

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
            self.complete_file(state, partition_offsets).await?;
        }

        Ok(())
    }

    async fn run_loop(
        &mut self,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let mut state = ProcessingState {
            delta_writer: DeltaWriter::for_table_path(self.opts.table_location.clone()).await?,
            value_buffers: ValueBuffers::new(),
            transformer: Transformer::from_transforms(&self.transforms)?,
            latency_timer: Instant::now(),
        };

        self.init(&state).await?;

        let mut stream = self.consumer.stream();
        state.latency_timer = Instant::now();

        info!("Starting run loop.");

        while let Some(message) = stream.next().await {
            match message {
                Ok(m) => {
                    match self.process_message(&mut state, &m).await {
                        Ok(_) | Err(ProcessingError::Continue) => {
                            if self.should_complete_record_batch(&mut state) {
                                self.finalize_record_batch(&mut state).await?;
                            }

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

    fn should_complete_record_batch(&self, state: &mut ProcessingState) -> bool {
        let should = state.value_buffers.len() == self.opts.max_messages_per_batch
            || state.latency_timer.elapsed().as_millis()
                >= (self.opts.allowed_latency * 1000) as u128;

        debug!(
            "Should complete record batch? {}, {} >= {}",
            should,
            state.latency_timer.elapsed().as_millis(),
            (self.opts.allowed_latency * 1000) as u128
        );
        debug!("Value buffers len is {}", state.value_buffers.len());

        should
    }

    fn should_complete_file(&self, delta_writer: &DeltaWriter, latency_timer: &Instant) -> bool {
        let should = delta_writer.buffer_len() >= self.opts.min_bytes_per_file
            || latency_timer.elapsed().as_secs() >= self.opts.allowed_latency;

        debug!("Should complete file? {}", should);
        debug!(
            "Latency timer at {} secs",
            latency_timer.elapsed().as_secs()
        );
        debug!("Delta buffer len at {} bytes", delta_writer.buffer_len());

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
                        self.opts.topic.as_str(),
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

    async fn consume_value_buffers(
        &self,
        state: &mut ProcessingState,
    ) -> Result<(Vec<Value>, HashMap<DataTypePartition, DataTypeOffset>), KafkaJsonToDeltaError>
    {
        let mut partition_assignment = self.partition_assignment.lock().await;

        let ConsumedBuffers {
            values,
            partition_offsets,
        } = state.value_buffers.consume();

        partition_assignment.update_offsets(&partition_offsets);

        let partition_offsets = partition_assignment.partition_offsets();

        Ok((values, partition_offsets))
    }

    async fn build_actions(
        &self,
        partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
        add: Add,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        for (partition, offset) in partition_offsets.iter() {
            let txn = action::Action::txn(action::Txn {
                app_id: self.app_id_for_partition(*partition),
                version: *offset,
                last_updated: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            });

            actions.push(txn);
        }

        actions.push(Action::add(add));
        actions
    }

    async fn complete_file(
        &self,
        state: &mut ProcessingState,
        partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        // Reset the latency timer to track allowed latency for the next file
        state.latency_timer = Instant::now();

        let delta_write_timer = Instant::now();

        self.log_delta_write_started().await;

        // upload pending parquet file to delta store
        // TODO remove it if we got conflict error? or it'll be considered as tombstone
        let add = state.delta_writer.write_parquet_file().await?;

        // update the table to get the latest version
        // the consecutive updates will be made within error handles
        state.delta_writer.update_table().await?;

        let mut attempt_number: u32 = 0;

        loop {
            let version = state.delta_writer.table_version() + 1;
            let actions = self.build_actions(&partition_offsets, add.clone()).await;
            let commit_result = state.delta_writer.commit_version(version, actions).await;

            match commit_result {
                Ok(v) => {
                    assert_eq!(v, version);
                    self.log_delta_write_completed(version, &delta_write_timer)
                        .await;
                    return Ok(());
                }
                Err(e) => match e {
                    DeltaTransactionError::VersionAlreadyExists { .. }
                        if attempt_number > DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS + 1 =>
                    {
                        debug!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing.", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                        self.log_delta_write_failed().await;
                        return Err(e.into());
                    }
                    DeltaTransactionError::VersionAlreadyExists { .. } => {
                        state.delta_writer.update_table().await?;

                        if self.are_partition_offsets_match(state, &partition_offsets) {
                            attempt_number += 1;
                            debug!("Transaction attempt failed. Incrementing attempt number to {} and retrying.", attempt_number);
                        } else {
                            info!("Transaction attempt failed. Got conflict partition offsets from delta store. Re-seeking consumer");
                            self.reset_writer_and_partition_assignment(state).await?;
                            // todo delete parquet file
                            return Ok(());
                        }
                    }
                    _ => {
                        self.log_delta_write_failed().await;
                        return Err(e.into());
                    }
                },
            }
        }
    }

    fn are_partition_offsets_match(
        &self,
        state: &mut ProcessingState,
        partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> bool {
        for (partition, offset) in partition_offsets {
            let version = state
                .delta_writer
                .last_transaction_version(&self.app_id_for_partition(*partition));

            if let Some(v) = version {
                if v != *offset {
                    return false;
                }
            }
        }
        true
    }

    async fn reset_writer_and_partition_assignment(
        &self,
        state: &mut ProcessingState,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let mut partition_assignment = self.partition_assignment.lock().await;
        let mut map = HashMap::new();
        for partition in partition_assignment.partitions() {
            if let Some(offset) = state
                .delta_writer
                .last_transaction_version(&self.app_id_for_partition(partition))
            {
                map.insert(partition, offset);
            }
        }

        partition_assignment.update_offsets(&map);

        self.update_partition_assignment(state, None).await?;

        Ok(())
    }
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
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);

        match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions = partition_vec_from_topic_partition_list(tpl);
                info!(
                    "REBALANCE - Received new partition assignment list {:?}",
                    partitions
                );

                let partitions = Arc::new(partitions);
                on_rebalance_assign(self.partition_assignment.clone(), partitions);
            }
            Rebalance::Revoke => {
                info!("REBALANCE - Partition assignments revoked");
                on_rebalance_revoke(self.partition_assignment.clone());
            }
            Rebalance::Error(e) => {
                warn!(
                    "REBALANCE - Unexpected Kafka error in post_rebalance invocation {:?}",
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
