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
    transformer: Transformer,
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
        let transformer = Transformer::from_transforms(&transforms)?;

        let consumer: StreamConsumer<Context> =
            kafka_client_config.create_with_context(consumer_context)?;

        Ok(Self {
            opts,
            transformer,
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

    async fn process_message(
        &self,
        state: &mut ProcessingState,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), ProcessingError> {
        self.check_rebalance_event(state).await?;
        self.check_message_tracking(state, msg)?;

        let mut value = self.deserialize_message(msg).await?;
        self.transform_value(&mut value, msg).await?;

        state
            .value_buffers
            .add(msg.partition(), msg.offset(), value);

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
        value: &mut Value,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), ProcessingError> {
        // Transform
        // TODO: Add better transform failure handling, ideally send to a dlq
        match self.transformer.transform(value, msg) {
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

    async fn seek_consumer(
        &self,
        partition_offsets: &HashMap<DataTypePartition, Option<DataTypeOffset>>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        for (p, v) in partition_offsets.iter() {
            match v {
                Some(offset) => {
                    info!("Seeking consumer to {}:{}", p, offset);
                    self.consumer.seek(
                        &self.opts.topic,
                        *p,
                        Offset::Offset(*offset),
                        Timeout::Never,
                    )?;
                }
                _ => {
                    info!(
                        "Partition {} has no recorded offset. Not seeking consumer.",
                        p
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
            latency_timer: Instant::now(),
            delta_partition_offsets: HashMap::new(),
        };

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

    /// Check and seek consumer if rebalance has happened
    async fn check_rebalance_event(
        &self,
        state: &mut ProcessingState,
    ) -> Result<(), ProcessingError> {
        let mut partition_assignment = self.partition_assignment.lock().await;

        // TODO if the new assignment is only an addition of partitions we don't need to reset state
        if partition_assignment.rebalance.is_some() {
            let partitions = partition_assignment.rebalance.as_ref().unwrap().clone();

            partition_assignment.reset_with(&partitions);
            self.reset_state(state, &mut partition_assignment).await?;

            // clear `rebalance` list only if reset state is successful
            partition_assignment.rebalance = None;
        }

        Ok(())
    }

    fn check_message_tracking(
        &self,
        state: &mut ProcessingState,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), ProcessingError> {
        if let Some(offset) = state.delta_partition_offsets.get(&msg.partition()) {
            if *offset > msg.offset() {
                // This message was consumed after rebalance but before the seek.
                // Then next consumption will get us the messages with expected offsets
                return Err(ProcessingError::Continue);
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
            state.delta_writer.update_table().await?;

            if !self.are_partition_offsets_match(state) {
                debug!("Transaction attempt failed. Got conflict partition offsets from delta store. Resetting consumer");
                self.reset_state_guarded(state).await?;
                // todo delete parquet file
                return Ok(());
            }

            let version = state.delta_writer.table_version() + 1;
            let actions = self.build_actions(&partition_offsets, add.clone()).await;
            let commit_result = state.delta_writer.commit_version(version, actions).await;

            match commit_result {
                Ok(v) => {
                    assert_eq!(v, version);

                    state.delta_partition_offsets = partition_offsets;

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
                        attempt_number += 1;
                        debug!("Transaction attempt failed. Incrementing attempt number to {} and retrying.", attempt_number);
                    }
                    _ => {
                        self.log_delta_write_failed().await;
                        return Err(e.into());
                    }
                },
            }
        }
    }

    /// Checks whether partition offsets from last writes matches the ones from delta log
    /// If not then other consumers already processed them.
    fn are_partition_offsets_match(&self, state: &mut ProcessingState) -> bool {
        let mut result = true;
        for (partition, offset) in &state.delta_partition_offsets {
            let version = state
                .delta_writer
                .last_transaction_version(&self.app_id_for_partition(*partition));

            if let Some(version) = version {
                // if messages in kafka are consecutive then offset should always be `version+1` for
                // safe commit, but since kafka does not guarantee contiguous offsets, we only check for `less than`.
                if *offset != version {
                    info!(
                        "Conflict offset for partition {}: state={:?}, delta={:?}",
                        partition, offset, version
                    );
                    result = false;
                }
            }
        }
        result
    }

    async fn reset_state_guarded(
        &self,
        state: &mut ProcessingState,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let mut partition_assignment = self.partition_assignment.lock().await;
        self.reset_state(state, &mut partition_assignment).await?;
        Ok(())
    }

    async fn reset_state(
        &self,
        state: &mut ProcessingState,
        partition_assignment: &mut PartitionAssignment,
    ) -> Result<(), KafkaJsonToDeltaError> {
        state.delta_writer.reset()?;
        state.value_buffers.reset();
        state.delta_partition_offsets.clear();

        // assuming that partition_assignment has correct partitions as keys
        let partitions: Vec<DataTypePartition> =
            partition_assignment.assignment.keys().map(|p| *p).collect();

        info!("Resetting state with partitions: {:?}", &partitions);

        // update offsets to the latest from the delta log
        for partition in partitions.iter() {
            let version = state
                .delta_writer
                .last_transaction_version(&self.app_id_for_partition(*partition));

            if let Some(version) = version {
                // transaction version in delta log is the latest offset stored for partition,
                // so we need to seek to the incremented to get the next messages.
                let offset = version + 1;
                partition_assignment
                    .assignment
                    .insert(*partition, Some(offset));

                // here we store `version` to remember the latest offset in delta
                // for a conflict resolution
                state.delta_partition_offsets.insert(*partition, version);
            }
        }

        // re seek consumer to the latest offsets of the partition assignment
        self.seek_consumer(&partition_assignment.assignment).await?;

        Ok(())
    }
}

/// Processing state, contains mutable data within run_loop
struct ProcessingState {
    delta_writer: DeltaWriter,
    value_buffers: ValueBuffers,
    latency_timer: Instant,
    delta_partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
}

#[derive(thiserror::Error, Debug)]
pub enum ValueBufferError {
    #[error("Illegal tracked buffer state for partition {partition}.")]
    IllegalTrackedBufferState { partition: DataTypePartition },
}

pub struct ValueBuffers {
    buffers: HashMap<DataTypePartition, ValueBuffer>,
    len: usize,
}

impl ValueBuffers {
    pub fn new() -> Self {
        Self {
            buffers: HashMap::new(),
            len: 0,
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

    pub fn reset(&mut self) {
        self.len = 0;
        self.buffers.clear();
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

                PartitionAssignment::on_rebalance_assign(
                    self.partition_assignment.clone(),
                    partitions,
                );
            }
            Rebalance::Revoke => {
                info!("REBALANCE - Partition assignments revoked");

                PartitionAssignment::on_rebalance_revoke(self.partition_assignment.clone());
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

impl Context {
    pub fn new(partition_assignment: Arc<Mutex<PartitionAssignment>>) -> Self {
        Self {
            partition_assignment,
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

/// Contains the partition to offset assignment for a consumer.
pub struct PartitionAssignment {
    /// The `None` offset for a partition means that it has never been consumed by
    /// application before.
    assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,

    /// The `rebalance` is a new partition assigment that has been set after rebalance event.
    /// Since rebalance event happens asynchronously to the run_loop we only mark the new partitions
    /// so the consumer will change/seek whenever it's suitable for him to avoid conflicts.
    rebalance: Option<Vec<DataTypePartition>>,
}

impl PartitionAssignment {
    pub fn new() -> Self {
        Self {
            assignment: HashMap::new(),
            rebalance: None,
        }
    }

    pub fn on_rebalance_assign(
        partition_assignment: Arc<Mutex<PartitionAssignment>>,
        partitions: Vec<DataTypePartition>,
    ) {
        let _ = tokio::spawn(async move {
            let mut pa = partition_assignment.lock().await;
            pa.rebalance = Some(partitions);
        });
    }

    pub fn on_rebalance_revoke(partition_assignment: Arc<Mutex<PartitionAssignment>>) {
        let _ = tokio::spawn(async move {
            let mut pa = partition_assignment.lock().await;
            pa.rebalance = Some(Vec::new());
        });
    }

    /// Resets this assignment with new list of partitions.
    ///
    /// Note that this should be called only within loop on the executing thread.
    fn reset_with(&mut self, partitions: &Vec<DataTypePartition>) {
        self.assignment.clear();
        for p in partitions {
            self.assignment.insert(*p, None);
        }
    }

    fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(k) {
                *entry = Some(*v);
            } else {
                warn!("Partition {} is not part of the assignment.", k);
            }
        }
    }

    fn partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
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
}
