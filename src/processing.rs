use deltalake::{
    storage::s3::dynamodb_lock::DynamoError, DeltaTable, DeltaTableError, StorageError,
};
use log::{debug, error, info, warn};
use rdkafka::{message::OwnedMessage, Message};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{
    coercions::{self, CoercionTree},
    dead_letters::{DeadLetter, DeadLetterQueue},
    delta_helpers,
    errors::{
        DataWriterError, DeadLetterQueueError, MessageDeserializationError, MessageProcessingError,
        ProcessingError, TimerError, TransformError, WriteOffsetsError,
    },
    kafka::{
        AutoOffsetReset, DataTypeOffset, DataTypePartition, IngestConsumer, MessagePollResult,
        PartitionAssignment, RebalanceState,
    },
    metrics::IngestMetrics,
    offsets,
    settings::{self, IngestOptions, SeekOffsets, DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS},
    transforms::Transformer,
    util::Timer,
    value_buffers::{ConsumedBuffers, ValueBuffers},
    writer::DataWriter,
};

type DeltaOffsets = HashMap<DataTypePartition, Option<DataTypeOffset>>;

struct ProcessingConfig {
    topic: String,
    app_id: String,
    allowed_latency: u64,
    max_messages_per_batch: usize,
    min_bytes_per_file: usize,
    write_checkpoints: bool,
    auto_offset_reset: AutoOffsetReset,
    seek_offsets: Option<SeekOffsets>,
}

impl ProcessingConfig {
    fn new(topic: String, ingest_options: &IngestOptions) -> Self {
        Self {
            topic,
            app_id: ingest_options.app_id.clone(),
            allowed_latency: ingest_options.allowed_latency,
            max_messages_per_batch: ingest_options.max_messages_per_batch,
            min_bytes_per_file: ingest_options.min_bytes_per_file,
            write_checkpoints: ingest_options.write_checkpoints,
            auto_offset_reset: ingest_options.auto_offset_reset.clone(),
            seek_offsets: ingest_options.seek_offsets.clone(),
        }
    }
}

struct ProcessingState {
    value_buffers: ValueBuffers,
    partition_assignment: PartitionAssignment,
    delta_offsets: DeltaOffsets,
}

/// Processes a stream of messages consumed from Kafka and writes them to a delta table.
pub struct Processor {
    config: ProcessingConfig,
    state: ProcessingState,
    consumer: IngestConsumer,
    transformer: Transformer,
    coercion_tree: CoercionTree,
    table: DeltaTable,
    writer: DataWriter,
    dlq: Box<dyn DeadLetterQueue>,
    metrics: IngestMetrics,
    cancellation_token: Arc<CancellationToken>,
}

impl Processor {
    /// Constructs a new instance of [`Processor`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: String,
        consumer: IngestConsumer,
        table: DeltaTable,
        delta_writer: DataWriter,
        dlq: Box<dyn DeadLetterQueue>,
        transformer: Transformer,
        coercion_tree: CoercionTree,
        metrics: IngestMetrics,
        cancellation_token: Arc<CancellationToken>,
        options: &IngestOptions,
    ) -> Self {
        let config = ProcessingConfig::new(topic, options);

        let state = ProcessingState {
            value_buffers: ValueBuffers::default(),
            partition_assignment: PartitionAssignment::default(),
            delta_offsets: HashMap::new(),
        };

        Self {
            config,
            state,
            consumer,
            table,
            writer: delta_writer,
            dlq,
            transformer,
            coercion_tree,
            metrics,
            cancellation_token,
        }
    }

    /// Starts a process to ingest messages received on a Kafka topic to a delta table.
    /// Stops when the cancellation_token held by the [`Processor`] is cancelled.
    pub async fn start(&mut self) -> Result<(), ProcessingError> {
        self.initialize_delta_offsets().await?;

        let mut write_timer = Timer::new(Duration::from_secs(self.config.allowed_latency));
        let mut lag_report_timer =
            Timer::new_from_now(Duration::from_secs(settings::DEFAULT_LAG_REPORT_SECONDS));
        let mut consumed = 0usize;

        self.consumer.subscribe()?;

        loop {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            match self.consumer.poll() {
                Some(Ok(MessagePollResult::RebalanceState(rebalance_state))) => {
                    match rebalance_state {
                        RebalanceState::Uninitialized | RebalanceState::Revoked(_) => {
                            continue;
                        }
                        RebalanceState::Assigned(new_partitions) => {
                            info!("Resetting state for assigned partitions.");
                            self.state
                                .partition_assignment
                                .reset_with(new_partitions.as_slice());

                            self.reset_state()?;
                            self.consumer.clear_rebalance_state();
                        }
                    }
                }
                Some(Ok(MessagePollResult::Message(message))) => {
                    if consumed == 0 {
                        write_timer.reset();
                    }
                    consumed += 1;
                    match self.process_message(&message) {
                        Err(MessageProcessingError::AlreadyProcessedOffset {
                            partition: _,
                            offset: _,
                        }) => {
                            continue;
                        }
                        Err(MessageProcessingError::Deserialization { source }) => {
                            self.handle_deserialization_err(&message, source).await?;
                        }
                        Err(MessageProcessingError::Transform { value, error }) => {
                            self.handle_transform_err(&message, &value, error).await?;
                        }
                        Ok(()) => {}
                    }
                }
                Some(Err(e)) => {
                    return Err(e.into());
                }
                None => {
                    debug!("No message or rebalance received on poll.");
                }
            }

            if self.should_complete_record_batch(&write_timer, self.state.value_buffers.len())? {
                let operation_timer = Instant::now();
                self.metrics.batch_started();
                match self.complete_record_batch() {
                    Err(DataWriterError::PartialParquetWrite {
                        skipped_values,
                        sample_error,
                    }) => {
                        warn!(
                            "Partial parquet write, skipped {} values, sample ParquetError {:?}",
                            skipped_values.len(),
                            sample_error
                        );
                        let dead_letters = DeadLetter::vec_from_failed_parquet_rows(skipped_values);
                        self.dlq.write_dead_letters(dead_letters).await?;
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                    Ok(()) => {}
                }
                self.metrics
                    .batch_completed(self.writer.buffered_record_batch_count(), &operation_timer);
            }

            if self.should_complete_file(&write_timer, self.writer.buffer_len())? {
                let operation_timer = Instant::now();
                self.metrics.delta_write_started();
                match self.complete_file().await {
                    Err(DataWriterError::ConflictingOffsets) => {
                        let _ = self.reset_state()?;
                        continue;
                    }
                    Err(DataWriterError::DeltaSchemaChanged) => {
                        let _ = self.rebuild_coercion_tree()?;
                        let _ = self.reset_state()?;
                        continue;
                    }
                    Err(e) => {
                        self.metrics.delta_write_failed();
                        return Err(e.into());
                    }
                    Ok(v) => {
                        info!(
                            "Delta version {} completed in {} milliseconds.",
                            v,
                            operation_timer.elapsed().as_millis()
                        );
                        write_timer.reset();
                    }
                };
                self.metrics.delta_write_completed(&operation_timer);
            }

            if lag_report_timer.is_expired()? {
                self.metrics.buffer_lag(self.consumer.calculate_lag(
                    &self.state.partition_assignment.nonempty_partition_offsets(),
                )?);
                self.metrics.delta_lag(self.consumer.calculate_lag(
                    &self.state.partition_assignment.nonempty_partition_offsets(),
                )?);

                lag_report_timer.reset();
            }
        }
    }

    /// Processes a single message received from Kafka.
    /// This method deserializes, transforms and writes the message to buffers.
    fn process_message(&mut self, message: &OwnedMessage) -> Result<(), MessageProcessingError> {
        let partition = message.partition();
        let offset = message.offset();

        if !self.should_process_offset(partition, offset) {
            return Err(MessageProcessingError::AlreadyProcessedOffset { partition, offset });
        }

        // deserialize the message into a `serde_json::Value`
        match self.deserialize_message(message) {
            Ok(mut value) => {
                self.metrics.message_deserialized();
                // transform the message
                match self.transformer.transform(&mut value, Some(message)) {
                    Ok(()) => {
                        self.metrics.message_transformed();
                        // coerce data types
                        coercions::coerce(&mut value, &self.coercion_tree);
                        // buffer the message
                        self.state.value_buffers.add(partition, offset, value)?;
                    }
                    Err(e) => {
                        return Err(MessageProcessingError::Transform {
                            value: value.to_owned(),
                            error: e,
                        });
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    /// Deserializes a message received from Kafka
    fn deserialize_message<M>(&self, msg: &M) -> Result<Value, MessageDeserializationError>
    where
        M: Message + Send + Sync,
    {
        // Deserialize the rdkafka message into a serde_json::Value
        let message_bytes = match msg.payload() {
            Some(bytes) => bytes,
            None => {
                return Err(MessageDeserializationError::EmptyPayload);
            }
        };

        self.metrics.message_deserialized_size(message_bytes.len());

        let value: Value = match serde_json::from_slice(message_bytes) {
            Ok(v) => v,
            Err(e) => {
                return Err(MessageDeserializationError::JsonDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(message_bytes, e),
                });
            }
        };

        Ok(value)
    }

    /// Writes the transformed messages currently held in buffer to parquet byte buffers.
    fn complete_record_batch(&mut self) -> Result<(), DataWriterError> {
        let ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts: _,
        } = self.state.value_buffers.consume();
        self.state
            .partition_assignment
            .update_offsets(&partition_offsets);

        // don't complete the batch if there aren't any values in buffer.
        if values.is_empty() {
            return Ok(());
        }

        self.writer.write(values)
    }

    /// Write parquet to a file in the destination delta table and create a delta transaction.
    async fn complete_file(&mut self) -> Result<i64, DataWriterError> {
        // reset the latency timer to track allowed latency for the next file
        let partition_offsets = self.state.partition_assignment.nonempty_partition_offsets();

        // upload pending parquet file to delta store
        // TODO: remove it if we got conflict error? or it'll be considered as tombstone
        let add_action = self
            .writer
            .write_parquet_files(&self.table.table_uri)
            .await?;

        // file size metrics
        for a in add_action.iter() {
            self.metrics.delta_file_size(a.size);
        }

        // delta commit
        let mut attempt_number: u32 = 0;
        let prepared_commit = {
            let mut tx = self.table.create_transaction(None);
            tx.add_actions(delta_helpers::build_actions(
                &partition_offsets,
                &self.config.app_id,
                add_action,
            ));
            tx.prepare_commit(None).await?
        };

        loop {
            self.table.update().await?;

            if !self.are_partition_offsets_match() {
                return Err(DataWriterError::ConflictingOffsets);
            }

            if self.writer.update_schema(self.table.get_metadata()?)? {
                info!("Table schema has been updated");
                self.rebuild_coercion_tree()?;
                return Err(DataWriterError::DeltaSchemaChanged);
            }

            let version = self.table.version + 1;
            let commit_result = self
                .table
                .try_commit_transaction(&prepared_commit, version)
                .await;

            match commit_result {
                Ok(v) => {
                    if v != version {
                        return Err(DataWriterError::UnexpectedVersionMismatch {
                            expected_version: version,
                            actual_version: v,
                        });
                    }

                    assert_eq!(v, version);

                    for (p, o) in &partition_offsets {
                        self.state.delta_offsets.insert(*p, Some(*o));
                    }

                    if self.config.write_checkpoints {
                        delta_helpers::try_create_checkpoint(&mut self.table, version).await?;
                    }

                    return Ok(version);
                }
                Err(e) => match e {
                    DeltaTableError::VersionAlreadyExists(_)
                        if attempt_number > DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS + 1 =>
                    {
                        error!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                        return Err(e.into());
                    }
                    DeltaTableError::VersionAlreadyExists(_) => {
                        attempt_number += 1;
                        warn!("Transaction attempt failed. Incrementing attempt number to {} and retrying", attempt_number);
                    }
                    DeltaTableError::StorageError {
                        source:
                            StorageError::DynamoDb {
                                source: DynamoError::NonAcquirableLock,
                            },
                    } => {
                        error!("Delta write failed.. DeltaTableError: {}", e);
                        return Err(DataWriterError::InconsistentState(
                            "The remote dynamodb lock is non-acquirable!".to_string(),
                        ));
                    }
                    _ => {
                        return Err(e.into());
                    }
                },
            }
        }
    }

    /// Rebuilds and replaces the coercion tree from the current schema.
    fn rebuild_coercion_tree(&mut self) -> Result<(), DeltaTableError> {
        let coercion_tree = coercions::create_coercion_tree(&self.table.get_metadata()?.schema);
        let _ = std::mem::replace(&mut self.coercion_tree, coercion_tree);

        Ok(())
    }

    /// Resets all current state to the correct starting points and seeks the consumer.
    fn reset_state(&mut self) -> Result<(), ProcessingError> {
        self.state.value_buffers.reset();
        self.state.delta_offsets.clear();
        self.writer.reset();

        // update offsets stored in PartitionAssignment to the latest from the delta log
        let partitions = self.state.partition_assignment.assigned_partitions();
        for partition in partitions.iter() {
            let txn_app_id =
                delta_helpers::txn_app_id_for_partition(&self.config.app_id, *partition);
            let version = delta_helpers::last_txn_version(&self.table, &txn_app_id);
            self.state
                .partition_assignment
                .update_offset(*partition, version);
            self.state.delta_offsets.insert(*partition, version);
        }

        self.consumer.seek(
            &self.state.partition_assignment,
            &self.config.auto_offset_reset,
        )?;

        Ok(())
    }

    /// Returns true if the message at `partition`:`offset` should be processed.
    fn should_process_offset(&self, partition: DataTypePartition, offset: DataTypeOffset) -> bool {
        if let Some(Some(written_offset)) = self.state.delta_offsets.get(&partition) {
            if offset <= *written_offset {
                debug!(
                    "Message with partition {} offset {} on topic {} is already in delta log so skipping.",
                    partition, offset, &self.config.topic
                );
                return false;
            }
        }
        true
    }

    /// Returns true if a record batch should be completed.
    fn should_complete_record_batch(
        &self,
        timer: &Timer,
        buffered_messages: usize,
    ) -> Result<bool, TimerError> {
        should_complete(timer, buffered_messages, self.config.max_messages_per_batch)
    }

    /// Returns true if a file should be written.
    fn should_complete_file(
        &self,
        timer: &Timer,
        buffered_bytes: usize,
    ) -> Result<bool, TimerError> {
        should_complete(timer, buffered_bytes, self.config.min_bytes_per_file)
    }

    /// Returns a bool indicating whether the partition offsets currently held in memory
    /// match those stored in the delta log.
    fn are_partition_offsets_match(&self) -> bool {
        let mut result = true;
        for (partition, offset) in &self.state.delta_offsets {
            let version = delta_helpers::last_txn_version(
                &self.table,
                &delta_helpers::txn_app_id_for_partition(&self.config.app_id, *partition),
            );

            if let Some(version) = version {
                match offset {
                    Some(offset) if *offset == version => (),
                    _ => {
                        info!(
                            "Conflicting offset for partition {}: offset={:?}, delta={}",
                            partition, offset, version
                        );
                        result = false;
                    }
                }
            }
        }
        result
    }

    /// If seek offsets are provided in options, writes these to the delta log.
    async fn initialize_delta_offsets(&mut self) -> Result<(), WriteOffsetsError> {
        if let Some(ref offsets) = self.config.seek_offsets {
            offsets::write_offsets_to_delta(&mut self.table, &self.config.app_id, offsets).await?;
        }
        Ok(())
    }

    async fn handle_deserialization_err(
        &mut self,
        m: &OwnedMessage,
        e: MessageDeserializationError,
    ) -> Result<(), DeadLetterQueueError> {
        match e {
            MessageDeserializationError::EmptyPayload => {
                warn!(
                    "Message has empty payload - partition {}, offset {}",
                    m.partition(),
                    m.offset()
                );
                self.metrics.message_deserialization_failed();
                Ok(())
            }
            MessageDeserializationError::JsonDeserialization { dead_letter } => {
                warn!(
                    "Message deserialization failed - partition {}, offset {}",
                    m.partition(),
                    m.offset()
                );
                self.metrics.message_transform_failed();
                self.dlq.write_dead_letter(dead_letter).await
            }
        }
    }

    async fn handle_transform_err(
        &mut self,
        m: &OwnedMessage,
        v: &Value,
        e: TransformError,
    ) -> Result<(), DeadLetterQueueError> {
        warn!(
            "Transform failed - partition {}, offset {}",
            m.partition(),
            m.offset()
        );
        self.metrics.message_transform_failed();

        self.dlq
            .write_dead_letter(DeadLetter::from_failed_transform(v, e))
            .await
    }
}

fn should_complete(timer: &Timer, items: usize, threshold: usize) -> Result<bool, TimerError> {
    if items == 0 {
        return Ok(false);
    }

    if items >= threshold {
        return Ok(true);
    }

    timer.is_expired()
}
