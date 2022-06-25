use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance},
    error::{KafkaError, KafkaResult},
    message::OwnedMessage,
    util::Timeout,
    ClientContext, Offset, TopicPartitionList,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    ops::Add,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::settings::{self, IngestOptions};

/// Type alias for Kafka partition
pub type DataTypePartition = i32;
/// Type alias for Kafka message offset
pub type DataTypeOffset = i64;

/// The enum to represent 'auto.offset.reset' options.
#[derive(Clone)]
pub enum AutoOffsetReset {
    /// The "earliest" option. Messages will be ingested from the beginning of a partition on reset.
    Earliest,
    /// The "latest" option. Messages will be ingested from the end of a partition on reset.
    Latest,
}

impl AutoOffsetReset {
    /// The librdkafka config key used to specify an `auto.offset.reset` policy.
    pub const CONFIG_KEY: &'static str = "auto.offset.reset";
}

/// Type alias for the Wrapped rdkafka consumer.
type WrappedConsumer = BaseConsumer<IngestContext>;

/// Represents the rebalance state after the most recent poll.
#[derive(Clone, Debug)]
pub enum RebalanceState {
    /// The application has not received an initial assignment so no messages should be processed.
    Uninitialized,
    /// Some partitions have been revoked.
    Revoked(Vec<DataTypePartition>),
    /// New partitions have been assigned.
    Assigned(Vec<DataTypePartition>),
}

/// A result which includes a message if rebalance is not in flight, or
/// a rebalance state that needs to be handled before continuing to process messages.
pub enum MessagePollResult {
    Message(OwnedMessage),
    RebalanceState(RebalanceState),
}

///
pub struct IngestConsumer {
    topic: String,
    consumer: WrappedConsumer,
}

impl IngestConsumer {
    pub fn new(client_config: ClientConfig, topic: &str) -> Result<IngestConsumer, KafkaError> {
        let context = IngestContext {
            rebalance_state: Arc::new(RwLock::new(Some(RebalanceState::Uninitialized))),
        };
        let consumer: BaseConsumer<IngestContext> = client_config.create_with_context(context)?;

        Ok(Self {
            topic: topic.to_string(),
            consumer,
        })
    }

    /// Subscribes to the Kafka topic.
    pub fn subscribe(&self) -> KafkaResult<()> {
        self.consumer.subscribe(&[&self.topic])
    }

    /// Polls the Kafka topic.
    pub fn poll(&self) -> Option<Result<MessagePollResult, KafkaError>> {
        let poll_option = self.consumer.poll(Timeout::After(Duration::from_secs(
            settings::DEFAULT_POLL_TIMEOUT,
        )));

        let rebalance_state = self.consumer.context().rebalance_state.read().unwrap();
        if rebalance_state.is_some() {
            Some(Ok(MessagePollResult::RebalanceState(
                rebalance_state.to_owned().unwrap(),
            )))
        } else {
            poll_option.map(|result| result.map(|m| MessagePollResult::Message(m.detach())))
        }
    }

    /// Sets rebalance state to [`None`].
    pub fn clear_rebalance_state(&self) {
        let mut inner = self.consumer.context().rebalance_state.write().unwrap();
        *inner = None;
    }

    /// Seeks the Kafka consumer to the appropriate offsets based on the [`PartitionAssignment`].
    pub fn seek(
        &self,
        partition_assignment: &PartitionAssignment,
        auto_offset_reset: &AutoOffsetReset,
    ) -> Result<(), KafkaError> {
        let mut log_message = String::new();

        for (p, offset) in partition_assignment.assignment() {
            match offset {
                Some(o) if *o == 0 => {
                    // MARK: workaround for rdkafka error when attempting seek to offset 0
                    log::info!("Seeking consumer to beginning for partition {}. Delta log offset is 0, but seek to zero is not possible.", p);
                    self.consumer
                        .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                }
                Some(o) => {
                    self.consumer
                        .seek(&self.topic, *p, Offset::Offset(*o), Timeout::Never)?;

                    log_message = log_message.add(format!("{}:{},", p, o).as_str());
                }
                None => match auto_offset_reset {
                    AutoOffsetReset::Earliest => {
                        log::info!("Seeking consumer to beginning for partition {}. Partition has no stored offset but 'auto.offset.reset' is earliest", p);
                        self.consumer
                            .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                    }
                    AutoOffsetReset::Latest => {
                        log::info!("Seeking consumer to end for partition {}. Partition has no stored offset but 'auto.offset.reset' is latest", p);
                        self.consumer
                            .seek(&self.topic, *p, Offset::End, Timeout::Never)?;
                    }
                },
            };
        }
        if !log_message.is_empty() {
            log::info!("Seeking consumer to partition offsets: [{}]", log_message);
        }
        Ok(())
    }

    /// Calculates lag for all partitions in the given list of partition offsets.
    pub fn calculate_lag(
        &self,
        partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Result<Vec<DataTypeOffset>, KafkaError> {
        let high_watermarks = self.get_high_watermarks(partition_offsets.keys().copied())?;
        let lags = partition_offsets
            .iter()
            .zip(high_watermarks.iter())
            .map(|((_, offset), high_watermark_offset)| high_watermark_offset - offset)
            .collect();

        Ok(lags)
    }

    /// Fetches high watermarks (latest offsets) from Kafka from the iterator of partitions.
    fn get_high_watermarks<I>(&self, partitions: I) -> Result<Vec<i64>, KafkaError>
    where
        I: Iterator<Item = DataTypePartition>,
    {
        partitions
            .map(|partition| {
                self.consumer
                    .fetch_watermarks(self.topic.as_str(), partition, Timeout::Never)
                    .map(|(_, latest_offset)| latest_offset)
            })
            .collect()
    }
}

struct IngestContext {
    rebalance_state: Arc<RwLock<Option<RebalanceState>>>,
}

impl ClientContext for IngestContext {}

impl ConsumerContext for IngestContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                log::info!("PRE_REBALANCE - Revoke {:?}", tpl);
                let partitions = partition_vec_from_topic_partition_list(tpl);
                let mut inner = self.rebalance_state.write().unwrap();
                *inner = Some(RebalanceState::Revoked(partitions));
            }
            Rebalance::Assign(tpl) => {
                log::info!("PRE_REBALANCE - Assign {:?}", tpl);
            }
            Rebalance::Error(e) => {
                panic!("PRE_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                log::info!("POST_REBALANCE - Revoke {:?}", tpl);
            }
            Rebalance::Assign(tpl) => {
                log::info!("POST_REBALANCE - Assign {:?}", tpl);
                let partitions = partition_vec_from_topic_partition_list(tpl);
                let mut inner = self.rebalance_state.write().unwrap();
                *inner = Some(RebalanceState::Assigned(partitions));
            }
            Rebalance::Error(e) => {
                panic!("POST_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }
}

/// Contains the partition to offset map for all partitions assigned to the consumer.
#[derive(Default)]
pub struct PartitionAssignment {
    assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl PartitionAssignment {
    /// Resets the [`PartitionAssignment`] with a new list of partitions.
    /// Offsets are set as [`None`] for all partitions.
    pub fn reset_with(&mut self, partitions: &[DataTypePartition]) {
        self.assignment.clear();
        for p in partitions {
            self.assignment.insert(*p, None);
        }
    }

    /// Updates the offset for one partition.
    pub fn update_offset(&mut self, partition: DataTypePartition, offset: Option<DataTypeOffset>) {
        self.assignment.insert(partition, offset);
    }

    /// Updates the offsets for each partition.
    pub fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(k) {
                *entry = Some(*v);
            }
        }
    }

    /// Returns the full list of assigned partitions as a [`Vec`] whether offsets are recorded for them in-memory or not.
    pub fn assigned_partitions(&self) -> Vec<DataTypePartition> {
        self.assignment.keys().copied().collect()
    }

    /// Returns a copy of the current partition offsets as a [`HashMap`] for all partitions that have an offset stored in memory.
    /// Partitions that do not have an offset stored in memory (offset is [`None`]) are **not** included in the returned HashMap.
    pub fn nonempty_partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let partition_offsets = self
            .assignment
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|o| (*k, *o)))
            .collect();

        partition_offsets
    }

    /// Returns an [`Iterator`] of tuples representing the current assignment, where each tuple names a unique assigned partition and
    /// the last stored offset for the partition.
    pub fn assignment(
        &self,
    ) -> impl Iterator<Item = (&DataTypePartition, &Option<DataTypeOffset>)> {
        self.assignment.iter()
    }
}

/// Creates an rdkafka [`ClientConfig`] from the provided [`IngestOptions`].
pub fn config_from_options(opts: &IngestOptions) -> ClientConfig {
    let mut kafka_client_config = ClientConfig::new();
    if let Ok(cert_pem) = std::env::var("KAFKA_DELTA_INGEST_CERT") {
        kafka_client_config.set("ssl.certificate.pem", cert_pem);
    }
    if let Ok(key_pem) = std::env::var("KAFKA_DELTA_INGEST_KEY") {
        kafka_client_config.set("ssl.key.pem", key_pem);
    }
    if let Ok(scram_json) = std::env::var("KAFKA_DELTA_INGEST_SCRAM_JSON") {
        let value: Value = serde_json::from_str(scram_json.as_str())
            .expect("KAFKA_DELTA_INGEST_SCRAM_JSON should be valid JSON");

        let username = value["username"]
            .as_str()
            .expect("'username' must be present in KAFKA_DELTA_INGEST_SCRAM_JSON");
        let password = value["password"]
            .as_str()
            .expect("'password' must be present in KAFKA_DELTA_INGEST_SCRAM_JSON");

        kafka_client_config.set("sasl.username", username);
        kafka_client_config.set("sasl.password", password);
    }

    let auto_offset_reset = match opts.auto_offset_reset {
        AutoOffsetReset::Earliest => "earliest",
        AutoOffsetReset::Latest => "latest",
    };
    kafka_client_config.set(AutoOffsetReset::CONFIG_KEY, auto_offset_reset);

    if let Some(additional) = &opts.additional_kafka_settings {
        for (k, v) in additional.iter() {
            kafka_client_config.set(k, v);
        }
    }
    kafka_client_config
        .set("bootstrap.servers", opts.kafka_brokers.clone())
        .set("group.id", opts.consumer_group_id.clone())
        .set("enable.auto.commit", "false");

    kafka_client_config
}

/// Creates a vec of partitions from a topic partition list.
fn partition_vec_from_topic_partition_list(
    topic_partition_list: &TopicPartitionList,
) -> Vec<DataTypePartition> {
    topic_partition_list
        .to_topic_map()
        .iter()
        .map(|((_, p), _)| *p)
        .collect()
}
