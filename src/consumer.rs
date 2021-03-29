use log::{debug, error, info, warn};
use rdkafka::{
    client::ClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaError,
    Message, Offset, TopicPartitionList,
};
use std::collections::HashMap;
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc, Mutex,
};

use crate::DataTypeOffset;
use crate::DataTypePartition;

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
                match self.partition_assignment.lock() {
                    Ok(mut partition_assignment) => {
                        partition_assignment.reset_from_topic_partition_list(tpl);
                    }
                    Err(e) => {
                        error!("Error locking partition_assignment {:?}", e);
                    }
                }
            }
            Rebalance::Revoke => {
                info!("Partition assignments revoked");
                match self.partition_assignment.lock() {
                    Ok(mut partition_offsets) => {
                        partition_offsets.clear();
                    }
                    Err(e) => {
                        error!("Error locking partition_offsets {:?}", e);
                    }
                }
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

impl Context {
    pub fn new(partition_assignment: Arc<Mutex<PartitionAssignment>>) -> Self {
        Self {
            partition_assignment,
        }
    }
}

pub struct PartitionAssignment {
    offsets: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl PartitionAssignment {
    pub fn new() -> Self {
        Self {
            offsets: HashMap::new(),
        }
    }

    pub fn reset_from_topic_partition_list(&mut self, topic_partition_list: &TopicPartitionList) {
        let mut current_partitions = Vec::new();

        for element in topic_partition_list.elements().iter() {
            let partition = element.partition();
            current_partitions.push(partition);
            if !self.offsets.contains_key(&partition) {
                self.offsets.insert(partition, None);
            }
        }

        self.offsets.retain(|k, _| current_partitions.contains(k));
    }

    pub fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.offsets.get_mut(&k) {
                *entry = Some(*v);
            } else {
                warn!("Partition {} is not part of the assignment.", k);
            }
        }
    }

    pub fn partitions(&self) -> Vec<DataTypePartition> {
        self.offsets.keys().map(|k| *k).collect()
    }

    pub fn partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let partition_offsets = self
            .offsets
            .iter()
            .filter_map(|(k, v)| match v {
                Some(o) => Some((*k, *o)),
                None => None,
            })
            .collect();

        partition_offsets
    }

    pub fn clear(&mut self) {
        self.offsets.clear();
    }
}
