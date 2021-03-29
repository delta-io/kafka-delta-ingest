use deltalake::DeltaDataTypeVersion;
use log::{debug, error, info, warn};
use rdkafka::{error::KafkaError, Offset, TopicPartitionList};
use serde_json::Value;
use std::collections::HashMap;

use crate::DataTypeOffset;
use crate::DataTypePartition;

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
            .or_insert_with(|| ValueBuffer::new(partition));
        buffer.add(value, offset);
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        self.buffers
            .iter()
            .filter_map(|(k, v)| {
                if v.non_empty() {
                    Some((k.clone(), v.last_offset.unwrap()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn consume(&mut self) -> ConsumedBuffers {
        let mut partition_offsets = HashMap::new();

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| {
                if buffer.non_empty() {
                    let (values, offset) = buffer.consume();
                    partition_offsets.insert(partition.clone(), offset);
                    Some(values)
                } else {
                    None
                }
            })
            .flatten()
            .collect();

        ConsumedBuffers {
            values,
            partition_offsets,
        }
    }

    pub fn consume_or_drop_partitions(
        &mut self,
        partitions: &[DataTypePartition],
    ) -> ConsumedBuffers {
        let mut partition_offsets = HashMap::new();
        let mut partitions_to_drop: Vec<DataTypePartition> = Vec::new();

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| {
                if buffer.non_empty() && partitions.contains(partition) {
                    let (values, offset) = buffer.consume();
                    partition_offsets.insert(partition.clone(), offset);
                    Some(values)
                } else {
                    partitions_to_drop.push(partition.clone());
                    None
                }
            })
            .flatten()
            .collect();

        for p in partitions_to_drop {
            self.drop_partition(p);
        }

        ConsumedBuffers {
            values,
            partition_offsets,
        }
    }

    pub fn drop_partition(&mut self, partition: DataTypePartition) {
        let buffer = self.buffers.remove(&partition);

        if let Some(b) = buffer {
            self.len = self.len - b.values.len();
        }
    }
}

pub struct ConsumedBuffers {
    pub values: Vec<Value>,
    pub partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
}

// pub struct BufferedMessage {
//     pub value: Value,
//     pub offset: DataTypeOffset,
// }

// impl BufferedMessage {
//     fn new(offset: DataTypeOffset, value: Value) -> Self {
//         Self { offset, value }
//     }
// }

pub struct ValueBuffer {
    partition: DataTypePartition,
    last_offset: Option<DataTypeOffset>,
    values: Vec<Value>,
}

impl ValueBuffer {
    fn new(partition: DataTypePartition) -> Self {
        Self {
            partition,
            last_offset: None,
            values: Vec::new(),
        }
    }

    fn add(&mut self, value: Value, offset: DataTypeOffset) {
        self.last_offset = Some(offset);
        self.values.push(value);
    }

    fn consume(&mut self) -> (Vec<Value>, DataTypeOffset) {
        let consumed = (self.values.drain(0..).collect(), self.last_offset.unwrap());
        self.last_offset = None;
        consumed
    }

    fn non_empty(&self) -> bool {
        self.last_offset != None
    }
}
