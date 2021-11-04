use crate::{DataTypeOffset, DataTypePartition};
use serde_json::Value;
use std::collections::HashMap;

/// Provides a single interface into the multiple [`ValueBuffer`] instances used to buffer data for each assigned partition.
#[derive(Debug)]
pub(crate) struct ValueBuffers {
    buffers: HashMap<DataTypePartition, ValueBuffer>,
    len: usize,
}

impl Default for ValueBuffers {
    fn default() -> Self {
        Self {
            buffers: HashMap::new(),
            len: 0,
        }
    }
}

impl ValueBuffers {
    /// Adds a value to in-memory buffers and tracks the partition and offset.
    pub(crate) fn add(
        &mut self,
        partition: DataTypePartition,
        offset: DataTypeOffset,
        value: Value,
    ) {
        let buffer = self
            .buffers
            .entry(partition)
            .or_insert_with(ValueBuffer::new);
        buffer.add(value, offset);
        self.len += 1;
    }

    /// Returns the total number of items stored across each partition specific [`ValueBuffer`].
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns values, partition offsets and partition counts currently held in buffer and resets buffers to empty.
    pub(crate) fn consume(&mut self) -> ConsumedBuffers {
        let mut partition_offsets = HashMap::new();
        let mut partition_counts = HashMap::new();

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| match buffer.consume() {
                Some((values, offset)) => {
                    partition_offsets.insert(*partition, offset);
                    partition_counts.insert(*partition, values.len());
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
            partition_counts,
        }
    }

    /// Clears all value buffers currently held in memory.
    pub(crate) fn reset(&mut self) {
        self.len = 0;
        self.buffers.clear();
    }
}

/// Buffer of values held in memory for a single Kafka partition.
#[derive(Debug)]
pub(crate) struct ValueBuffer {
    /// The offset of the last message stored in the buffer.
    last_offset: Option<DataTypeOffset>,
    /// The buffer of [`Value`] instances.
    values: Vec<Value>,
}

impl ValueBuffer {
    /// Creates a new [`ValueBuffer`] to store messages from a Kafka partition.
    pub(crate) fn new() -> Self {
        Self {
            last_offset: None,
            values: Vec::new(),
        }
    }

    /// Adds the value to buffer and stores its offset as the `last_offset` of the buffer.
    pub(crate) fn add(&mut self, value: Value, offset: DataTypeOffset) {
        self.last_offset = Some(offset);
        self.values.push(value);
    }

    /// Consumes and returns the buffer and last offset so it may be written to delta and clears internal state.
    pub(crate) fn consume(&mut self) -> Option<(Vec<Value>, DataTypeOffset)> {
        match self.last_offset {
            Some(last_offset) => {
                let consumed = (std::mem::take(&mut self.values), last_offset);
                self.last_offset = None;
                Some(consumed)
            }
            None => None,
        }
    }
}

/// A struct that wraps the data consumed from [`ValueBuffers`] before writing to a [`arrow::record_batch::RecordBatch`].
pub(crate) struct ConsumedBuffers {
    /// The vector of [`Value`] instances consumed.
    pub(crate) values: Vec<Value>,
    /// A [`HashMap`] from partition to last offset represented by the consumed buffers.
    pub(crate) partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
    /// A [`HashMap`] from partition to number of messages consumed for each partition.
    pub(crate) partition_counts: HashMap<DataTypePartition, usize>,
}
