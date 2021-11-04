use crate::{DataTypeOffset, DataTypePartition, IngestError};
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
    ) -> Result<(), IngestError> {
        let buffer = self
            .buffers
            .entry(partition)
            .or_insert_with(ValueBuffer::new);

        if let Some(last_offset) = buffer.last_offset {
            if offset <= last_offset {
                return Err(IngestError::AlreadyProcessedPartitionOffset { partition, offset });
            }
        }

        buffer.add(value, offset);
        self.len += 1;
        Ok(())
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
struct ValueBuffer {
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

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;

    #[test]
    fn value_buffers_test() {
        let mut buffers = ValueBuffers::default();
        let mut add = |p, o| {
            buffers
                .add(p, o, Value::String(format!("{}:{}", p, o)))
                .unwrap();
        };

        add(0, 0);
        add(1, 0);
        add(0, 1);
        add(0, 2);
        add(1, 1);

        assert_eq!(buffers.len, 5);
        assert_eq!(buffers.buffers.len(), 2);
        assert_eq!(buffers.buffers.get(&0).unwrap().last_offset, Some(2));
        assert_eq!(buffers.buffers.get(&0).unwrap().values.len(), 3);
        assert_eq!(buffers.buffers.get(&1).unwrap().last_offset, Some(1));
        assert_eq!(buffers.buffers.get(&1).unwrap().values.len(), 2);

        let consumed = buffers.consume();

        assert_eq!(buffers.len, 0);
        assert_eq!(buffers.buffers.len(), 2);
        assert_eq!(buffers.buffers.get(&0).unwrap().last_offset, None);
        assert_eq!(buffers.buffers.get(&0).unwrap().values.len(), 0);
        assert_eq!(buffers.buffers.get(&1).unwrap().last_offset, None);
        assert_eq!(buffers.buffers.get(&1).unwrap().values.len(), 0);

        assert_eq!(
            consumed.partition_counts,
            hashmap! {
                0 => 3,
                1 => 2
            }
        );
        assert_eq!(
            consumed.partition_offsets,
            hashmap! {
                0 => 2,
                1 => 1
            }
        );

        let mut values: Vec<String> = consumed.values.iter().map(|j| j.to_string()).collect();

        values.sort();

        let expected: Vec<String> = vec!["\"0:0\"", "\"0:1\"", "\"0:2\"", "\"1:0\"", "\"1:1\""]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn value_buffers_conflict_offsets_test() {
        let mut buffers = ValueBuffers::default();
        let mut add = |o| buffers.add(0, o, Value::Number(o.into()));

        let verify_error = |res: Result<(), IngestError>, o: i64| {
            match res.err().unwrap() {
                IngestError::AlreadyProcessedPartitionOffset { partition, offset } => {
                    assert_eq!(partition, 0);
                    assert_eq!(offset, o);
                }
                other => panic!("{:?}", other),
            };
        };

        add(0).unwrap();
        add(1).unwrap();
        verify_error(add(0), 0);
        verify_error(add(1), 1);
        add(2).unwrap();

        let consumed = buffers.consume();

        assert_eq!(consumed.values.len(), 3);
        assert_eq!(
            consumed.partition_offsets,
            hashmap! {
                0 => 2,
            }
        );
    }
}
