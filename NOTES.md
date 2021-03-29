

MessageBuffers: HashMap<DataTypePartition, MessageBuffer>

MessageBuffer: 
  messages: Vec<BufferedMessage>
  last_offset: DataTypeOffset

BufferedMessage
  message: Value
  offset: DataTypeOffset


WriteAheadLogEntry


WriteAheadLog


