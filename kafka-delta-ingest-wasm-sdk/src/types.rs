use crate::traits::*;

pub type NewRootContext = fn(context_id: u32) -> Box<dyn RootContext>;
pub type NewMessageContext = fn(context_id: u32, root_context_id: u32) -> Box<dyn MessageContext>;

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Critical = 5,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum Action {
    Use = 0,
    Discard = 1,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    BadArgument = 2,
    ParseFailure = 4,
    Empty = 7,
    CasMismatch = 8,
    InternalFailure = 10,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum ContextType {
    MessageContext = 0,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum StreamType {
    HttpRequest = 0,
    HttpResponse = 1,
    Downstream = 2,
    Upstream = 3,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum BufferType {
    MessageBody = 0,
    VmConfiguration = 1,
    PluginConfiguration = 2,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum MapType {
    MessageHeaders = 0,
    MessageTopic = 1,
    MessageKey = 2,
    MessagePartition = 3,
    MessageOffset = 4,
    MessageTimestamp = 5,
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum MetricType {
    Counter = 0,
    Gauge = 1,
    Histogram = 2,
}

pub type Bytes = Vec<u8>;
