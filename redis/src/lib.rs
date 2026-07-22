#![forbid(unsafe_code)]

use std::fmt;

mod client;
mod decode;
#[allow(dead_code)]
mod encode;
mod port;
#[doc(hidden)]
pub mod protocol;
mod value;

pub use client::{
    Capacities, Config, ConfigError, Connect, DEFAULT_BACKOFF, Factory, GeoCoord,
    MAX_FRAME_CAPACITY, Ops, Redis, Store,
};
pub use protocol::Error as ProtocolError;
pub use value::{FromValue, Value};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Closed,
    Protocol(ProtocolError),
    Redis(String),
    Backpressure { inflight: usize, capacity: usize },
    ResponseBufferCapacity,
    WaiterCapacity,
    RequestEntryCapacity,
    RequestBufferCapacity,
    ResponseFrameCapacity,
    ResponseValueCapacity,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Closed => f.write_str("closed"),
            Self::Protocol(err) => write!(f, "protocol error: {err}"),
            Self::Redis(err) => write!(f, "redis error: {err}"),
            Self::Backpressure { inflight, capacity } => {
                write!(f, "backpressure: {inflight}/{capacity} requests in flight")
            }
            Self::ResponseBufferCapacity => f.write_str("response buffer capacity exceeded"),
            Self::WaiterCapacity => f.write_str("waiter capacity exhausted"),
            Self::RequestEntryCapacity => f.write_str("request entry capacity exhausted"),
            Self::RequestBufferCapacity => f.write_str("request buffer capacity exceeded"),
            Self::ResponseFrameCapacity => f.write_str("response frame capacity exceeded"),
            Self::ResponseValueCapacity => f.write_str("response value capacity exceeded"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}
