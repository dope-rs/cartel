use std::fmt;

mod client;
mod decode;
mod encode;
pub mod protocol;
mod value;

pub use client::{DEFAULT_BACKOFF, Ops};
pub use protocol::Session;
pub use value::{FromValue, Value};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Closed,
    Protocol(protocol::Error),
    Redis(String),
    Backpressure { message: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Closed => f.write_str("closed"),
            Self::Protocol(err) => write!(f, "protocol error: {err}"),
            Self::Redis(err) => write!(f, "redis error: {err}"),
            Self::Backpressure { message } => write!(f, "backpressure: {message}"),
        }
    }
}
