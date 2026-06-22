use std::fmt;

pub enum Error {
    Backpressure {
        inflight: usize,
        queued: usize,
        cap: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backpressure {
                inflight,
                queued,
                cap,
            } => write!(
                f,
                "backpressure: pipeline full ({inflight}/{cap}, queued={queued})"
            ),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
