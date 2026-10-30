//! PostgreSQL statement timing values and exact validation failures.

mod error;
mod timeout;

pub use error::Error;
pub use timeout::Timeout;
