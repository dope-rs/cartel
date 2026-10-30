use crate::protocol::parser::{capacity, limit};

use dope_fiber::wait;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Limits {
    pub(super) waiters: wait::Capacity,
    pub(super) parser: capacity::Capacity,
    pub(super) frame: limit::Bound<limit::Frame>,
    pub(super) values: limit::Bound<limit::Value>,
}
