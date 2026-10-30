use std::time;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Exact invalid statement-timeout duration.
pub enum Error {
    Short(time::Duration),
    Long(time::Duration),
}
