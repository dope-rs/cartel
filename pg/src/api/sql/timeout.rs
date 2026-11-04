use crate::api::sql::error;
use std::time;

/// A statement timeout proven to fit PostgreSQL's millisecond setting.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Timeout(u32);

impl Timeout {
    pub const DEFAULT: Self = Self(30_000);
    pub const DISABLED: Self = Self(0);

    pub fn new(duration: time::Duration) -> Result<Self, error::Error> {
        let millis = duration.as_millis();
        if millis == 0 && !duration.is_zero() {
            return Err(error::Error::Short(duration));
        }
        if millis > i32::MAX as u128 {
            return Err(error::Error::Long(duration));
        }
        Ok(Self(millis as u32))
    }

    pub const fn milliseconds(self) -> u32 {
        self.0
    }
}
