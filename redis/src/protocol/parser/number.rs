use crate::protocol::{self, parser};
use parser::limit;

pub(super) enum Length {
    Nil,
    Count(u64),
}

pub(super) struct Number<'a> {
    bytes: &'a [u8],
    body: limit::Body,
}

impl<'a> Number<'a> {
    pub(super) const fn new(bytes: &'a [u8], body: limit::Body) -> Self {
        Self { bytes, body }
    }

    pub(super) fn length(self, line: protocol::Line) -> Result<Length, protocol::Error> {
        if self.bytes.first() != Some(&b'-') {
            return match self.unsigned() {
                Ok(value) => Ok(Length::Count(value)),
                Err(cause) => Err(protocol::Error::Number { line, cause }),
            };
        }
        let value = match self.signed() {
            Ok(value) => value,
            Err(cause) => return Err(protocol::Error::Number { line, cause }),
        };
        if value == -1 {
            return Ok(Length::Nil);
        }
        if value < -1 {
            return Err(protocol::Error::Length {
                line,
                actual: value,
            });
        }
        Ok(Length::Count(value as u64))
    }

    pub(super) fn signed(self) -> Result<i64, protocol::Number> {
        if self.bytes.is_empty() {
            return Err(protocol::Number::Empty {
                offset: self.offset(0),
            });
        }
        let mut cursor = 0;
        let negative = self.bytes[0] == b'-';
        if negative || self.bytes[0] == b'+' {
            cursor = 1;
            if cursor == self.bytes.len() {
                return Err(protocol::Number::Missing {
                    offset: self.offset(cursor),
                });
            }
        }
        let mut value = 0i64;
        while cursor < self.bytes.len() {
            let byte = self.bytes[cursor];
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return Err(protocol::Number::Byte {
                    offset: self.offset(cursor),
                    actual: byte,
                });
            }
            let next = if negative {
                value
                    .checked_mul(10)
                    .and_then(|value| value.checked_sub(i64::from(digit)))
            } else {
                value
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(i64::from(digit)))
            };
            let Some(next) = next else {
                return Err(protocol::Number::Overflow {
                    offset: self.offset(cursor),
                });
            };
            value = next;
            cursor += 1;
        }
        Ok(value)
    }

    fn unsigned(self) -> Result<u64, protocol::Number> {
        if self.bytes.is_empty() {
            return Err(protocol::Number::Empty {
                offset: self.offset(0),
            });
        }
        let mut value = 0u64;
        for (index, byte) in self.bytes.iter().copied().enumerate() {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return Err(protocol::Number::Byte {
                    offset: self.offset(index),
                    actual: byte,
                });
            }
            let Some(next) = value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(digit)))
            else {
                return Err(protocol::Number::Overflow {
                    offset: self.offset(index),
                });
            };
            value = next;
        }
        Ok(value)
    }

    /// The matching admitted body bounds this line and its end within u32.
    /// Every relative offset comes from these bytes and is at most their length.
    fn offset(&self, relative: usize) -> u32 {
        self.body.start().wrapping_add(relative as u32)
    }
}
