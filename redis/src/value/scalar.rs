use crate::{
    protocol::parser::{self, frame},
    value::reply,
};
use std::str;

pub(super) fn float<'id>(
    value: parser::Value<frame::Selection<'id>>,
    frame: &frame::Owner<'id, '_>,
) -> Result<f64, reply::Fault<'id>> {
    match value {
        parser::Value::Bulk(span) | parser::Value::Status(span) => {
            parse_float(frame.slice(span)).map_err(|reason| reply::Fault::Float { span, reason })
        }
        parser::Value::Integer(value) => Ok(value as f64),
        actual => Err(reply::Fault::expected(reply::Expectation::Float, actual)),
    }
}

pub(super) enum Error {
    Invalid(reply::Invalid),
    Negative(i64),
}

fn parse_float(bytes: &[u8]) -> Result<f64, reply::Float> {
    let Some((&first, tail)) = bytes.split_first() else {
        return Err(reply::Float::Invalid(reply::Invalid::Empty));
    };
    let (body, start) = if matches!(first, b'+' | b'-') {
        (tail, 1)
    } else {
        (bytes, 0)
    };
    let Some(&first) = body.first() else {
        return Err(reply::Float::Invalid(reply::Invalid::Missing {
            offset: start,
        }));
    };
    let value = str::from_utf8(bytes).map_err(|error| {
        let offset = error.valid_up_to();
        // SAFETY: rejection identifies an invalid or incomplete sequence in
        // these exact bytes, so its first byte is before the source end.
        let actual = unsafe { bytes.get_unchecked(offset) };
        reply::Float::Invalid(reply::Invalid::Byte {
            offset,
            actual: *actual,
        })
    })?;
    match value.parse::<f64>() {
        Ok(value) => Ok(value),
        Err(error) => Err(reply::Float::Parse {
            error,
            cause: float_cause(bytes, first, body, start),
        }),
    }
}

fn float_cause(bytes: &[u8], first: u8, body: &[u8], start: usize) -> Option<reply::Invalid> {
    let named: Option<(&[u8], usize)> = match first.to_ascii_lowercase() {
        b'i' => Some((b"infinity", 3)),
        b'n' => Some((b"nan", 3)),
        _ => None,
    };
    if let Some((spelling, terminal)) = named {
        for (offset, (&actual, &expected)) in body.iter().zip(spelling).enumerate() {
            if actual.to_ascii_lowercase() != expected {
                return Some(reply::Invalid::Byte {
                    offset: start + offset,
                    actual,
                });
            }
        }
        if body.len() == terminal || body.len() == spelling.len() {
            return None;
        }
        if body.len() < spelling.len() {
            return Some(reply::Invalid::Missing {
                offset: start + body.len(),
            });
        }
        return body
            .get(spelling.len())
            .copied()
            .map(|actual| reply::Invalid::Byte {
                offset: start + spelling.len(),
                actual,
            });
    }

    let mut cursor = start;
    let mut digits = 0;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
        digits += 1;
    }
    if bytes.get(cursor).copied() == Some(b'.') {
        cursor += 1;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return Some(match bytes.get(cursor).copied() {
            Some(actual) => reply::Invalid::Byte {
                offset: cursor,
                actual,
            },
            None => reply::Invalid::Missing { offset: cursor },
        });
    }
    if matches!(bytes.get(cursor).copied(), Some(b'e' | b'E')) {
        cursor += 1;
        if matches!(bytes.get(cursor).copied(), Some(b'+' | b'-')) {
            cursor += 1;
        }
        let exponent = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == exponent {
            return Some(match bytes.get(cursor).copied() {
                Some(actual) => reply::Invalid::Byte {
                    offset: cursor,
                    actual,
                },
                None => reply::Invalid::Missing { offset: cursor },
            });
        }
    }
    if let Some(actual) = bytes.get(cursor).copied() {
        return Some(reply::Invalid::Byte {
            offset: cursor,
            actual,
        });
    }
    None
}

pub(super) fn signed(bytes: &[u8]) -> Result<i64, reply::Invalid> {
    let Some((&first, tail)) = bytes.split_first() else {
        return Err(reply::Invalid::Empty);
    };
    let (negative, digits, start) = if first == b'-' {
        (true, tail, 1)
    } else if first == b'+' {
        (false, tail, 1)
    } else {
        (false, bytes, 0)
    };
    let limit = if negative {
        i64::MAX as u64 + 1
    } else {
        i64::MAX as u64
    };
    let value = magnitude(digits, start, limit)?;
    if negative {
        Ok((value as i64).wrapping_neg())
    } else {
        Ok(value as i64)
    }
}

pub(super) fn unsigned(bytes: &[u8]) -> Result<u64, Error> {
    let Some((&first, tail)) = bytes.split_first() else {
        return Err(Error::Invalid(reply::Invalid::Empty));
    };
    if first == b'-' {
        let value = signed(bytes).map_err(Error::Invalid)?;
        return if value < 0 {
            Err(Error::Negative(value))
        } else {
            Ok(0)
        };
    }
    let (digits, start) = if first == b'+' { (tail, 1) } else { (bytes, 0) };
    magnitude(digits, start, u64::MAX).map_err(Error::Invalid)
}

fn magnitude(bytes: &[u8], start: usize, limit: u64) -> Result<u64, reply::Invalid> {
    if bytes.is_empty() {
        return Err(reply::Invalid::Missing { offset: start });
    }
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if !byte.is_ascii_digit() {
            return Err(reply::Invalid::Byte {
                offset: start + index,
                actual: byte,
            });
        }
        let digit = u64::from(byte - b'0');
        let Some(next) = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
        else {
            return Err(reply::Invalid::Overflow {
                offset: start + index,
            });
        };
        if next > limit {
            return Err(reply::Invalid::Overflow {
                offset: start + index,
            });
        }
        value = next;
    }
    Ok(value)
}
