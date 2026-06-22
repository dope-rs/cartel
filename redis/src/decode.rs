use o3::buffer::Shared;

use crate::value::Value;
use crate::{Error, protocol};

const MAX_DEPTH: usize = 64;

pub(super) fn parse_one_bytes(buf: &Shared) -> Result<Option<(Value, usize)>, Error> {
    parse(buf, 0, 0)
}

fn parse(buf: &Shared, off: usize, depth: usize) -> Result<Option<(Value, usize)>, Error> {
    if depth > MAX_DEPTH {
        return Err(Error::Redis("RESP nesting too deep".into()));
    }
    let view = &buf.as_slice()[off..];
    if view.is_empty() {
        return Ok(None);
    }
    match view[0] {
        b'+' => parse_simple(buf, off + 1),
        b'-' => parse_error(buf, off + 1),
        b':' => parse_integer(buf, off + 1),
        b'$' => parse_bulk(buf, off + 1),
        b'*' => parse_array(buf, off + 1, depth),
        other => Err(Error::Redis(format!(
            "unknown RESP marker: {:?}",
            other as char
        ))),
    }
}

fn parse_simple(buf: &Shared, off: usize) -> Result<Option<(Value, usize)>, Error> {
    let view = &buf.as_slice()[off..];
    let Some(line_len) = find_crlf(view) else {
        return Ok(None);
    };
    let value = if view[..line_len] == *b"OK" {
        Value::Ok
    } else {
        Value::Status(buf.slice(off..off + line_len))
    };
    Ok(Some((value, off + line_len + 2)))
}

fn parse_error(buf: &Shared, off: usize) -> Result<Option<(Value, usize)>, Error> {
    let view = &buf.as_slice()[off..];
    let Some(line_len) = find_crlf(view) else {
        return Ok(None);
    };
    Ok(Some((
        Value::Error(buf.slice(off..off + line_len)),
        off + line_len + 2,
    )))
}

fn parse_integer(buf: &Shared, off: usize) -> Result<Option<(Value, usize)>, Error> {
    let view = &buf.as_slice()[off..];
    let Some(line_len) = find_crlf(view) else {
        return Ok(None);
    };
    let n = parse_signed(&view[..line_len])?;
    Ok(Some((Value::Integer(n), off + line_len + 2)))
}

fn parse_bulk(buf: &Shared, off: usize) -> Result<Option<(Value, usize)>, Error> {
    let view = &buf.as_slice()[off..];
    let Some(header_len) = find_crlf(view) else {
        return Ok(None);
    };
    let len = parse_signed(&view[..header_len])?;
    if len < 0 {
        return Ok(Some((Value::Nil, off + header_len + 2)));
    }
    let data_len = len as usize;
    let total = header_len + 2 + data_len + 2;
    if view.len() < total {
        return Ok(None);
    }
    let payload_off = off + header_len + 2;
    let trailer_idx = header_len + 2 + data_len;
    let trailer_ok = view[trailer_idx] == b'\r' && view[trailer_idx + 1] == b'\n';
    if !trailer_ok {
        return Err(Error::Redis("missing CRLF after bulk payload".into()));
    }
    Ok(Some((
        Value::Bulk(buf.slice(payload_off..payload_off + data_len)),
        off + total,
    )))
}

fn parse_array(buf: &Shared, off: usize, depth: usize) -> Result<Option<(Value, usize)>, Error> {
    let view = &buf.as_slice()[off..];
    let Some(header_len) = find_crlf(view) else {
        return Ok(None);
    };
    let count = parse_signed(&view[..header_len])?;
    if count < 0 {
        return Ok(Some((Value::Nil, off + header_len + 2)));
    }
    let n = count as usize;
    let mut items = Vec::with_capacity(n.min(64));
    let mut cursor = off + header_len + 2;
    for _ in 0..n {
        match parse(buf, cursor, depth + 1)? {
            None => return Ok(None),
            Some((value, used_total)) => {
                items.push(value);
                cursor = used_total;
            }
        }
    }
    Ok(Some((Value::Array(items), cursor)))
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_signed(buf: &[u8]) -> Result<i64, Error> {
    std::str::from_utf8(buf)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(Error::Protocol(protocol::Error::InvalidInteger))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(s: &[u8]) -> Result<Option<Value>, Error> {
        let buf = Shared::copy_from_slice(s);
        Ok(parse_one_bytes(&buf)?.map(|(v, _)| v))
    }

    #[test]
    fn parses_ok() {
        let v = decode(b"+OK\r\n").unwrap().unwrap();
        assert!(matches!(v, Value::Ok));
    }

    #[test]
    fn parses_integer() {
        let v = decode(b":-42\r\n").unwrap().unwrap();
        assert!(matches!(v, Value::Integer(-42)));
    }

    #[test]
    fn parses_bulk() {
        let v = decode(b"$5\r\nhello\r\n").unwrap().unwrap();
        match v {
            Value::Bulk(bytes) => assert_eq!(bytes.as_slice(), b"hello"),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn parses_null_bulk() {
        let v = decode(b"$-1\r\n").unwrap().unwrap();
        assert!(matches!(v, Value::Nil));
    }

    #[test]
    fn parses_array() {
        let v = decode(b"*2\r\n$3\r\nfoo\r\n:7\r\n").unwrap().unwrap();
        match v {
            Value::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], Value::Bulk(b) if b.as_slice() == b"foo"));
                assert!(matches!(items[1], Value::Integer(7)));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn returns_none_on_partial() {
        assert!(decode(b"$5\r\nhel").unwrap().is_none());
    }

    #[test]
    fn error_response_parses_to_value_error() {
        let v = decode(b"-ERR bad cmd\r\n").unwrap().unwrap();
        match v {
            Value::Error(msg) => assert_eq!(msg.as_slice(), b"ERR bad cmd"),
            _ => panic!("expected error variant"),
        }
    }

    #[test]
    fn unknown_marker_is_hard_error() {
        assert!(matches!(decode(b"?garbage\r\n"), Err(Error::Redis(_))));
    }

    #[test]
    fn invalid_integer_is_hard_error() {
        assert!(decode(b":notanint\r\n").is_err());
    }

    #[test]
    fn missing_bulk_trailer_is_hard_error() {
        assert!(matches!(decode(b"$3\r\nfooXX"), Err(Error::Redis(_))));
    }

    #[test]
    fn deep_nesting_is_rejected() {
        let mut s = Vec::new();
        for _ in 0..(MAX_DEPTH + 5) {
            s.extend_from_slice(b"*1\r\n");
        }
        s.extend_from_slice(b":1\r\n");
        let buf = Shared::copy_from_slice(&s);
        assert!(matches!(parse_one_bytes(&buf), Err(Error::Redis(_))));
    }

    #[test]
    fn huge_array_count_does_not_preallocate() {
        let buf = Shared::copy_from_slice(b"*1000000000\r\n");
        assert!(parse_one_bytes(&buf).unwrap().is_none());
    }

    #[test]
    fn bulk_view_is_zero_copy() {
        let buf = Shared::copy_from_slice(b"$5\r\nhello\r\n");
        let (v, _) = parse_one_bytes(&buf).unwrap().unwrap();
        let Value::Bulk(view) = v else {
            panic!("expected Bulk");
        };
        let buf_ptr = buf.as_slice().as_ptr() as usize;
        let view_ptr = view.as_slice().as_ptr() as usize;
        let buf_end = buf_ptr + buf.as_slice().len();
        assert!(
            view_ptr >= buf_ptr && view_ptr < buf_end,
            "Bulk view ({view_ptr:#x}) must point inside parent buffer ({buf_ptr:#x}..{buf_end:#x})"
        );
    }
}
