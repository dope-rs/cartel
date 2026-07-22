use o3::buffer::Shared;

use crate::Error;

pub enum Value {
    Nil,
    Ok,
    Status(Shared),
    Integer(i64),
    Bulk(Shared),
    Array(Vec<Value>),
    Error(Shared),
}

impl Value {
    pub fn into_bulk(self) -> Result<Shared, Error> {
        match self {
            Value::Bulk(b) => Ok(b),
            Value::Error(b) => Err(redis_error(b)),
            Value::Nil => Err(Error::Redis("expected bulk string, got nil".into())),
            _ => Err(Error::Redis("expected bulk string".into())),
        }
    }

    pub fn into_array(self) -> Result<Vec<Value>, Error> {
        match self {
            Value::Array(a) => Ok(a),
            Value::Error(b) => Err(redis_error(b)),
            Value::Nil => Ok(Vec::new()),
            _ => Err(Error::Redis("expected array".into())),
        }
    }

    pub fn into_result(self) -> Result<Self, Error> {
        match self {
            Value::Error(b) => Err(redis_error(b)),
            other => Ok(other),
        }
    }
}

fn redis_error(b: Shared) -> Error {
    Error::Redis(
        std::str::from_utf8(b.as_slice())
            .unwrap_or("<non-utf8 redis error>")
            .to_string(),
    )
}

pub trait FromValue: Sized {
    fn from_value(v: Value) -> Result<Self, Error>;
}

impl FromValue for Value {
    fn from_value(v: Value) -> Result<Self, Error> {
        v.into_result()
    }
}

impl FromValue for () {
    fn from_value(v: Value) -> Result<Self, Error> {
        v.into_result()?;
        Ok(())
    }
}

impl FromValue for i64 {
    fn from_value(v: Value) -> Result<Self, Error> {
        match v.into_result()? {
            Value::Integer(n) => Ok(n),
            Value::Bulk(b) => parse_signed(b.as_slice()),
            Value::Status(b) => parse_signed(b.as_slice()),
            _ => Err(Error::Redis("expected integer".into())),
        }
    }
}

impl FromValue for u64 {
    fn from_value(v: Value) -> Result<Self, Error> {
        let n = i64::from_value(v)?;
        if n < 0 {
            return Err(Error::Redis("expected non-negative integer".into()));
        }
        Ok(n as u64)
    }
}

impl FromValue for bool {
    fn from_value(v: Value) -> Result<Self, Error> {
        match v.into_result()? {
            Value::Integer(n) => Ok(n != 0),
            Value::Ok => Ok(true),
            Value::Nil => Ok(false),
            Value::Status(b) if b.as_slice() == b"OK" => Ok(true),
            _ => Err(Error::Redis("expected boolean".into())),
        }
    }
}

impl FromValue for Shared {
    fn from_value(v: Value) -> Result<Self, Error> {
        v.into_bulk()
    }
}

impl FromValue for Option<Shared> {
    fn from_value(v: Value) -> Result<Self, Error> {
        match v.into_result()? {
            Value::Nil => Ok(None),
            Value::Bulk(b) => Ok(Some(b)),
            _ => Err(Error::Redis("expected bulk or nil".into())),
        }
    }
}

impl FromValue for Vec<Option<Shared>> {
    fn from_value(v: Value) -> Result<Self, Error> {
        let arr = v.into_array()?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(<Option<Shared>>::from_value(item)?);
        }
        Ok(out)
    }
}

impl FromValue for Vec<Shared> {
    fn from_value(v: Value) -> Result<Self, Error> {
        let arr = v.into_array()?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(item.into_bulk()?);
        }
        Ok(out)
    }
}

impl FromValue for Vec<i64> {
    fn from_value(v: Value) -> Result<Self, Error> {
        let arr = v.into_array()?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            match item.into_result()? {
                Value::Integer(n) => out.push(n),
                _ => return Err(Error::Redis("expected integer in array".into())),
            }
        }
        Ok(out)
    }
}

impl FromValue for (Shared, Shared) {
    fn from_value(v: Value) -> Result<Self, Error> {
        let mut arr = v.into_array()?;
        if arr.len() != 2 {
            return Err(Error::Redis(format!(
                "expected 2-element array, got {}",
                arr.len()
            )));
        }
        let second = arr
            .pop()
            .ok_or_else(|| Error::Redis("missing second tuple element".into()))?
            .into_bulk()?;
        let first = arr
            .pop()
            .ok_or_else(|| Error::Redis("missing first tuple element".into()))?
            .into_bulk()?;
        Ok((first, second))
    }
}

impl FromValue for f64 {
    fn from_value(v: Value) -> Result<Self, Error> {
        match v.into_result()? {
            Value::Bulk(b) => parse_float(b.as_slice()),
            Value::Status(b) => parse_float(b.as_slice()),
            Value::Integer(n) => Ok(n as f64),
            _ => Err(Error::Redis("expected float".into())),
        }
    }
}

impl FromValue for Option<f64> {
    fn from_value(v: Value) -> Result<Self, Error> {
        match v.into_result()? {
            Value::Nil => Ok(None),
            other => Ok(Some(f64::from_value(other)?)),
        }
    }
}

impl FromValue for Option<u64> {
    fn from_value(v: Value) -> Result<Self, Error> {
        match v.into_result()? {
            Value::Nil => Ok(None),
            Value::Integer(n) if n >= 0 => Ok(Some(n as u64)),
            Value::Integer(_) => Err(Error::Redis("expected non-negative integer".into())),
            Value::Bulk(b) | Value::Status(b) => {
                let n = parse_signed(b.as_slice())?;
                if n < 0 {
                    return Err(Error::Redis("expected non-negative integer".into()));
                }
                Ok(Some(n as u64))
            }
            _ => Err(Error::Redis("expected integer or nil".into())),
        }
    }
}

impl FromValue for Vec<(Shared, Shared)> {
    fn from_value(v: Value) -> Result<Self, Error> {
        let arr = v.into_array()?;
        if arr.len() % 2 != 0 {
            return Err(Error::Redis(format!(
                "expected even-length array for pairs, got {}",
                arr.len()
            )));
        }
        let mut out = Vec::with_capacity(arr.len() / 2);
        let mut iter = arr.into_iter();
        while let Some(k) = iter.next() {
            let v = iter
                .next()
                .ok_or_else(|| Error::Redis("missing pair value".into()))?;
            out.push((k.into_bulk()?, v.into_bulk()?));
        }
        Ok(out)
    }
}

impl FromValue for Vec<(Shared, f64)> {
    fn from_value(v: Value) -> Result<Self, Error> {
        let arr = v.into_array()?;
        if arr.len() % 2 != 0 {
            return Err(Error::Redis(format!(
                "expected even-length array for member-score pairs, got {}",
                arr.len()
            )));
        }
        let mut out = Vec::with_capacity(arr.len() / 2);
        let mut iter = arr.into_iter();
        while let Some(m) = iter.next() {
            let s = iter
                .next()
                .ok_or_else(|| Error::Redis("missing score".into()))?;
            out.push((m.into_bulk()?, f64::from_value(s)?));
        }
        Ok(out)
    }
}

impl FromValue for (u64, Vec<Shared>) {
    fn from_value(v: Value) -> Result<Self, Error> {
        let mut arr = v.into_array()?;
        if arr.len() != 2 {
            return Err(Error::Redis(format!(
                "expected 2-element [cursor, keys], got {}",
                arr.len()
            )));
        }
        let keys_v = arr
            .pop()
            .ok_or_else(|| Error::Redis("missing scan keys".into()))?;
        let cursor_v = arr
            .pop()
            .ok_or_else(|| Error::Redis("missing scan cursor".into()))?;
        let cursor_bytes = cursor_v.into_bulk()?;
        let cursor = parse_signed(cursor_bytes.as_slice()).and_then(|n| {
            if n < 0 {
                Err(Error::Redis("negative cursor".into()))
            } else {
                Ok(n as u64)
            }
        })?;
        let keys = Vec::<Shared>::from_value(keys_v)?;
        Ok((cursor, keys))
    }
}

fn parse_float(buf: &[u8]) -> Result<f64, Error> {
    let s = std::str::from_utf8(buf).map_err(|_| Error::Redis("non-utf8 float".into()))?;
    match s {
        "inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        _ => s
            .parse::<f64>()
            .map_err(|_| Error::Redis(format!("invalid float: {s}"))),
    }
}

fn parse_signed(buf: &[u8]) -> Result<i64, Error> {
    std::str::from_utf8(buf)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(Error::Protocol(crate::protocol::Error::InvalidInteger))
}
