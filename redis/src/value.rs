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
        let second = arr.pop().unwrap().into_bulk()?;
        let first = arr.pop().unwrap().into_bulk()?;
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
        let keys_v = arr.pop().unwrap();
        let cursor_v = arr.pop().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &[u8]) -> Shared {
        Shared::copy_from_slice(s)
    }

    #[test]
    fn unit_accepts_any_non_error_value() {
        assert!(<()>::from_value(Value::Ok).is_ok());
        assert!(<()>::from_value(Value::Integer(0)).is_ok());
        assert!(<()>::from_value(Value::Nil).is_ok());
        assert!(<()>::from_value(Value::Bulk(b(b"x"))).is_ok());
    }

    #[test]
    fn unit_rejects_error_value() {
        assert!(<()>::from_value(Value::Error(b(b"ERR fail"))).is_err());
    }

    #[test]
    fn i64_decodes_integer_and_bulk() {
        assert_eq!(i64::from_value(Value::Integer(42)).unwrap(), 42);
        assert_eq!(i64::from_value(Value::Bulk(b(b"-7"))).unwrap(), -7);
        assert_eq!(i64::from_value(Value::Status(b(b"99"))).unwrap(), 99);
    }

    #[test]
    fn i64_rejects_garbage_bulk() {
        assert!(i64::from_value(Value::Bulk(b(b"oops"))).is_err());
    }

    #[test]
    fn u64_rejects_negative() {
        assert!(u64::from_value(Value::Integer(-1)).is_err());
        assert_eq!(u64::from_value(Value::Integer(7)).unwrap(), 7);
    }

    #[test]
    fn bool_decodes_integer_ok_and_nil() {
        assert!(bool::from_value(Value::Integer(1)).unwrap());
        assert!(!bool::from_value(Value::Integer(0)).unwrap());
        assert!(bool::from_value(Value::Ok).unwrap());
        assert!(!bool::from_value(Value::Nil).unwrap());
    }

    #[test]
    fn bytes_requires_bulk() {
        assert_eq!(
            Shared::from_value(Value::Bulk(b(b"abc")))
                .unwrap()
                .as_slice(),
            b"abc"
        );
        assert!(Shared::from_value(Value::Nil).is_err());
        assert!(Shared::from_value(Value::Integer(1)).is_err());
    }

    #[test]
    fn option_bytes_handles_nil() {
        assert!(Option::<Shared>::from_value(Value::Nil).unwrap().is_none());
        let s = Option::<Shared>::from_value(Value::Bulk(b(b"x"))).unwrap();
        assert_eq!(s.unwrap().as_slice(), b"x");
    }

    #[test]
    fn vec_option_bytes_decodes_array() {
        let arr = Value::Array(vec![Value::Bulk(b(b"a")), Value::Nil, Value::Bulk(b(b"c"))]);
        let v = <Vec<Option<Shared>>>::from_value(arr).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].as_ref().unwrap().as_slice(), b"a");
        assert!(v[1].is_none());
        assert_eq!(v[2].as_ref().unwrap().as_slice(), b"c");
    }

    #[test]
    fn pair_requires_two_bulk() {
        let arr = Value::Array(vec![Value::Bulk(b(b"host")), Value::Bulk(b(b"6379"))]);
        let (h, p) = <(Shared, Shared)>::from_value(arr).unwrap();
        assert_eq!(h.as_slice(), b"host");
        assert_eq!(p.as_slice(), b"6379");
    }

    #[test]
    fn pair_rejects_wrong_arity() {
        let arr = Value::Array(vec![Value::Bulk(b(b"only"))]);
        assert!(<(Shared, Shared)>::from_value(arr).is_err());
    }

    #[test]
    fn from_value_propagates_error_variant() {
        let err = Value::Error(b(b"ERR boom"));
        let res = i64::from_value(err);
        assert!(matches!(res, Err(Error::Redis(_))));
    }

    #[test]
    fn f64_decodes_bulk_status_and_int() {
        assert_eq!(f64::from_value(Value::Bulk(b(b"3.25"))).unwrap(), 3.25);
        assert_eq!(f64::from_value(Value::Status(b(b"-2.5"))).unwrap(), -2.5);
        assert_eq!(f64::from_value(Value::Integer(7)).unwrap(), 7.0);
        assert!(
            f64::from_value(Value::Bulk(b(b"inf")))
                .unwrap()
                .is_infinite()
        );
    }

    #[test]
    fn option_f64_handles_nil() {
        assert!(Option::<f64>::from_value(Value::Nil).unwrap().is_none());
        assert_eq!(
            Option::<f64>::from_value(Value::Bulk(b(b"1.5"))).unwrap(),
            Some(1.5)
        );
    }

    #[test]
    fn option_u64_handles_nil_and_negative() {
        assert!(Option::<u64>::from_value(Value::Nil).unwrap().is_none());
        assert_eq!(
            Option::<u64>::from_value(Value::Integer(5)).unwrap(),
            Some(5)
        );
        assert!(Option::<u64>::from_value(Value::Integer(-1)).is_err());
    }

    #[test]
    fn vec_bytes_decodes() {
        let arr = Value::Array(vec![Value::Bulk(b(b"a")), Value::Bulk(b(b"b"))]);
        let v = Vec::<Shared>::from_value(arr).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].as_slice(), b"a");
        assert_eq!(v[1].as_slice(), b"b");
    }

    #[test]
    fn vec_pair_bytes_decodes_hgetall() {
        let arr = Value::Array(vec![
            Value::Bulk(b(b"name")),
            Value::Bulk(b(b"alice")),
            Value::Bulk(b(b"age")),
            Value::Bulk(b(b"30")),
        ]);
        let v = <Vec<(Shared, Shared)>>::from_value(arr).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].0.as_slice(), b"name");
        assert_eq!(v[0].1.as_slice(), b"alice");
        assert_eq!(v[1].0.as_slice(), b"age");
        assert_eq!(v[1].1.as_slice(), b"30");
    }

    #[test]
    fn vec_pair_bytes_rejects_odd_length() {
        let arr = Value::Array(vec![Value::Bulk(b(b"only"))]);
        assert!(<Vec<(Shared, Shared)>>::from_value(arr).is_err());
    }

    #[test]
    fn vec_member_score_decodes_zrange_with_scores() {
        let arr = Value::Array(vec![
            Value::Bulk(b(b"alice")),
            Value::Bulk(b(b"3.5")),
            Value::Bulk(b(b"bob")),
            Value::Bulk(b(b"1.0")),
        ]);
        let v = <Vec<(Shared, f64)>>::from_value(arr).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].0.as_slice(), b"alice");
        assert_eq!(v[0].1, 3.5);
        assert_eq!(v[1].1, 1.0);
    }

    #[test]
    fn cursor_keys_decodes_scan() {
        let arr = Value::Array(vec![
            Value::Bulk(b(b"42")),
            Value::Array(vec![Value::Bulk(b(b"k1")), Value::Bulk(b(b"k2"))]),
        ]);
        let (cursor, keys) = <(u64, Vec<Shared>)>::from_value(arr).unwrap();
        assert_eq!(cursor, 42);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].as_slice(), b"k1");
    }
}
