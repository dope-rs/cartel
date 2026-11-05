use crate::request::encode;
use std::num::NonZero;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn add(
        self,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.float(b"ZADD", key, score, member)
    }

    pub(crate) fn range(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        self.range_with(b"ZRANGE", &[], key, start, stop)
    }

    pub(crate) fn scan(
        self,
        key: &[u8],
        cursor: u64,
        count: Option<NonZero<u64>>,
    ) -> Result<encode::Complete, crate::request::Error> {
        encode::write::space::Writer::new(self.0).scan(&[b"ZSCAN", key], cursor, None, count)
    }

    pub(crate) fn rev_range(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        self.range_with(b"ZREVRANGE", &[b"WITHSCORES"], key, start, stop)
    }

    pub(crate) fn scored_range(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        self.range_with(b"ZRANGE", &[b"WITHSCORES"], key, start, stop)
    }

    pub(crate) fn score_range(
        self,
        key: &[u8],
        min: f64,
        max: f64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut lower = ryu::Buffer::new();
        let min = encode::format_float(&mut lower, min);
        let mut upper = ryu::Buffer::new();
        let max = encode::format_float(&mut upper, max);
        self.0.command(&[b"ZRANGEBYSCORE", key, min, max])
    }

    pub(crate) fn incr_by(
        self,
        key: &[u8],
        by: f64,
        member: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.float(b"ZINCRBY", key, by, member)
    }

    fn float(
        self,
        verb: &[u8],
        key: &[u8],
        value: f64,
        member: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = ryu::Buffer::new();
        let value = encode::format_float(&mut buffer, value);
        self.0.command(&[verb, key, value, member])
    }

    fn range_with<const N: usize>(
        self,
        verb: &[u8],
        suffix: &[&[u8]; N],
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut head = [0; 21];
        let start = encode::format_int_into(&mut head, start);
        let mut tail = [0; 21];
        let stop = encode::format_int_into(&mut tail, stop);
        self.0.slices(&[verb, key, start, stop], suffix)
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
