use crate::request::encode;
use std::num::NonZero;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn replace(
        self,
        key: &[u8],
        value: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"GETSET", key, value])
    }

    pub(crate) fn take(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"GETDEL", key])
    }

    pub(crate) fn append(
        self,
        key: &[u8],
        value: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"APPEND", key, value])
    }

    pub(crate) fn len(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"STRLEN", key])
    }

    pub(crate) fn kind(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"TYPE", key])
    }

    pub(crate) fn rename(
        self,
        src: &[u8],
        dst: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"RENAME", src, dst])
    }

    pub(crate) fn persist(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"PERSIST", key])
    }

    pub(crate) fn ttl(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"TTL", key])
    }

    pub(crate) fn expire(
        self,
        key: &[u8],
        seconds: u64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 20];
        let seconds = encode::format_uint_into(&mut buffer, seconds);
        self.0.command(&[b"EXPIRE", key, seconds])
    }

    pub(crate) fn unlink(self, keys: &[&[u8]]) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"UNLINK"], keys)
    }

    pub(crate) fn db_len(self) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"DBSIZE"])
    }

    pub(crate) fn scan<const N: usize>(
        self,
        prefix: &[&[u8]; N],
        cursor: u64,
        pattern: Option<&[u8]>,
        count: Option<NonZero<u64>>,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut wire = [0; 20];
        let cursor = encode::format_uint_into(&mut wire, cursor);
        let mut buf = [0; 20];
        let count = count.map(|value| encode::format_uint_into(&mut buf, value.get()));
        match (pattern, count) {
            (None, None) => self.0.slices(prefix, [cursor]),
            (Some(pattern), None) => self.0.slices(prefix, [cursor, b"MATCH", pattern]),
            (None, Some(count)) => self.0.slices(prefix, [cursor, b"COUNT", count]),
            (Some(pattern), Some(count)) => self
                .0
                .slices(prefix, [cursor, b"MATCH", pattern, b"COUNT", count]),
        }
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
