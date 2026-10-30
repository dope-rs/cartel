use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn count(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"BITCOUNT", key])
    }

    pub(crate) fn count_range(
        self,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut first = [0; 21];
        let start = encode::format_int_into(&mut first, start);
        let mut last = [0; 21];
        let end = encode::format_int_into(&mut last, end);
        self.0.command(&[b"BITCOUNT", key, start, end])
    }

    pub(crate) fn set(
        self,
        key: &[u8],
        offset: u64,
        value: bool,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 20];
        let offset = encode::format_uint_into(&mut buffer, offset);
        let value: &[u8] = if value { b"1" } else { b"0" };
        self.0.command(&[b"SETBIT", key, offset, value])
    }

    pub(crate) fn get(
        self,
        key: &[u8],
        offset: u64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 20];
        let offset = encode::format_uint_into(&mut buffer, offset);
        self.0.command(&[b"GETBIT", key, offset])
    }

    pub(crate) fn combine(
        self,
        op: &[u8],
        dst: &[u8],
        sources: &[&[u8]],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"BITOP", op, dst], sources)
    }

    pub(crate) fn not(
        self,
        dst: &[u8],
        source: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"BITOP", b"NOT", dst, source])
    }

    pub(crate) fn position(
        self,
        key: &[u8],
        value: bool,
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0
            .command(&[b"BITPOS", key, if value { b"1" } else { b"0" }])
    }
}
