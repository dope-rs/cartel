use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn set<I, K, V>(
        self,
        key: &[u8],
        declared: usize,
        pairs: I,
    ) -> Result<encode::Complete, crate::request::Error>
    where
        I: Iterator<Item = (K, V)> + Clone,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.0.sequence(&[b"HSET", key], declared, pairs)
    }

    pub(crate) fn incr_by(
        self,
        key: &[u8],
        field: &[u8],
        by: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 21];
        let by = encode::format_int_into(&mut buffer, by);
        self.0.command(&[b"HINCRBY", key, field, by])
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
