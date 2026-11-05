use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn del<I, K>(
        self,
        declared: usize,
        keys: I,
    ) -> Result<encode::Complete, crate::request::Error>
    where
        I: Iterator<Item = K> + Clone,
        K: AsRef<[u8]>,
    {
        self.0.sequence(&[b"DEL"], declared, keys.map(|key| [key]))
    }

    pub(crate) fn incr_by(
        self,
        key: &[u8],
        by: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 21];
        let by = encode::format_int_into(&mut buffer, by);
        self.0.command(&[b"INCRBY", key, by])
    }

    pub(crate) fn incr_float(
        self,
        key: &[u8],
        by: f64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = ryu::Buffer::new();
        let by = encode::format_float(&mut buffer, by);
        self.0.command(&[b"INCRBYFLOAT", key, by])
    }

    pub(crate) fn mset(
        self,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0
            .sequence(&[b"MSET"], pairs.len(), pairs.iter().copied())
    }

    pub(crate) fn set_ttl(
        self,
        key: &[u8],
        value: &[u8],
        unit: &[u8],
        ttl: u64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 20];
        let ttl = encode::format_uint_into(&mut buffer, ttl);
        self.0.command(&[b"SET", key, value, unit, ttl])
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
