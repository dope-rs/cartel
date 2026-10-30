use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn get(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"GET", key])
    }

    pub(crate) fn set(
        self,
        key: &[u8],
        value: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"SET", key, value])
    }

    pub(crate) fn set_ex(
        self,
        key: &[u8],
        value: &[u8],
        seconds: u64,
    ) -> Result<encode::Complete, crate::request::Error> {
        self.set_ttl(key, value, b"EX", seconds)
    }

    pub(crate) fn set_px(
        self,
        key: &[u8],
        value: &[u8],
        millis: u64,
    ) -> Result<encode::Complete, crate::request::Error> {
        self.set_ttl(key, value, b"PX", millis)
    }

    pub(crate) fn set_nx(
        self,
        key: &[u8],
        value: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"SET", key, value, b"NX"])
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

    pub(crate) fn exists(self, keys: &[&[u8]]) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"EXISTS"], keys)
    }

    pub(crate) fn incr(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"INCR", key])
    }

    pub(crate) fn decr(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"DECR", key])
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

    pub(crate) fn mget(self, keys: &[&[u8]]) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"MGET"], keys)
    }

    pub(crate) fn mset(
        self,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0
            .sequence(&[b"MSET"], pairs.len(), pairs.iter().copied())
    }

    fn set_ttl(
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
