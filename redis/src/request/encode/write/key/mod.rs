use crate::request::encode;

impl encode::Encoder {
    pub(crate) fn set_ttl(
        self,
        key: &[u8],
        value: &[u8],
        unit: &[u8],
        ttl: u64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = core::fmt::NumBuffer::new();
        let ttl = ttl.format_into(&mut buffer).as_bytes();
        self.command(&[b"SET", key, value, unit, ttl])
    }
}
