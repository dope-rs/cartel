use crate::request::encode;
use std::num::NonZero;

impl encode::Encoder {
    pub(crate) fn scan<const N: usize>(
        self,
        prefix: &[&[u8]; N],
        cursor: u64,
        pattern: Option<&[u8]>,
        count: Option<NonZero<u64>>,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut wire = core::fmt::NumBuffer::new();
        let cursor = cursor.format_into(&mut wire).as_bytes();
        let mut buf = core::fmt::NumBuffer::new();
        let count = count.map(|value| value.get().format_into(&mut buf).as_bytes());
        match (pattern, count) {
            (None, None) => self.slices(prefix, [cursor]),
            (Some(pattern), None) => self.slices(prefix, [cursor, b"MATCH", pattern]),
            (None, Some(count)) => self.slices(prefix, [cursor, b"COUNT", count]),
            (Some(pattern), Some(count)) => {
                self.slices(prefix, [cursor, b"MATCH", pattern, b"COUNT", count])
            }
        }
    }
}
