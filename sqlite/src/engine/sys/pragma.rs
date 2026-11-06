use o3::buffer::{storage::inline, write::Sink as _};
use std::str;

pub(super) struct Pragma {
    bytes: inline::Bytes<96>,
}

impl Pragma {
    pub(super) fn try_new(name: &str, value: i64) -> Result<Self, crate::Error> {
        let mut pragma = Self {
            bytes: inline::Bytes::new(),
        };
        pragma.push(b"PRAGMA ")?;
        pragma.push(name.as_bytes())?;
        pragma.push(b"=")?;
        if value.is_negative() {
            pragma.push(b"-")?;
        }
        let mut digits = [0; 20];
        let start = cartel_core::write_digits(&mut digits, value.unsigned_abs());
        pragma.push(&digits[start..])?;
        Ok(pragma)
    }

    pub(super) fn as_str(&self) -> &str {
        // SAFETY: try_new appends only complete UTF-8 strings and ASCII bytes;
        // this private buffer's admitted prefix is unchanged afterward.
        unsafe { str::from_utf8_unchecked(self.bytes.as_ref()) }
    }

    fn push(&mut self, value: &[u8]) -> Result<(), crate::Error> {
        let start = self.bytes.len();
        self.bytes
            .write_slice(value)
            .map_err(|_| crate::Error::PragmaBytes {
                start,
                added: value.len(),
            })
    }
}
