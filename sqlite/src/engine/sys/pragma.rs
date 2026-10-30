use o3::buffer::{storage::inline, write::Sink as _};
use std::str;

pub(super) struct Pragma {
    bytes: inline::Bytes<96, usize>,
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
        let mut items = 0;
        let mut left = value.unsigned_abs();
        loop {
            digits[items] = b'0' + (left % 10) as u8;
            items += 1;
            left /= 10;
            if left == 0 {
                break;
            }
        }
        while items != 0 {
            items -= 1;
            pragma.push(&digits[items..items + 1])?;
        }
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
