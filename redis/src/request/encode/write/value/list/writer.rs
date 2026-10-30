use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn push_front(
        self,
        key: &[u8],
        values: &[&[u8]],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"LPUSH", key], values)
    }

    pub(crate) fn push_back(
        self,
        key: &[u8],
        values: &[&[u8]],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"RPUSH", key], values)
    }

    pub(crate) fn pop_front(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"LPOP", key])
    }

    pub(crate) fn pop_back(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"RPOP", key])
    }

    pub(crate) fn range(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut head = [0; 21];
        let start = encode::format_int_into(&mut head, start);
        let mut tail = [0; 21];
        let stop = encode::format_int_into(&mut tail, stop);
        self.0.command(&[b"LRANGE", key, start, stop])
    }

    pub(crate) fn len(self, key: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"LLEN", key])
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
