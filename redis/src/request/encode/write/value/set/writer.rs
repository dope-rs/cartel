use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn add<I, M>(
        self,
        key: &[u8],
        declared: usize,
        members: I,
    ) -> Result<encode::Complete, crate::request::Error>
    where
        I: Iterator<Item = M> + Clone,
        M: AsRef<[u8]>,
    {
        self.0
            .sequence(&[b"SADD", key], declared, members.map(|member| [member]))
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
