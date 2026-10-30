use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Encoder);

impl Writer {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn auth(self, password: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.secret(&[b"AUTH", password])
    }

    pub(crate) fn auth_user(
        self,
        username: &[u8],
        password: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.secret(&[b"AUTH", username, password])
    }

    pub(crate) fn select(self, database: u32) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = [0; 20];
        let database = encode::format_uint_into(&mut buffer, u64::from(database));
        self.0.command(&[b"SELECT", database])
    }

    pub(crate) fn set_name(self, name: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.0.command(&[b"CLIENT", b"SETNAME", name])
    }
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Encoder>());
