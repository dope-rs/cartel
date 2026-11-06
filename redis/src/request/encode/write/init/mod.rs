use crate::request::encode;

impl encode::Encoder {
    pub(crate) fn auth(self, password: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.secret(&[b"AUTH", password], [])
    }

    pub(crate) fn auth_user(
        self,
        username: &[u8],
        password: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.secret(&[b"AUTH", username, password], [])
    }

    pub(crate) fn select(self, database: u32) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = core::fmt::NumBuffer::new();
        let database = database.format_into(&mut buffer).as_bytes();
        self.command(&[b"SELECT", database])
    }

    pub(crate) fn set_name(self, name: &[u8]) -> Result<encode::Complete, crate::request::Error> {
        self.command(&[b"CLIENT", b"SETNAME", name])
    }
}
