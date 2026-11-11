use crate::api::{Commands, decode};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Server<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Commands<'d, ID>, const ID: u8> Server<C, ID> {
    pub fn ping(self) -> C::Call<()> {
        self.0.encode::<()>(|out| out.command(&[b"PING"]))
    }

    pub fn publish(self, channel: &[u8], message: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| out.command(&[b"PUBLISH", channel, message]))
    }

    pub fn get_user<D: decode::Decode<C::Client>>(self, username: &[u8]) -> C::Call<D> {
        self.0
            .encode::<D>(move |out| out.command(&[b"ACL", b"GETUSER", username]))
    }

    pub fn set_user(self, username: &[u8], rules: &[&[u8]]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| out.secret(&[b"ACL", b"SETUSER", username], rules))
    }

    pub fn del_user(self, username: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"ACL", b"DELUSER", username]))
    }

    pub fn info(self, section: Option<&[u8]>) -> C::Call<decode::Bytes> {
        self.0.encode::<decode::Bytes>(move |out| match section {
            None => out.command(&[b"INFO"]),
            Some(section) => out.command(&[b"INFO", section]),
        })
    }
}
