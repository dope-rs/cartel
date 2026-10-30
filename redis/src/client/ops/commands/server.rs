use crate::api::{Client, decode};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Server<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Server<C, ID> {
    pub fn ping(self) -> C::Call<()> {
        self.0.encode::<()>(|out| out.command(&[b"PING"]))
    }

    pub fn publish(self, channel: &[u8], message: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| out.command(&[b"PUBLISH", channel, message]))
    }

    pub fn info(self, section: Option<&[u8]>) -> C::Call<decode::Bytes> {
        self.0.encode::<decode::Bytes>(move |out| match section {
            None => out.command(&[b"INFO"]),
            Some(section) => out.command(&[b"INFO", section]),
        })
    }
}
