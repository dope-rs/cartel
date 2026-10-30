use dope::{
    core::driver::route::KeyTag,
    manifold::client::{conn, side::registry},
};

use crate::port::pool::key;

mod wire;
pub(crate) use wire::Storage;

pub struct Backend<'d, const ID: u8> {
    target: conn::Id<'d, KeyTag<ID>>,
    payload: wire::Payload,
}

impl<'d, const ID: u8> Backend<'d, ID> {
    pub(in crate::port) fn try_new(
        pool: &Storage,
        target: conn::Id<'d, KeyTag<ID>>,
        key: &key::Key,
    ) -> Option<Self> {
        Some(Self {
            target,
            payload: pool.acquire(key.pid(), key.secret())?,
        })
    }
}

impl<'d, const ID: u8> registry::Intent<'d, KeyTag<ID>> for Backend<'d, ID> {
    type Payload = wire::Payload;

    fn target(&self) -> conn::Id<'d, KeyTag<ID>> {
        self.target
    }

    fn into_payload(self) -> Self::Payload {
        self.payload
    }
}

const _: () = assert!(
    std::mem::size_of::<Backend<'static, 0>>()
        <= std::mem::size_of::<conn::Id<'static, KeyTag<0>>>()
            + std::mem::size_of::<wire::Payload>()
);
