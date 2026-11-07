use dope::{core::driver::route, manifold::client::conn};

use crate::{client, port::schedule::routes::slot};

#[must_use = "a Redis connection lease releases its exact generation when dropped"]
pub(crate) struct Lease<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> {
    redis: client::Redis<'app, 's, 'b, 'c, 'd, B, ID>,
    slot: slot::Bound<'app, 'b, 'd, ID>,
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Lease<'app, 's, 'b, 'c, 'd, B, ID> {
    pub(super) const fn new(
        redis: client::Redis<'app, 's, 'b, 'c, 'd, B, ID>,
        slot: slot::Bound<'app, 'b, 'd, ID>,
    ) -> Self {
        Self { redis, slot }
    }

    pub(crate) const fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, B, ID> {
        self.redis
    }

    pub(crate) const fn target(&self) -> conn::Id<'d, route::KeyTag<ID>> {
        self.slot.connection()
    }
}

impl<B: ?Sized, const ID: u8> Drop for Lease<'_, '_, '_, '_, '_, B, ID> {
    fn drop(&mut self) {
        self.redis
            .port
            .schedule
            .release(&self.slot, &self.redis.port.activity);
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>> for Lease<'app, 's, 'b, 'c, 'd, B, ID>
{
    fn as_ref(&self) -> &crate::Port<'s, 'app, 'b, 'c, 'd, ID> {
        &self.redis.port
    }
}

const _: () = assert!(std::mem::size_of::<Lease<'_, '_, '_, '_, '_, (), 0>>() == 40);
