use crate::{api, port::pool::access, protocol::runtime::tx, schema};
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use o3::mem::{self, lease};
use std::marker;

pub(crate) trait Target<'d, const ID: u8>: Copy {
    fn connection(self) -> conn::Id<'d, KeyTag<ID>>;
    fn current(self, tx: &tx::Access<'_, 'd, ID>) -> Option<conn::Id<'d, KeyTag<ID>>>;
}

impl<'d, const ID: u8> Target<'d, ID> for conn::Id<'d, KeyTag<ID>> {
    fn connection(self) -> conn::Id<'d, KeyTag<ID>> {
        self
    }

    fn current(self, _: &tx::Access<'_, 'd, ID>) -> Option<conn::Id<'d, KeyTag<ID>>> {
        Some(self)
    }
}

impl<'d, const ID: u8> Target<'d, ID> for access::Transaction<'d, ID> {
    fn connection(self) -> conn::Id<'d, KeyTag<ID>> {
        self.conn
    }

    fn current(self, tx: &tx::Access<'_, 'd, ID>) -> Option<conn::Id<'d, KeyTag<ID>>> {
        tx.is_current::<false>(self).then_some(self.conn)
    }
}

impl<'d, const ID: u8> Target<'d, ID> for access::Finalization<'d, ID> {
    fn connection(self) -> conn::Id<'d, KeyTag<ID>> {
        self.id().conn
    }

    fn current(self, tx: &tx::Access<'_, 'd, ID>) -> Option<conn::Id<'d, KeyTag<ID>>> {
        match self {
            Self::Pending(id) => tx.is_current::<true>(id).then_some(id.conn),
            Self::Reusable(_) | Self::Lost(_) => None,
        }
    }
}

pub(in crate::api) trait Policy {
    const RECOVER: bool;
}

pub(crate) struct Detached;
pub(crate) struct Recover;

impl Policy for Detached {
    const RECOVER: bool = false;
}

impl Policy for Recover {
    const RECOVER: bool = true;
}

/// Owner-bound request lease with compile-time abandonment policy.
pub(in crate::api) struct Lease<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I,
    T,
    P,
    const ID: u8 = 0,
> where
    I: schema::QuerySet + 'd,
    T: Target<'d, ID>,
    P: Policy,
{
    inner: mem::Lease<Owner<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, T, P, ID>, lease::Open>,
}

struct Owner<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, P, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    policy: marker::PhantomData<(T, P)>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, P, const ID: u8>
    Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, T, P, ID>
where
    I: schema::QuerySet + 'd,
    T: Target<'d, ID>,
    P: Policy,
{
    pub(in crate::api) fn admit(
        (): (),
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: T,
    ) -> Self {
        Self::new(client, target)
    }

    pub(in crate::api) fn abort(self, error: crate::Error) {
        let (owner, target) = self.inner.into_parts();
        owner.client.abort(target, error);
    }

    pub(in crate::api) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: T,
    ) -> Self {
        Self {
            inner: mem::Lease::open(
                Owner {
                    client,
                    policy: marker::PhantomData,
                },
                target,
            ),
        }
    }

    pub(in crate::api) fn client(&self) -> api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        self.inner.sink().client
    }

    pub(in crate::api) fn reborrow<'short>(
        &'short self,
    ) -> api::Client<'short, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        'app: 'short,
    {
        self.inner.sink().client.reborrow()
    }

    pub(in crate::api) fn target(&self) -> T {
        *self.inner.item()
    }

    pub(in crate::api) fn retarget<U>(
        self,
        target: U,
    ) -> Result<Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, U, P, ID>, crate::Error>
    where
        U: Target<'d, ID>,
    {
        if self.target().connection() != target.connection() {
            return Err(crate::Error::Closed);
        }
        let (owner, _) = self.inner.into_parts();
        Ok(Lease::new(owner.client, target))
    }

    pub(in crate::api) fn settle(self) -> T {
        self.inner.into_parts().1
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, const ID: u8>
    Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, T, Recover, ID>
where
    I: schema::QuerySet + 'd,
    T: Target<'d, ID>,
{
    pub(in crate::api) fn quarantine(self) {
        self.abort(crate::Error::Closed);
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, P, const ID: u8> mem::Return
    for Owner<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, T, P, ID>
where
    I: schema::QuerySet + 'd,
    T: Target<'d, ID>,
    P: Policy,
{
    type Item = T;

    fn return_item(self, target: Self::Item) {
        if P::RECOVER {
            self.client.abort(target, crate::Error::Closed);
        }
    }
}
