use crate::api::task::lease::{Lease, Recover};
use crate::{api, port::pool::access, schema};

pub(in crate::api) struct Held<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    inner: Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, access::Transaction<'d, ID>, Recover, ID>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(super) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: access::Transaction<'d, ID>,
    ) -> Self {
        Self {
            inner: Lease::new(client, target),
        }
    }

    pub(in crate::api) fn client(&self) -> api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        self.inner.client()
    }

    pub(in crate::api) fn reborrow<'short>(
        &'short self,
    ) -> api::Client<'short, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        'app: 'short,
    {
        self.inner.reborrow()
    }

    pub(in crate::api) fn target(&self) -> access::Transaction<'d, ID> {
        self.inner.target()
    }

    pub(in crate::api) fn begin(
        self,
    ) -> Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, access::Finalization<'d, ID>, Recover, ID> {
        let client = self.inner.client();
        let state = client
            .port
            .shared
            .transaction()
            .begin_finish(self.inner.settle());
        Lease::new(client, state)
    }

    pub(in crate::api) fn abort(self, error: crate::Error) {
        self.inner.abort(error);
    }
}
