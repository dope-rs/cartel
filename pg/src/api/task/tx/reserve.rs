use api::task::tx::Held;
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use o3::{
    base::cell::region,
    mem::{self, lease},
};

use crate::{api, port::pool::access, schema};

pub(in crate::api) struct Reserve<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I,
    const ID: u8 = 0,
> where
    I: schema::QuerySet + 'd,
{
    client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    proof: access::Reservation<'app, 'd, ID>,
    load: usize,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Reserve<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn acquire(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Option<mem::Lease<Self, lease::Open>> {
        let mut left = client.port.capacity();
        while left != 0 {
            left -= 1;
            let conn = client.port.shared.pick_conn(None)?;
            let Some(load) = client.port.load(conn, region) else {
                continue;
            };
            match client.port.shared.transaction().reserve(conn, load) {
                access::Reserve::Reserved(proof) => {
                    return Some(mem::Lease::open(
                        Self {
                            client,
                            proof,
                            load,
                        },
                        (),
                    ));
                }
                access::Reserve::Exhausted => {}
                access::Reserve::Rejected => {}
            }
        }
        None
    }

    pub(in crate::api) fn connection(&self) -> conn::Id<'d, KeyTag<ID>> {
        self.proof.id().conn
    }

    pub(in crate::api) fn commit(
        lease: mem::Lease<Self, lease::Open>,
    ) -> Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        let (this, ()) = lease.into_parts();
        let target = this.proof.activate();
        Held::new(this.client, target)
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> mem::Return
    for Reserve<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    type Item = ();

    fn return_item(self, (): ()) {
        self.client
            .port
            .shared
            .transaction()
            .release(self.proof, self.load);
    }
}
