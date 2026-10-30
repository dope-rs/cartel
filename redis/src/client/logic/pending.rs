use cartel_core::frame::Buffer;
use dope::core::driver::route;
use dope::manifold::client::session::{
    response::{self, work},
    turns,
};

use crate::{
    Port,
    client::{
        CONNECTOR_CONNECTION_LIMIT, Logic,
        logic::{Owner, reply},
    },
    protocol::{
        self,
        parser::{frame, storage},
    },
};

/// A retained response awaiting insertion into its connection's reply queue.
pub struct Pending<'s, 'd>(
    pub(super) frame::Stored<'s, 'd>,
    pub(super) storage::Lane<'s, 'd>,
);

impl<'s, 'd> Pending<'s, 'd> {
    pub(super) fn new(frame: frame::Stored<'s, 'd>, lane: storage::Lane<'s, 'd>) -> Self {
        Self(frame, lane)
    }
}

impl<'life, 's, 'storage, 'b, 'c, 'd: 'life, const ID: u8>
    response::Action<
        'life,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        Logic<'s, 'd>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Pending<'s, 'd>
{
    fn settle<'e, A: work::Admission>(
        item: response::Item<Self>,
        owner: &mut Owner<'life, 's, 'storage, 'b, 'c, 'd, ID>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
    ) -> response::Settlement<Self> {
        let port = owner.root();
        match work.admit_with(item.into_inner(), |pending| {
            match reply::push(port, pending, ctx) {
                Some(pending) => response::Settlement::Blocked(pending),
                None => response::Settlement::Ready,
            }
        }) {
            work::Capacity::Available(next) => next,
            work::Capacity::Exhausted(item) => response::Settlement::Yield(item),
        }
    }
}

const _: () = {
    use std::mem;
    assert!(
        mem::size_of::<Pending<'static, 'static>>()
            == mem::size_of::<frame::Stored<'static, 'static>>()
                + mem::size_of::<storage::Lane<'static, 'static>>()
    );
    assert!(
        mem::size_of::<Option<Pending<'static, 'static>>>()
            == mem::size_of::<Option<frame::Stored<'static, 'static>>>()
                + mem::size_of::<storage::Lane<'static, 'static>>()
    );
};
