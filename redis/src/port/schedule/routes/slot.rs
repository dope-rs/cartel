use std::{mem, ops};

use cartel_core::reply::layout::Position;
use dope::{core::driver::route, manifold::client::conn};
use o3::depot::queue::indexed::shared;

use crate::port::schedule::routes::Conn;

pub(in crate::port) struct Slot<'a, 'b, 'd, const ID: u8>(
    pub(super) shared::Slot<'a, Position<'b, 'd, route::KeyTag<ID>>, Conn<'b, 'd, ID>, 2>,
);

pub(in crate::port) struct Bound<'a, 'b, 'd, const ID: u8> {
    pub(super) slot: Slot<'a, 'b, 'd, ID>,
    pub(super) position: Position<'b, 'd, route::KeyTag<ID>>,
}

impl<'b, 'd, const ID: u8> Bound<'_, 'b, 'd, ID> {
    pub(in crate::port) const fn position(&self) -> Position<'b, 'd, route::KeyTag<ID>> {
        self.position
    }

    pub(in crate::port) const fn connection(&self) -> conn::Id<'d, route::KeyTag<ID>> {
        self.position.connection()
    }
}

impl<'a, 'b, 'd, const ID: u8> ops::Deref for Bound<'a, 'b, 'd, ID> {
    type Target = Slot<'a, 'b, 'd, ID>;

    fn deref(&self) -> &Self::Target {
        &self.slot
    }
}

impl<'a, 'b, 'd, const ID: u8> Slot<'a, 'b, 'd, ID> {
    pub(in crate::port::schedule) fn schedule<const P: usize>(
        &self,
        target: Position<'b, 'd, route::KeyTag<ID>>,
    ) {
        if self.0.get::<P>().map(Position::connection) != Some(target.connection()) {
            self.0.push::<P>(target);
        }
    }

    pub(in crate::port::schedule) fn unlink<const P: usize>(
        &self,
        target: conn::Id<'d, route::KeyTag<ID>>,
    ) {
        if self.0.get::<P>().map(Position::connection) == Some(target) {
            self.0.clear::<P>();
        }
    }
}

impl<'b, 'd, const ID: u8> ops::Deref for Slot<'_, 'b, 'd, ID> {
    type Target = Conn<'b, 'd, ID>;

    fn deref(&self) -> &Self::Target {
        self.0.state()
    }
}

const _: () = {
    assert!(mem::size_of::<Slot<'_, '_, '_, 0>>() == 16);
    assert!(mem::size_of::<Option<Slot<'_, '_, '_, 0>>>() == 16);
};
