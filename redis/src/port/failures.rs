use std::{mem, ptr::NonNull};

use cartel_core::reply::layout::{Domain, Position};
use dope::core::driver::route::KeyTag;
use o3::alloc::{self, array};

use crate::client;

/// Owns a fixed lane allocation and the lowest occupied payload within it.
/// No operation lends its backing or replaces it while the cache is retained.
/// Payload pointer order is the physical order in this nonzero-sized lane array.
pub(super) struct Failures<'b, 'd, const ID: u8> {
    lanes: array::Block<Option<client::Failure<'d>>, Domain<'b>>,
    first: Option<NonNull<client::Failure<'d>>>,
}

impl<'b, 'd, const ID: u8> Failures<'b, 'd, ID> {
    fn allocation(&mut self) -> NonNull<client::Failure<'d>> {
        // SAFETY: Block retains its original nonnull allocation pointer. The
        // pointer is used only to retain that allocation's provenance while
        // selecting the address of an occupied payload in the same backing.
        unsafe { NonNull::new_unchecked(self.lanes.as_mut_ptr().cast()) }
    }

    pub(super) fn try_new(domain: Domain<'b>) -> Result<Self, alloc::Error> {
        Ok(Self {
            lanes: array::Block::try_new(domain, ())?.repeat_with(|| None),
            first: None,
        })
    }

    pub(super) fn cause(&self) -> Option<crate::Error<'d, ID>> {
        self.first.map(|first| {
            // SAFETY: record derives this pointer from an occupied payload in
            // this fixed allocation. clear invalidates it before moving that
            // payload; the immutable owner borrow excludes every mutation.
            client::Failure::from(unsafe { first.as_ref() }).into_error()
        })
    }

    pub(super) fn clear(
        &mut self,
        position: Position<'b, 'd, KeyTag<ID>>,
    ) -> Option<client::Failure<'d>> {
        let base = self.allocation();
        let slot = position.entry_mut(&mut self.lanes);
        let first = self.first.is_some() && self.first == slot.as_mut().map(NonNull::from);
        if first {
            self.first = None;
        }
        let previous = slot.take();
        if first {
            // The cached minimum proves every earlier slot is empty.
            self.first = self
                .lanes
                .iter_mut()
                .skip(position.connection().index().into_usize() + 1)
                .find_map(|slot| {
                    slot.as_mut()
                        .map(|value| base.with_addr(NonNull::from(value).addr()))
                });
        }
        previous
    }

    pub(super) fn record(
        &mut self,
        position: Position<'b, 'd, KeyTag<ID>>,
        failure: client::Failure<'d>,
    ) -> Option<client::Failure<'d>> {
        let base = self.allocation();
        let slot = position.entry_mut(&mut self.lanes);
        let previous = slot.take();
        let occupied = base.with_addr(NonNull::from(slot.insert(failure)).addr());
        self.first = Some(self.first.map_or(occupied, |first| {
            if first.as_ptr() < occupied.as_ptr() {
                first
            } else {
                occupied
            }
        }));
        previous
    }
}

const _: () = {
    assert!(mem::size_of::<Failures<'static, 'static, 0>>() == 3 * mem::size_of::<usize>());
};
