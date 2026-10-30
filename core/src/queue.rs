//! Fair, region-owned request queues and their affine transfer guards.

use std::{cell, marker, mem::size_of, num::NonZero};

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        schedule::app,
    },
    manifold::client::app::request,
    net::link::egress::{self, data, reserve::exact},
};

use crate::reply::{self, layout};
use o3::{
    alloc,
    base::cell::region,
    depot::{self, queue::pool::fair},
    mem::{
        self,
        credit::fair::{Axis, Cause},
        lease, quota,
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Drain {
    Empty,
    Exhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rejection {
    Capacity(Cause<usize>),
    Weight { lane: usize, cause: Cause<usize> },
}

#[derive(Debug, Eq, PartialEq)]
pub struct Rejected<T> {
    pub value: T,
    pub cause: Rejection,
}

struct Entry<T> {
    value: T,
    weight: usize,
}

/// Region-owned fixed storage shared fairly by a fixed number of FIFO lanes.
pub struct Arena<'d, T> {
    items: fair::Lanes<'d, Entry<T>, cell::Cell<usize>>,
}

/// A request arena admitted to one exact reply lane domain.
#[repr(transparent)]
pub struct Scoped<'b, 'd, T> {
    arena: Arena<'d, T>,
    domain: marker::PhantomData<fn(&'b ()) -> &'b ()>,
}

/// The request and reply lane extents differ at binding admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Mismatch {
    pub expected: NonZero<u32>,
    pub actual: usize,
}

/// A zero-cost typed view of one [`Arena`] lane.
pub struct Lane<'a, 'd, T> {
    binding: fair::Binding<'a, 'd, Entry<T>, cell::Cell<usize>>,
}

/// Ownership of one queue entry and its fair-lane charge until publication.
#[must_use = "a queue reservation must be published or released"]
pub struct Reservation<'turn, 'lane, 'd, T: Unpin> {
    lease: mem::Lease<Undo<'turn, 'lane, 'd, T>, lease::Open>,
}

struct Undo<'turn, 'lane, 'd, T: Unpin> {
    front: Front<'lane, 'd, T>,
    region: &'turn mut region::Token<region::Identity<'d>>,
}

impl<'d, T: Unpin> Undo<'_, '_, 'd, T> {
    fn publish(self, item: T) {
        self.front.permit.push_back(
            self.region,
            Entry {
                value: item,
                weight: self.front.weight,
            },
        );
    }
}

/// Ownership of a detached lane head until its charge is released.
#[must_use]
struct Front<'lane, 'd, T> {
    permit: fair::Permit<'lane, 'd, Entry<T>, cell::Cell<usize>>,
    weight: usize,
}

impl<T> Copy for Lane<'_, '_, T> {}

impl<T> Clone for Lane<'_, '_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, T: Unpin> Arena<'d, T> {
    pub fn try_new(
        config: fair::Plan,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            items: fair::Lanes::try_new(config, token)?,
        })
    }

    pub fn lane(&self, lane: usize) -> Option<Lane<'_, 'd, T>> {
        Some(Lane {
            binding: self.items.bind(lane)?,
        })
    }

    /// Checks the common immutable extent once, retaining its generative domain.
    pub fn try_bind<'b>(self, domain: layout::Domain<'b>) -> Result<Scoped<'b, 'd, T>, Mismatch> {
        let expected = domain.capacity();
        let actual = self.items.capacity();
        if actual != expected {
            return Err(Mismatch {
                expected,
                actual: actual.get() as usize,
            });
        }
        Ok(Scoped {
            arena: self,
            domain: marker::PhantomData,
        })
    }
}

impl<'b, 'd, T: Unpin> Scoped<'b, 'd, T> {
    pub fn lane<ID: Identity>(&self, position: layout::Position<'b, 'd, ID>) -> Lane<'_, 'd, T> {
        let index = position.connection().index().into_usize();
        // SAFETY: Position retains a checked index in the exact invariant domain
        // whose immutable extent try_bind matched to this original owner.
        let proof = unsafe { depot::raw::Proof::new((&self.arena.items, index)) };
        Lane {
            binding: fair::Binding::from_source(proof),
        }
    }

    pub fn lane_for<D, E, K>(
        &self,
        reply: reply::Lane<'_, 'b, '_, 'd, D, E, K>,
    ) -> Lane<'_, 'd, T> {
        let index = reply.index().get();
        // SAFETY: the admitted reply lane retains this invariant domain's exact
        // physical index; try_bind matched its extent to this original owner.
        let proof = unsafe { depot::raw::Proof::new((&self.arena.items, index)) };
        Lane {
            binding: fair::Binding::from_source(proof),
        }
    }
}

impl<'lane, 'd, T: Unpin> Lane<'lane, 'd, T> {
    pub const fn index(self) -> usize {
        self.binding.index()
    }

    pub fn len(self, token: &region::Token<region::Identity<'d>>) -> usize {
        self.binding.view(token).len()
    }

    pub fn is_empty(self, token: &region::Token<region::Identity<'d>>) -> bool {
        self.len(token) == 0
    }

    pub fn weight(self, token: &region::Token<region::Identity<'d>>) -> usize {
        self.binding.view(token).state().value().get()
    }

    pub fn try_push(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        item: T,
        weight: usize,
    ) -> Result<(), Rejected<T>> {
        let slot = match self.try_reserve(token, weight) {
            Ok(slot) => slot,
            Err(cause) => return Err(Rejected { value: item, cause }),
        };
        slot.publish(item);
        Ok(())
    }

    pub fn try_reserve<'turn>(
        self,
        token: &'turn mut region::Token<region::Identity<'d>>,
        weight: usize,
    ) -> Result<Reservation<'turn, 'lane, 'd, T>, Rejection> {
        let state = self.binding.view(token).state().value();
        let held = state.get();
        let Some(total) = held.checked_add(weight) else {
            return Err(Rejection::Weight {
                lane: self.index(),
                cause: Cause::Overflow {
                    axis: Axis::at::<0>(),
                    held,
                    requested: weight,
                },
            });
        };
        let permit = self.binding.reserve().map_err(Rejection::Capacity)?;
        state.set(total);
        Ok(Reservation {
            lease: mem::Lease::open(
                Undo {
                    front: Front { permit, weight },
                    region: token,
                },
                (),
            ),
        })
    }

    pub fn pop_front(self, token: &mut region::Token<region::Identity<'d>>) -> Option<T> {
        let (item, front) = self.take_front(token)?;
        front.release(token);
        Some(item)
    }

    fn take_front(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<(T, Front<'lane, 'd, T>)> {
        let (entry, permit) = self.binding.take_front(token)?;
        let Entry { value, weight } = entry;
        Some((value, Front { permit, weight }))
    }

    pub fn clear(
        self,
        work: app::Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        mut release: impl FnMut(T),
    ) -> Drain {
        loop {
            match work.run(|| self.pop_front(token)) {
                quota::Admission::Item(item) => release(item),
                quota::Admission::Empty => return Drain::Empty,
                quota::Admission::Exhausted => return Drain::Exhausted,
            }
        }
    }
}

impl<'lane, 'd, T: Unpin + data::raw::Payload<'d>> Lane<'lane, 'd, egress::NonEmpty<T>> {
    /// Moves source heads through owned exact final-link admission.
    pub fn drain<const ID: u8>(
        self,
        requests: &mut request::Requests<'_, '_, '_, 'd, T, KeyTag<ID>>,
    ) -> Result<(), exact::Failure> {
        loop {
            let Some((value, front)) = self.take_front(requests.region()) else {
                return Ok(());
            };
            let (value, result) = match requests.admit(value) {
                request::Admission::Ready(ready) => {
                    ready.push_with(|token| front.release(token));
                    continue;
                }
                request::Admission::Exhausted(value) | request::Admission::Saturated(value) => {
                    (value, Ok(()))
                }
                request::Admission::Rejected(rejected) => {
                    let (value, cause) = rejected.into_parts();
                    (value, Err(cause))
                }
            };
            front.restore(requests.region(), value);
            return result;
        }
    }
}

impl<'d, T: Unpin> Front<'_, 'd, T> {
    fn restore(self, token: &mut region::Token<region::Identity<'d>>, value: T) {
        self.permit.push_front(
            token,
            Entry {
                value,
                weight: self.weight,
            },
        );
    }

    /// Checked admission adds each charge once. Entries and detached guards
    /// stay in this permit's lane; consuming this guard refunds its charge.
    /// A forgotten guard can only leave excess held weight.
    fn release(self, token: &mut region::Token<region::Identity<'d>>) {
        let held = self.permit.state_mut(token);
        held.set(held.get().wrapping_sub(self.weight));
    }
}

impl<'turn, 'lane, 'd, T: Unpin> Reservation<'turn, 'lane, 'd, T> {
    pub fn publish(self, item: T) {
        let (undo, ()) = self.lease.into_parts();
        undo.publish(item);
    }

    pub(crate) fn try_with<I, J, P, R, E>(
        self,
        input: I,
        prepare: impl FnOnce(&mut region::Token<region::Identity<'d>>, I) -> Result<(P, J), (E, I)>,
        build: impl FnOnce(J, P) -> (T, R),
    ) -> Result<R, (E, I)> {
        let mut lease = self.lease;
        let (proof, input) = prepare(lease.parts_mut().0.region, input)?;
        let (item, output) = build(input, proof);
        let (undo, ()) = lease.into_parts();
        undo.publish(item);
        Ok(output)
    }
}

impl<T: Unpin> mem::Return for Undo<'_, '_, '_, T> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.front.release(self.region);
    }
}

const _: () = {
    const WORD: usize = size_of::<usize>();

    assert!(usize::BITS >= u32::BITS);
    assert!(size_of::<Entry<u64>>() == 2 * WORD);
    assert!(size_of::<Arena<'static, u64>>() == 6 * WORD);
    assert!(size_of::<Scoped<'static, 'static, u64>>() == size_of::<Arena<'static, u64>>());
    assert!(size_of::<Lane<'static, 'static, u64>>() == 2 * WORD);
    assert!(size_of::<Reservation<'static, 'static, 'static, u64>>() == 4 * WORD);
    assert!(size_of::<Front<'static, 'static, u64>>() == 3 * WORD);
};
