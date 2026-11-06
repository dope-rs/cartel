//! Fair, region-owned request queues and their affine transfer guards.

use std::{marker, mem::size_of, num::NonZero};

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

mod sealed {
    pub trait Weight {}
    impl Weight for usize {}
    impl Weight for () {}
}

/// The supported per-entry charges: usize weights or no weight accounting.
pub trait Weight: sealed::Weight + Copy {
    fn try_add(self, other: Self) -> Result<Self, Cause<usize>>;
    fn wrapping_sub(self, other: Self) -> Self;
}

impl Weight for usize {
    #[inline]
    fn try_add(self, other: Self) -> Result<Self, Cause<usize>> {
        self.checked_add(other).ok_or(Cause::Overflow {
            axis: Axis::at::<0>(),
            held: self,
            requested: other,
        })
    }

    #[inline]
    fn wrapping_sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
    }
}

impl Weight for () {
    #[inline]
    fn try_add(self, _: Self) -> Result<Self, Cause<usize>> {
        Ok(())
    }

    #[inline]
    fn wrapping_sub(self, _: Self) -> Self {}
}

struct Entry<T, W> {
    value: T,
    weight: W,
}

/// Region-owned fixed storage shared fairly by a fixed number of FIFO lanes.
pub struct Arena<'d, T, W: Weight = usize> {
    items: fair::Lanes<'d, Entry<T, W>, W>,
}

/// A request arena admitted to one exact reply lane domain.
#[repr(transparent)]
pub struct Scoped<'b, 'd, T, W: Weight = usize> {
    arena: Arena<'d, T, W>,
    domain: marker::PhantomData<fn(&'b ()) -> &'b ()>,
}

/// The request and reply lane extents differ at binding admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Mismatch {
    pub expected: NonZero<u32>,
    pub actual: usize,
}

/// A zero-cost typed view of one [`Arena`] lane.
pub struct Lane<'a, 'd, T, W: Weight = usize> {
    binding: fair::Binding<'a, 'd, Entry<T, W>, W>,
}

/// Ownership of one queue entry and its fair-lane charge until publication.
#[must_use = "a queue reservation must be published or released"]
pub struct Reservation<'turn, 'lane, 'd, T: Unpin, W: Weight = usize> {
    lease: mem::Lease<Undo<'turn, 'lane, 'd, T, W>, lease::Open>,
}

struct Undo<'turn, 'lane, 'd, T: Unpin, W: Weight> {
    front: Front<'lane, 'd, T, W>,
    region: &'turn mut region::Token<region::Identity<'d>>,
}

impl<'d, T: Unpin, W: Weight> Undo<'_, '_, 'd, T, W> {
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
struct Front<'lane, 'd, T, W> {
    permit: fair::Permit<'lane, 'd, Entry<T, W>, W>,
    weight: W,
}

impl<T, W: Weight> Copy for Lane<'_, '_, T, W> {}

impl<T, W: Weight> Clone for Lane<'_, '_, T, W> {
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

    /// Creates fair queue storage without retaining or accounting entry weights.
    pub fn try_unweighted(
        config: fair::Plan,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Arena<'d, T, ()>, alloc::Error> {
        Ok(Arena {
            items: fair::Lanes::try_new(config, token)?,
        })
    }
}

impl<'d, T: Unpin, W: Weight> Arena<'d, T, W> {
    pub fn lane(&self, lane: usize) -> Option<Lane<'_, 'd, T, W>> {
        Some(Lane {
            binding: self.items.bind(lane)?,
        })
    }

    /// Checks the common immutable extent once, retaining its generative domain.
    pub fn try_bind<'b>(
        self,
        domain: layout::Domain<'b>,
    ) -> Result<Scoped<'b, 'd, T, W>, Mismatch> {
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

impl<'b, 'd, T: Unpin, W: Weight> Scoped<'b, 'd, T, W> {
    pub fn lane<ID: Identity>(&self, position: layout::Position<'b, 'd, ID>) -> Lane<'_, 'd, T, W> {
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
    ) -> Lane<'_, 'd, T, W> {
        let index = reply.index().get();
        // SAFETY: the admitted reply lane retains this invariant domain's exact
        // physical index; try_bind matched its extent to this original owner.
        let proof = unsafe { depot::raw::Proof::new((&self.arena.items, index)) };
        Lane {
            binding: fair::Binding::from_source(proof),
        }
    }
}

impl<'d, T: Unpin> Lane<'_, 'd, T> {
    pub fn weight(self, token: &region::Token<region::Identity<'d>>) -> usize {
        *self.binding.view(token).state().value()
    }
}

impl<'lane, 'd, T: Unpin, W: Weight> Lane<'lane, 'd, T, W> {
    pub const fn index(self) -> usize {
        self.binding.index()
    }

    pub fn len(self, token: &region::Token<region::Identity<'d>>) -> usize {
        self.binding.view(token).len()
    }

    pub fn is_empty(self, token: &region::Token<region::Identity<'d>>) -> bool {
        self.len(token) == 0
    }

    pub fn try_push(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        item: T,
        weight: W,
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
        weight: W,
    ) -> Result<Reservation<'turn, 'lane, 'd, T, W>, Rejection> {
        let held = *self.binding.view(token).state().value();
        let total = held.try_add(weight).map_err(|cause| Rejection::Weight {
            lane: self.index(),
            cause,
        })?;
        let permit = self.binding.reserve().map_err(Rejection::Capacity)?;
        *permit.state_mut(token) = total;
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
    ) -> Option<(T, Front<'lane, 'd, T, W>)> {
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
                quota::Admission::Exhausted if self.is_empty(token) => return Drain::Empty,
                quota::Admission::Exhausted => return Drain::Exhausted,
            }
        }
    }
}

impl<'lane, 'd, T: Unpin + data::raw::Payload<'d>, W: Weight>
    Lane<'lane, 'd, egress::NonEmpty<T>, W>
{
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
                Ok(request::Admission::Ready(ready)) => {
                    ready.push_with(|token| front.release(token));
                    continue;
                }
                Err(value) | Ok(request::Admission::Saturated(value)) => {
                    (value, Ok(()))
                }
                Ok(request::Admission::Rejected(rejected)) => {
                    let (value, cause) = rejected.into_parts();
                    (value, Err(cause))
                }
            };
            front.restore(requests.region(), value);
            return result;
        }
    }
}

impl<'d, T: Unpin, W: Weight> Front<'_, 'd, T, W> {
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
        *held = held.wrapping_sub(self.weight);
    }
}

impl<'turn, 'lane, 'd, T: Unpin, W: Weight> Reservation<'turn, 'lane, 'd, T, W> {
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

impl<T: Unpin, W: Weight> mem::Return for Undo<'_, '_, '_, T, W> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.front.release(self.region);
    }
}

const _: () = {
    const WORD: usize = size_of::<usize>();

    assert!(usize::BITS >= u32::BITS);
    assert!(size_of::<Entry<usize, usize>>() == 2 * WORD);
    assert!(size_of::<Entry<usize, ()>>() == WORD);
    assert!(size_of::<Arena<'static, u64>>() == 5 * WORD);
    assert!(size_of::<Scoped<'static, 'static, u64>>() == size_of::<Arena<'static, u64>>());
    assert!(size_of::<Lane<'static, 'static, u64>>() == 2 * WORD);
    assert!(size_of::<Reservation<'static, 'static, 'static, u64>>() == 4 * WORD);
    assert!(size_of::<Reservation<'static, 'static, 'static, u64, ()>>() == 3 * WORD);
    assert!(size_of::<Front<'static, 'static, u64, usize>>() == 3 * WORD);
};
