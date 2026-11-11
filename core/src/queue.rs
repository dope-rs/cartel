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

use crate::reply::{self, layout, recv::attach, usage::Footprint};
use o3::{
    alloc,
    base::cell::region,
    depot::{self, queue::pool::fair},
    mem::{
        credit::fair::{Axis, Cause},
        quota,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Reply(attach::Error),
    Queue(Rejection),
}

impl From<Rejection> for Failure {
    fn from(cause: Rejection) -> Self {
        Self::Queue(cause)
    }
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

    pub fn lane_for<D: Footprint, E, K>(
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

    /// Reserves both sides before consuming input or publishing the request.
    pub fn try_publish<'s, 'c, D: Footprint, E, K, I, R>(
        &self,
        replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
        token: &mut region::Token<region::Identity<'d>>,
        weight: W,
        reply: &mut impl attach::Attach<'s, 'b, 'c, 'd, D, E, K>,
        tag: K,
        input: I,
        build: impl FnOnce(I) -> (T, R),
    ) -> Result<R, (Failure, I)> {
        self.try_with(
            replies,
            token,
            weight,
            tag,
            input,
            |token, replies, tag, input| match reply.try_tagged(token, replies, tag) {
                Ok(()) => Ok(((), input)),
                Err(cause) => Err((cause, input)),
            },
            |input, ()| build(input),
        )
    }

    /// Retains owned preparation input until reply admission succeeds.
    pub fn try_with<'s, 'c, D: Footprint, E, K, I, J, P, R>(
        &self,
        replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
        token: &mut region::Token<region::Identity<'d>>,
        weight: W,
        tag: K,
        input: I,
        prepare: impl FnOnce(
            &mut region::Token<region::Identity<'d>>,
            reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
            K,
            I,
        ) -> Result<(P, J), (attach::Error, I)>,
        build: impl FnOnce(J, P) -> (T, R),
    ) -> Result<R, (Failure, I)> {
        self.lane_for(replies).try_with(
            token,
            weight,
            input,
            |token, input| {
                prepare(token, replies, tag, input)
                    .map_err(|(cause, input)| (Failure::Reply(cause), input))
            },
            build,
        )
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
        self.try_with(
            token,
            weight,
            item,
            |_, item| Ok(((), item)),
            |item, ()| (item, ()),
        )
        .map_err(|(cause, value)| Rejected { value, cause })
    }

    pub fn try_with<I, J, P, R, E: From<Rejection>>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        weight: W,
        input: I,
        prepare: impl FnOnce(&mut region::Token<region::Identity<'d>>, I) -> Result<(P, J), (E, I)>,
        build: impl FnOnce(J, P) -> (T, R),
    ) -> Result<R, (E, I)> {
        let front = match self.try_reserve(token, weight) {
            Ok(front) => front,
            Err(cause) => return Err((E::from(cause), input)),
        };
        let (proof, input) = match prepare(token, input) {
            Ok(prepared) => prepared,
            Err(error) => {
                front.release(token);
                return Err(error);
            }
        };
        let (item, output) = build(input, proof);
        front.publish(token, item);
        Ok(output)
    }

    fn try_reserve(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        weight: W,
    ) -> Result<Front<'lane, 'd, T, W>, Rejection> {
        let held = *self.binding.view(token).state().value();
        let total = held.try_add(weight).map_err(|cause| Rejection::Weight {
            lane: self.index(),
            cause,
        })?;
        let permit = self.binding.reserve().map_err(Rejection::Capacity)?;
        *permit.state_mut(token) = total;
        Ok(Front { permit, weight })
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
        mut self,
        requests: &mut request::Requests<'_, '_, '_, 'd, T, KeyTag<ID>>,
    ) -> Result<(), exact::Failure> {
        while let Some(((entry, permit), work)) = requests.claim(&mut self.binding) {
            let Entry { value, weight } = entry;
            let front = Front { permit, weight };
            let (value, result) = match work.admit(value) {
                request::Admission::Ready(ready) => {
                    ready.push_with(|token| front.release(token));
                    continue;
                }
                request::Admission::Saturated(value) => (value, Ok(())),
                request::Admission::Rejected(rejected) => {
                    let (value, cause) = rejected.into_parts();
                    (value, Err(cause))
                }
            };
            front.restore(requests.region(), value);
            return result;
        }
        Ok(())
    }
}

impl<'d, T: Unpin, W: Weight> Front<'_, 'd, T, W> {
    fn publish(self, token: &mut region::Token<region::Identity<'d>>, value: T) {
        self.permit.push_back(
            token,
            Entry {
                value,
                weight: self.weight,
            },
        );
    }

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

const _: () = {
    const WORD: usize = size_of::<usize>();

    assert!(usize::BITS >= u32::BITS);
    assert!(size_of::<Entry<usize, usize>>() == 2 * WORD);
    assert!(size_of::<Entry<usize, ()>>() == WORD);
    assert!(size_of::<Arena<'static, u64>>() == 5 * WORD);
    assert!(size_of::<Scoped<'static, 'static, u64>>() == size_of::<Arena<'static, u64>>());
    assert!(size_of::<Lane<'static, 'static, u64>>() == 2 * WORD);
    assert!(size_of::<Front<'static, 'static, u64, usize>>() == 3 * WORD);
};
