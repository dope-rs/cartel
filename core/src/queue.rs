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
    mem::{credit::fair::Cause, quota},
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Drain {
    Empty,
    Exhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rejection {
    Capacity(Cause<usize>),
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
    pub trait Weight<T> {}
    impl<T> Weight<dope::net::link::egress::NonEmpty<T>> for usize {}
    impl<T> Weight<T> for () {}
}

/// Immutable wire lengths or no weight accounting.
pub trait Weight<T>: sealed::Weight<T> + Copy {
    fn of(value: &T) -> Self;
    fn add(self, other: Self) -> Self;
    fn wrapping_sub(self, other: Self) -> Self;
}

impl<T> Weight<egress::NonEmpty<T>> for usize {
    #[inline]
    fn of(value: &egress::NonEmpty<T>) -> Self {
        value.geometry().wire().into_nonzero().get() as usize
    }

    #[inline]
    fn add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }

    #[inline]
    fn wrapping_sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
    }
}

impl<T> Weight<T> for () {
    #[inline]
    fn of(_: &T) -> Self {}

    #[inline]
    fn add(self, _: Self) -> Self {}

    #[inline]
    fn wrapping_sub(self, _: Self) -> Self {}
}

/// Region-owned fixed storage shared fairly by a fixed number of FIFO lanes.
pub struct Arena<'d, T, W = usize> {
    items: fair::Lanes<'d, T, W>,
}

/// A request arena admitted to one exact reply lane domain.
#[repr(transparent)]
pub struct Scoped<'b, 'd, T, W = usize> {
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
pub struct Lane<'a, 'd, T, W = usize> {
    binding: fair::Binding<'a, 'd, T, W>,
}

impl<T, W: Weight<T>> Copy for Lane<'_, '_, T, W> {}

impl<T, W: Weight<T>> Clone for Lane<'_, '_, T, W> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, T: Unpin> Arena<'d, T> {
    pub fn try_new(
        config: fair::Plan,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error>
    where
        usize: Weight<T>,
    {
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

impl<'d, T: Unpin, W: Weight<T>> Arena<'d, T, W> {
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

impl<'b, 'd, T: Unpin, W: Weight<T>> Scoped<'b, 'd, T, W> {
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
        reply: &mut impl attach::Attach<'s, 'b, 'c, 'd, D, E, K>,
        tag: K,
        input: I,
        build: impl FnOnce(I) -> (T, R),
    ) -> Result<R, (Failure, I)> {
        self.try_with(
            replies,
            token,
            tag,
            input,
            |token, replies, tag, input| match reply.try_tagged(token, replies, tag) {
                Ok(()) => Ok(build(input)),
                Err(cause) => Err((cause, input)),
            },
        )
    }

    /// Retains owned preparation input until reply admission succeeds.
    pub fn try_with<'s, 'c, D: Footprint, E, K, I, R>(
        &self,
        replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
        token: &mut region::Token<region::Identity<'d>>,
        tag: K,
        input: I,
        prepare: impl FnOnce(
            &mut region::Token<region::Identity<'d>>,
            reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
            K,
            I,
        ) -> Result<(T, R), (attach::Error, I)>,
    ) -> Result<R, (Failure, I)> {
        self.lane_for(replies).try_with(
            token,
            input,
            |token, input| {
                prepare(token, replies, tag, input)
                    .map_err(|(cause, input)| (Failure::Reply(cause), input))
            },
        )
    }
}

impl<'d, T: Unpin> Lane<'_, 'd, T> {
    pub fn weight(self, token: &region::Token<region::Identity<'d>>) -> usize {
        *self.binding.view(token).state().value()
    }
}

impl<'lane, 'd, T: Unpin, W: Weight<T>> Lane<'lane, 'd, T, W> {
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
    ) -> Result<(), Rejected<T>> {
        self.try_with(token, item, |_, item| Ok((item, ())))
            .map_err(|(cause, value)| Rejected { value, cause })
    }

    pub fn try_with<I, R, E: From<Rejection>>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        input: I,
        prepare: impl FnOnce(&mut region::Token<region::Identity<'d>>, I) -> Result<(T, R), (E, I)>,
    ) -> Result<R, (E, I)> {
        let permit = match self.binding.reserve() {
            Ok(permit) => permit,
            Err(cause) => return Err((E::from(Rejection::Capacity(cause)), input)),
        };
        let (item, output) = prepare(token, input)?;
        // Fair entitlements bound the sum by u32::MAX * i32::MAX on the
        // supported o3 layouts; unpublished reservations carry no wire weight.
        let held = permit.state_mut(token);
        *held = held.add(W::of(&item));
        permit.push_back(token, item);
        Ok(output)
    }

    fn release(
        permit: fair::Permit<'lane, 'd, T, W>,
        token: &mut region::Token<region::Identity<'d>>,
        weight: W,
    ) {
        let held = permit.state_mut(token);
        *held = held.wrapping_sub(weight);
    }

    pub fn pop_front(self, token: &mut region::Token<region::Identity<'d>>) -> Option<T> {
        let (item, permit) = self.binding.take_front(token)?;
        Self::release(permit, token, W::of(&item));
        Some(item)
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

impl<'lane, 'd, T: Unpin + data::raw::Payload<'d>, W: Weight<egress::NonEmpty<T>>>
    Lane<'lane, 'd, egress::NonEmpty<T>, W>
{
    /// Moves source heads through owned exact final-link admission.
    pub fn drain<const ID: u8>(
        mut self,
        requests: &mut request::Requests<'_, '_, '_, 'd, T, KeyTag<ID>>,
    ) -> Result<(), exact::Failure> {
        while let Some(((value, permit), work)) = requests.claim(&mut self.binding) {
            let weight = W::of(&value);
            let (value, result) = match work.admit(value) {
                request::Admission::Ready(ready) => {
                    ready.push_with(|token| Self::release(permit, token, weight));
                    continue;
                }
                request::Admission::Saturated(value) => (value, Ok(())),
                request::Admission::Rejected(rejected) => {
                    let (value, cause) = rejected.into_parts();
                    (value, Err(cause))
                }
            };
            permit.push_front(requests.region(), value);
            return result;
        }
        Ok(())
    }
}

const _: () = {
    const WORD: usize = size_of::<usize>();

    assert!(usize::BITS >= u32::BITS);
    assert!(size_of::<Arena<'static, u64>>() == 5 * WORD);
    assert!(size_of::<Scoped<'static, 'static, u64>>() == size_of::<Arena<'static, u64>>());
    assert!(size_of::<Lane<'static, 'static, u64>>() == 2 * WORD);
};
