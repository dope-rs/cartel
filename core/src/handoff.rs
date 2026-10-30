//! Atomic request publication with reply ownership transfer.

use std::mem;

use o3::base::cell::region;
use reply::{order, recv::attach};

use crate::{queue, reply};

/// The request and reply lanes of one handoff.
pub struct Lane<'a, 's, 'b, 'c, 'd, T, D, E, K> {
    requests: queue::Lane<'a, 'd, T>,
    replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
}

/// An unpublished request reservation with its matching reply lane.
#[must_use = "a prepared handoff must be published or released"]
pub struct Prepared<'turn, 'a, 's, 'b, 'c, 'd, T: Unpin, D, E, K> {
    request: queue::Reservation<'turn, 'a, 'd, T>,
    replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
}

impl<'a, 's, 'b, 'c, 'd, T: Unpin, D, E, K> Lane<'a, 's, 'b, 'c, 'd, T, D, E, K> {
    pub fn new(
        requests: &'a queue::Scoped<'b, 'd, T>,
        replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
    ) -> Self {
        Self {
            requests: requests.lane_for(replies),
            replies,
        }
    }

    /// Reserves the request slot and weight without attaching a reply owner.
    pub fn try_prepare<'turn>(
        self,
        token: &'turn mut region::Token<region::Identity<'d>>,
        weight: usize,
    ) -> Result<Prepared<'turn, 'a, 's, 'b, 'c, 'd, T, D, E, K>, queue::Rejection> {
        Ok(Prepared {
            request: self.requests.try_reserve(token, weight)?,
            replies: self.replies,
        })
    }

    /// Reserves both sides before consuming input or publishing the request.
    pub fn try_publish<I, R>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        weight: usize,
        reply: &mut impl attach::Attach<'s, 'b, 'c, 'd, D, E, K>,
        tag: K,
        input: I,
        build: impl FnOnce(I) -> (T, R),
    ) -> Result<R, (Failure, I)> {
        let prepared = match self.try_prepare(token, weight) {
            Ok(prepared) => prepared,
            Err(cause) => return Err((Failure::Queue(cause), input)),
        };
        prepared
            .try_publish(reply, tag, input, build)
            .map_err(|(cause, input)| (Failure::Reply(cause), input))
    }
}

impl<'a, 's, 'b, 'c, 'd, T: Unpin, D, E, K> Prepared<'_, 'a, 's, 'b, 'c, 'd, T, D, E, K> {
    /// Attaches the reply owner and immediately publishes the reserved request.
    pub fn try_publish<I, R>(
        self,
        reply: &mut impl attach::Attach<'s, 'b, 'c, 'd, D, E, K>,
        tag: K,
        input: I,
        build: impl FnOnce(I) -> (T, R),
    ) -> Result<R, (attach::Error, I)> {
        self.request.try_with(
            input,
            |token, input| match reply.try_tagged(token, self.replies, tag) {
                Ok(()) => Ok(((), input)),
                Err(cause) => Err((cause, input)),
            },
            |input, ()| build(input),
        )
    }

    /// Retains owned preparation input until reply admission succeeds.
    pub fn try_with<I, J, R>(
        self,
        tag: K,
        input: I,
        prepare: impl FnOnce(
            &mut region::Token<region::Identity<'d>>,
            reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
            K,
            I,
        ) -> Result<(order::Depth, J), (attach::Error, I)>,
        build: impl FnOnce(J, order::Depth) -> (T, R),
    ) -> Result<R, (attach::Error, I)> {
        self.request.try_with(
            input,
            |token, input| prepare(token, self.replies, tag, input),
            build,
        )
    }
}

const _: () = assert!(
    mem::size_of::<Lane<'static, 'static, 'static, 'static, 'static, u64, (), (), ()>>()
        == mem::size_of::<queue::Lane<'static, 'static, u64>>()
            + mem::size_of::<reply::Lane<'static, 'static, 'static, 'static, (), (), ()>>()
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Reply(attach::Error),
    Queue(queue::Rejection),
}
