//! Atomic request publication with reply ownership transfer.

use std::mem;

use o3::base::cell::region;
use reply::{recv::attach, usage::Footprint};

use crate::{queue, reply};

/// The request and reply lanes of one handoff.
pub struct Lane<'a, 's, 'b, 'c, 'd, T, D: Footprint, E, K, W: queue::Weight = usize> {
    requests: queue::Lane<'a, 'd, T, W>,
    replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
}

impl<'a, 's, 'b, 'c, 'd, T: Unpin, D: Footprint, E, K, W: queue::Weight>
    Lane<'a, 's, 'b, 'c, 'd, T, D, E, K, W>
{
    pub fn new(
        requests: &'a queue::Scoped<'b, 'd, T, W>,
        replies: reply::Lane<'s, 'b, 'c, 'd, D, E, K>,
    ) -> Self {
        Self {
            requests: requests.lane_for(replies),
            replies,
        }
    }

    /// Reserves both sides before consuming input or publishing the request.
    pub fn try_publish<I, R>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        weight: W,
        reply: &mut impl attach::Attach<'s, 'b, 'c, 'd, D, E, K>,
        tag: K,
        input: I,
        build: impl FnOnce(I) -> (T, R),
    ) -> Result<R, (Failure, I)> {
        self.try_with(
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
    pub fn try_with<I, J, P, R>(
        self,
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
        self.requests.try_with(
            token,
            weight,
            input,
            |token, input| {
                prepare(token, self.replies, tag, input)
                    .map_err(|(cause, input)| (Failure::Reply(cause), input))
            },
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

impl From<queue::Rejection> for Failure {
    fn from(cause: queue::Rejection) -> Self {
        Self::Queue(cause)
    }
}
