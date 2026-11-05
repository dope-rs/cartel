use std::mem;

use cartel_core::{handoff, queue, reply};
use dope::{
    core::driver::route,
    manifold::client::{conn::Id, session::Input},
};
use o3::base::cell::region;
use reply::recv::attach;

use crate::{
    client,
    port::{schedule, schedule::routes::Access},
    protocol::parser::frame,
    request::encode,
};

pub(crate) enum Block {
    Pool,
    Queue(queue::Rejection),
    Unavailable,
}

pub(crate) enum Reject<A> {
    Blocked {
        cause: Block,
        frame: encode::Complete,
        admit: A,
    },
    Unavailable,
    Reply(attach::Error),
}

impl<'app, 's, 'b, 'c, 'd, const ID: u8> schedule::Schedule<'s, 'app, 'b, 'c, 'd, ID> {
    pub(crate) fn try_pool<A, R>(
        &'app self,
        region: &mut region::Token<region::Identity<'d>>,
        frame: encode::Complete,
        admit: A,
        reply: &mut impl attach::Attach<'app, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
        seal: impl FnOnce(A, Id<'d, route::KeyTag<ID>>) -> R,
    ) -> Result<R, Reject<A>> {
        let state = self;
        if state.is_exhausted(region) {
            return Err(Reject::Reply(attach::Error::IdentityExhausted));
        }
        schedule::refresh(state, region);
        let mut blocked = Block::Pool;
        let mut input = (frame, admit, seal);
        for _ in 0..state.routes.len() {
            let Some(conn) = state.routes.next() else {
                break;
            };
            let Some((position, Access::Shared, Input::Empty | Input::Partial)) = conn.access()
            else {
                continue;
            };
            let token = position.connection();
            let reps = state.lane(position);
            match reps.control().registration(region) {
                reply::Registration::Available => {}
                reply::Registration::Full => continue,
                reply::Registration::IdentityExhausted => {
                    return Err(Reject::Reply(attach::Error::IdentityExhausted));
                }
            }
            match handoff::Lane::new(&state.queue, reps).try_publish(
                region,
                0,
                reply,
                (),
                input,
                |(frame, admit, seal)| {
                    conn.unlink::<0>(token);
                    (frame.into_frame(), seal(admit, token))
                },
            ) {
                Ok(done) => {
                    conn.wake();
                    return Ok(done);
                }
                Err((handoff::Failure::Reply(cause), _)) => {
                    return Err(Reject::Reply(cause));
                }
                Err((handoff::Failure::Queue(cause), retained)) => {
                    input = retained;
                    blocked = Block::Queue(cause);
                    continue;
                }
            }
        }
        let cause = if state.routes.is_active() {
            blocked
        } else {
            Block::Unavailable
        };
        let (frame, admit, _) = input;
        Err(Reject::Blocked {
            cause,
            frame,
            admit,
        })
    }

    pub(crate) fn try_dispatch_lease<A, R>(
        &'app self,
        region: &mut region::Token<region::Identity<'d>>,
        frame: encode::Complete,
        admit: A,
        reply: &mut impl attach::Attach<'app, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
        token: Id<'d, route::KeyTag<ID>>,
        seal: impl FnOnce(A, Id<'d, route::KeyTag<ID>>) -> R,
    ) -> Result<R, Reject<A>> {
        let state = self;
        let Some((conn, input)) = state.routes.exclusive(token) else {
            return Err(Reject::Unavailable);
        };
        if matches!(input, Input::Yield | Input::Blocked) {
            return Err(Reject::Blocked {
                cause: Block::Pool,
                frame,
                admit,
            });
        }
        let reps = state.lane(conn.position());
        match handoff::Lane::new(&state.queue, reps).try_publish(
            region,
            0,
            reply,
            (),
            (frame, admit),
            |(frame, admit)| (frame.into_frame(), seal(admit, token)),
        ) {
            Ok(done) => {
                conn.wake();
                Ok(done)
            }
            Err((handoff::Failure::Queue(cause), (frame, admit))) => Err(Reject::Blocked {
                cause: Block::Queue(cause),
                frame,
                admit,
            }),
            Err((handoff::Failure::Reply(cause), _)) => Err(Reject::Reply(cause)),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Block>() == mem::size_of::<crate::Block>());
};
