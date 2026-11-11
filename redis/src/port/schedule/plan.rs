use cartel_core::{queue, reply};
use dope::{core::driver::route, manifold::client::conn::Id};
use o3::base::cell::region;
use reply::recv::attach;

use crate::{
    Block, Error, client,
    port::{schedule, schedule::routes::Access},
    protocol::parser::frame,
    request::encode,
};

pub(crate) enum Reject<'d, const ID: u8> {
    Blocked {
        cause: Block,
        frame: encode::Complete,
    },
    Failed(Error<'d, ID>),
}

impl<'app, 's, 'b, 'c, 'd, const ID: u8> schedule::Schedule<'s, 'app, 'b, 'c, 'd, ID> {
    pub(crate) fn try_pool(
        &'app self,
        region: &mut region::Token<region::Identity<'d>>,
        mut frame: encode::Complete,
        reply: &mut impl attach::Attach<'app, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
    ) -> Result<Id<'d, route::KeyTag<ID>>, Reject<'d, ID>> {
        if self.is_exhausted(region) {
            return Err(Reject::Failed(Error::Reply(
                attach::Error::IdentityExhausted,
            )));
        }
        let mut blocked = Block::Pool;
        for _ in 0..self.routes.len() {
            let Some(conn) = self.routes.next() else {
                break;
            };
            let Some((position, Access::Shared)) = conn.access() else {
                continue;
            };
            let token = position.connection();
            let reps = self.lane(position);
            match reps.control().registration(region) {
                reply::Registration::Available => {}
                reply::Registration::Full => continue,
                reply::Registration::IdentityExhausted => {
                    return Err(Reject::Failed(Error::Reply(
                        attach::Error::IdentityExhausted,
                    )));
                }
            }
            match self
                .queue
                .try_publish(reps, region, (), reply, (), frame, |frame| {
                    conn.unlink::<0>(token);
                    (frame.into_frame(), token)
                }) {
                Ok(done) => {
                    conn.wake();
                    return Ok(done);
                }
                Err((queue::Failure::Reply(cause), _)) => {
                    return Err(Reject::Failed(Error::Reply(cause)));
                }
                Err((queue::Failure::Queue(cause), retained)) => {
                    frame = retained;
                    blocked = Block::Queue(cause);
                }
            }
        }
        let cause = if self.routes.is_active() {
            blocked
        } else {
            Block::Inactive
        };
        Err(Reject::Blocked { cause, frame })
    }

    pub(crate) fn try_dispatch_lease(
        &'app self,
        region: &mut region::Token<region::Identity<'d>>,
        frame: encode::Complete,
        reply: &mut impl attach::Attach<'app, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
        token: Id<'d, route::KeyTag<ID>>,
    ) -> Result<Id<'d, route::KeyTag<ID>>, Reject<'d, ID>> {
        let Some(conn) = self.routes.exclusive(token) else {
            return Err(Reject::Failed(Error::Unavailable));
        };
        let reps = self.lane(conn.position());
        match self
            .queue
            .try_publish(reps, region, (), reply, (), frame, |frame| {
                (frame.into_frame(), token)
            }) {
            Ok(done) => {
                conn.wake();
                Ok(done)
            }
            Err((queue::Failure::Queue(cause), frame)) => Err(Reject::Blocked {
                cause: Block::Queue(cause),
                frame,
            }),
            Err((queue::Failure::Reply(cause), _)) => Err(Reject::Failed(Error::Reply(cause))),
        }
    }
}
