use cartel_core::{frame::Buffer, queue, reply};
use client::settings;
use dope::{
    core::driver::{route, schedule::app::Work},
    manifold::client::{
        app::{self, request},
        close,
        conn::{Activation, Id},
        session::{self, turns},
    },
    net::link::egress,
};
use dope_fiber::reply::{layout::Position, order::Depth};
pub(crate) use lease::Lease;
use o3::base::cell::region;
pub(crate) use plan::{Block, Plan, Reject};
pub(crate) use routes::Routes;
use routes::{Access, Acquire, Bind};

use crate::client::plan::Error;
use crate::{Owner, client, port, protocol::parser::frame};
use routes::slot;

mod lease;
mod plan;
mod routes;

#[derive(Clone, Copy)]
pub(crate) enum Ready {
    Committed,
    Rejected,
}

enum Release {
    Shared,
    Retired,
}

pub(crate) struct Schedule<'s, 'storage, 'b, 'c, 'd, const ID: u8> {
    pub(crate) routes: Routes<'b, 'd, ID>,
    pub(super) queue: queue::Scoped<'b, 'd, egress::NonEmpty<Buffer>>,
    responses: &'storage reply::Arena<'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
}

enum Link {
    Unchanged,
    Added,
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> Schedule<'s, 'storage, 'b, 'c, 'd, ID> {
    fn requests(
        &self,
        position: Position<'b, 'd, route::KeyTag<ID>>,
    ) -> queue::Lane<'_, 'd, egress::NonEmpty<Buffer>> {
        self.queue.lane(position)
    }

    pub(super) fn try_new(
        config: settings::Config,
        token: &region::Token<region::Identity<'d>>,
        replies: &'storage reply::Arena<'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
    ) -> Result<Self, Error> {
        let conns = config.connections();
        Ok(Self {
            routes: Routes::try_new(conns)?,
            queue: queue::Arena::try_new(config.queue(), token)?
                .try_bind(replies.domain())
                .map_err(Error::Queue)?,
            responses: replies,
        })
    }

    pub(crate) fn activate(
        &self,
        act: &Activation<'_, 'd, route::KeyTag<ID>>,
    ) -> Result<(), close::Directive> {
        let token = act.connection();
        let ready = act.ready();
        let lane = act.lane();
        let Some(conn) = self.routes.get(lane.slot().index().into_usize()) else {
            return Err(activation_rejected());
        };
        let position =
            Position::try_new(self.responses.domain(), token).map_err(|_| activation_rejected())?;
        let response = self.lane(position);
        if matches!(conn.bind(position, ready), Bind::Occupied) {
            return Err(activation_rejected());
        }
        response.bind_retire(ready);
        Ok(())
    }

    pub(crate) fn mark_ready(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
        active: &port::Activity<'b, 'd, ID>,
    ) -> Ready {
        let Some(conn) = self.routes.mark_ready(token) else {
            return Ready::Rejected;
        };
        let position = conn.position();
        let reps = self.lane(position);
        let lane = self.requests(position);
        publish(&conn, reps, lane, region);
        active.clear_fatal(position, region);
        active.wake_all();
        Ready::Committed
    }

    pub(crate) fn retire<'r>(
        &self,
        proof: turns::Drained<'d, route::KeyTag<ID>, Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>>,
    ) {
        self.routes.retire(proof);
    }

    pub(crate) fn responses(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
    ) -> Option<reply::Lane<'_, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>> {
        let conn = self.routes.bound(token)?;
        Some(self.lane(conn.position()))
    }

    pub(crate) fn try_input(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        input: session::Input,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), client::Failure<'d>> {
        let Some(conn) = self.routes.get(token.index().into_usize()) else {
            return Ok(());
        };
        let Some(position) = conn.set_input(token, input) else {
            return Ok(());
        };
        if input == session::Input::Partial && self.lane(position).depth(region) == Depth::ZERO {
            return Err(client::Failure::UnexpectedResponse);
        }
        Ok(())
    }

    pub(super) fn fail(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        err: client::Failure<'d>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Option<Position<'b, 'd, route::KeyTag<ID>>> {
        let conn = self.routes.fail(token, err)?;
        let position = conn.position();
        withdraw(&conn, self.lane(position), region);
        Some(position)
    }

    pub(crate) fn reconnect(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        err: client::Failure<'d>,
        region: &mut region::Token<region::Identity<'d>>,
        active: &port::Activity<'b, 'd, ID>,
    ) {
        let Some(conn) = self.routes.reconnect(token, err) else {
            return;
        };
        withdraw(&conn, self.lane(conn.position()), region);
        active.wake_all();
    }

    pub(crate) fn quarantine(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        reason: close::Reason,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        self.fail(token, client::Failure::from_close(reason), region);
    }

    pub(crate) fn clear_recv(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        reason: close::Reason,
        ctx: &mut turns::Retirement<'_, '_, 'd>,
    ) -> egress::Cleared {
        use dope_fiber::reply::{Progress, Retirement};

        let (work, region) = ctx.parts();
        self.quarantine(token, reason, region);
        let Some((conn, cause)) = self.routes.failure(token) else {
            return egress::Cleared::Done;
        };
        let reps = self.lane(conn.position());
        match Retirement::new(region).fail(work, reps, &cause) {
            Progress::Pending(_) => return egress::Cleared::Retry,
            Progress::Complete(_) => {}
        }
        let reqs = self.requests(conn.position());
        if reqs.is_empty(region) && reps.depth(region) == Depth::ZERO {
            egress::Cleared::Done
        } else {
            egress::Cleared::Retry
        }
    }

    pub(crate) fn clear_send(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        ctx: &mut turns::Retirement<'_, '_, 'd>,
    ) -> egress::Cleared {
        let Some(conn) = self.routes.bound(token) else {
            return egress::Cleared::Done;
        };
        let reqs = self.requests(conn.position());
        let (work, region) = ctx.parts();
        match reqs.clear(work, region, drop) {
            queue::Drain::Empty => egress::Cleared::Done,
            queue::Drain::Exhausted => egress::Cleared::Retry,
        }
    }
}

fn activation_rejected() -> close::Directive {
    close::Directive::cut_final(close::Reason::Local(close::LocalOrigin::ResourceFailure))
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> Schedule<'s, 'storage, 'b, 'c, 'd, ID> {
    pub(crate) fn try_lease<'app, B: ?Sized>(
        &'app self,
        redis: client::Redis<'app, 's, 'b, 'c, 'd, B, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Option<Lease<'app, 's, 'b, 'c, 'd, B, ID>> {
        refresh(self, region);
        for _ in 0..self.routes.len() {
            let conn = self.routes.pop::<0>()?;
            let token = conn.connection();
            let reps = self.lane(conn.position());
            let reqs = self.requests(conn.position());
            match conn.acquire(token, reqs, reps, region) {
                Acquire::Acquired => {}
                Acquire::Blocked => {
                    conn.schedule::<1>(conn.position());
                    continue;
                }
                Acquire::Stale => continue,
            }
            reps.control().deactivate(region);
            return Some(Lease::new(redis, conn));
        }
        None
    }

    pub(crate) fn drain<'e>(
        &self,
        reqs: &mut request::Requests<'_, '_, 'e, 'd, Buffer, route::KeyTag<ID>>,
    ) -> Result<(), client::Failure<'d>> {
        let token = reqs.connection();
        let Some(conn) = self.routes.bound(token) else {
            return Ok(());
        };
        let lane = self.requests(conn.position());
        if let Some(reason) = conn.retry_reason(token) {
            reqs.close(close::Directive::cut_retry(reason));
            return Ok(());
        }
        lane.drain(reqs).map_err(client::Failure::RequestReserve)
    }

    pub(crate) fn complete(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
        active: &port::Activity<'b, 'd, ID>,
    ) {
        let Some(conn) = self.routes.shared(token) else {
            return;
        };
        let reps = self.lane(conn.position());
        let reqs = self.requests(conn.position());
        match publish(&conn, reps, reqs, region) {
            Link::Unchanged => {}
            Link::Added => active.wake_lease(),
        }
    }

    pub(super) fn release(
        &self,
        conn: &slot::Bound<'_, 'b, 'd, ID>,
        active: &port::Activity<'b, 'd, ID>,
    ) {
        if let Release::Shared = conn.release(conn.connection()) {
            conn.schedule::<1>(conn.position());
            active.wake_all();
        }
    }
}

fn withdraw<'s, 'b, 'c, 'd, const ID: u8>(
    conn: &slot::Bound<'_, 'b, 'd, ID>,
    reps: reply::Lane<'_, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
    region: &mut region::Token<region::Identity<'d>>,
) {
    let token = conn.connection();
    conn.unlink::<0>(token);
    conn.unlink::<1>(token);
    reps.control().deactivate(region);
}

fn publish<'s, 'b, 'c, 'd, const ID: u8>(
    conn: &slot::Bound<'_, 'b, 'd, ID>,
    reps: reply::Lane<'_, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
    reqs: queue::Lane<'_, 'd, egress::NonEmpty<Buffer>>,
    region: &mut region::Token<region::Identity<'d>>,
) -> Link {
    reps.control().activate(region);
    if reps.depth(region) != Depth::ZERO || !reqs.is_empty(region) {
        return Link::Unchanged;
    }
    conn.schedule::<0>(conn.position());
    Link::Added
}

fn refresh<'s, 'storage, 'b, 'c, 'd, const ID: u8>(
    state: &Schedule<'s, 'storage, 'b, 'c, 'd, ID>,
    region: &mut region::Token<region::Identity<'d>>,
) {
    let mut deferred = None;
    while state.routes.front::<1>() != deferred {
        let Some(conn) = state.routes.pop::<1>() else {
            return;
        };
        let token = conn.connection();
        if !matches!(conn.access(), Some((position, Access::Shared, _)) if position.connection() == token)
        {
            continue;
        }
        let reps = state.lane(conn.position());
        let reqs = state.requests(conn.position());
        match publish(&conn, reps, reqs, region) {
            Link::Unchanged => {
                deferred.get_or_insert(token);
                conn.schedule::<1>(conn.position());
            }
            Link::Added => {}
        }
    }
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> Schedule<'s, 'storage, 'b, 'c, 'd, ID> {
    pub(crate) fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, route::KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'a, 'd, route::KeyTag<ID>> {
        let token = barrier.connection();
        let Some(conn) = self.routes.bound(token) else {
            return barrier.pending();
        };
        let reqs = self.requests(conn.position());
        let reps = self.lane(conn.position());
        if conn.is_closing(token) && reqs.is_empty(region) && reps.depth(region) == Depth::ZERO {
            barrier.ready()
        } else {
            barrier.pending()
        }
    }
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> Schedule<'s, 'storage, 'b, 'c, 'd, ID> {
    pub(crate) fn lane(
        &self,
        position: Position<'b, 'd, route::KeyTag<ID>>,
    ) -> reply::Lane<'_, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>> {
        self.responses.lane(position)
    }

    fn is_exhausted(&self, token: &region::Token<region::Identity<'d>>) -> bool {
        self.responses.is_exhausted(token)
    }

    pub(crate) fn maintain(
        &self,
        work: Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<reply::Released<'b>> {
        self.responses.maintain(work, token)
    }

    pub(crate) fn work(&self) -> reply::Work {
        self.responses.work()
    }
}
