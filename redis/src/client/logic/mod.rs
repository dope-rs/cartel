use codec::storage;
use core::convert::Infallible;

use cartel_core::{frame::Buffer, reply::order::Cutoff};
use codec::mode;
use codec::staging::Limit;
use dope::{
    core::{Errno, driver, driver::route, io::socket},
    manifold::{
        client::{
            app::{self, admit::Registered, connect::Full, failure, request},
            attempt::{self, ConnectFault},
            close, conn,
            session::{self, app::drain::Application, codec, policy, protocol, response, turns},
        },
        service::bindings::endpoint::State,
    },
    net::link::{egress, pool::change::opening},
};
use driver::schedule::{Progress, app::Work, clock};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer::{self, hold, resident},
};
use output::{Cancel, Sent};
use policy::{admit, output};

use crate::{
    Owner, Port,
    client::{
        self, CONNECTOR_CONNECTION_LIMIT,
        settings::init::{Plan, Step},
    },
    port,
    protocol::{Phase, parser::Parser},
};
mod pending;
pub(super) use pending::Pending;
pub(in crate::client) mod init;
pub(in crate::client) mod reply;

pub trait Root<'d, const ID: u8> {
    type Port;
    type Failure: failure::Mode;

    fn port(&self) -> &Self::Port;

    fn complete(
        &self,
        _: &attempt::Completion<'_, 'd, route::KeyTag<ID>>,
        _: Result<(), ConnectFault>,
    ) {
    }

    fn retired(&self, reason: close::Reason);
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> Root<'d, ID> for Port<'s, 'storage, 'b, 'c, 'd, ID> {
    type Port = Self;
    type Failure = failure::Controlled;

    fn port(&self) -> &Self {
        self
    }

    fn retired(&self, _: close::Reason) {
        self.activity.wake_all();
    }
}

pub struct Logic<'s, 'd> {
    pub(in crate::client) codec: crate::protocol::Codec<'s, 'd>,
    pub(in crate::client) init: Plan,
}

impl<'s, 'd> Logic<'s, 'd> {
    pub(crate) fn new<const ID: u8>(port: &Port<'s, '_, '_, '_, 'd, ID>, plan: Plan) -> Self {
        Self {
            codec: port.codec(),
            init: plan,
        }
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    protocol::Spec<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    type Codec = crate::protocol::Codec<'s, 'd>;
    type Connect = Full;
    type ConnState = crate::protocol::ConnState;
    type Failure = P::Failure;
    type Mode = mode::Stored;
    type Requests = request::Enabled;
    type Response = response::Resume<Pending<'s, 'd>, Pending<'s, 'd>>;
    type Receipt = (Option<Step>, Cutoff<'b>);
    type Send = Buffer;
    fn codec<'a>(owner: &'a Owner<'r, 's, 'd, P, ID>) -> &'a Self::Codec {
        &owner.logic().codec
    }

    fn receipt(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &Self::ConnState,
        region: &region::Token<region::Identity<'d>>,
    ) -> Self::Receipt {
        let step = match state.phase() {
            Phase::Init(step) => Some(step),
            _ => None,
        };
        let cutoff = owner
            .root()
            .port()
            .schedule
            .responses(conn)
            .map_or(Cutoff::EMPTY, |lane| lane.cutoff(region));
        (step, cutoff)
    }

    fn received<'e>(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        (step, cutoff): Self::Receipt,
        has_prefix: bool,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) {
        ctx.state.received(step, has_prefix);
        owner
            .root()
            .port()
            .schedule
            .received(ctx.conn_id, cutoff, has_prefix, ctx.region());
    }
}
impl<'r, 's: 'r, 'storage: 'r, 'b: 'r, 'c: 'r, 'd: 'r, P: 'r, const ID: u8>
    policy::Output<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn sent() -> impl for<'a, 'x> Call<(
        &'a mut Owner<'r, 's, 'd, P, ID>,
        conn::Id<'d, route::KeyTag<ID>>,
        egress::Pending,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        Sent::<_>::Ready(|owner: &mut State<P, _, _, _>| owner.root().port().activity.wake_send())
    }

    fn flush() -> Option<
        impl for<'a, 'x, 'y, 'z> Call<(
            &'a mut Owner<'r, 's, 'd, P, ID>,
            &'x mut turns::Context<'y, 'z, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
            session::Input,
        )>,
    > {
        Some(
            |(owner, ctx, input): (
                &mut Owner<'r, 's, 'd, P, ID>,
                &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
                session::Input,
            )| {
                let port = owner.root().port();
                if let Phase::Advance(step) = ctx.state.phase() {
                    owner.logic().init.advance(port, step, input, ctx);
                    return;
                }
                if matches!(ctx.state.phase(), Phase::Unsolicited) {
                    if matches!(input, session::Input::Partial)
                        && matches!(ctx.close_status(), conn::CloseStatus::Open)
                    {
                        reply::fail(port, client::Failure::UnexpectedResponse, ctx);
                    }
                    return;
                }
                match port.schedule.try_input(ctx.conn_id, input) {
                    Ok(()) => port.activity.wake_send(),
                    Err(cause) => reply::fail(port, cause, ctx),
                }
            },
        )
    }

    fn cancel() -> impl for<'a, 'x> Call<(
        &'a Owner<'r, 's, 'd, P, ID>,
        Self::Send,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop,
            |owner: &State<P, _, _, _>| owner.root().port().activity.wake_send(),
        )
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Requests<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn drain<'e>(
        owner: &Owner<'r, 's, 'd, P, ID>,
        reqs: &mut request::Requests<'_, '_, 'e, 'd, Self::Send, route::KeyTag<ID>>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let port = owner.root().port();
        let token = reqs.connection();
        let Err(cause) = port.schedule.drain(reqs) else {
            return;
        };
        reqs.close(close::Directive::cut_retry(cause.close_reason()));
        port.fail(token, cause, reqs.region());
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    admit::Register<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn register(
        owner: &Owner<'r, 's, 'd, P, ID>,
        entry: &conn::Activation<'_, 'd, route::KeyTag<ID>>,
    ) -> app::Admission {
        let port = owner.root().port();
        match port.schedule.activate(entry) {
            Ok(()) => app::Admission::Attached,
            Err(reason) => {
                port.activity
                    .record_stop(port::Stop::Closed(reason.reason()));
                owner.root().complete(entry.completion(), Ok(()));
                app::Admission::Rejected(reason)
            }
        }
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Connected<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    type Activation = Registered;

    fn connect<'e>(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        key: attempt::Completion<'_, 'd, route::KeyTag<ID>>,
        _: socket::Addr,
        ctx: turns::Startup<'_, 'e, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) {
        let store = owner.root().port();
        let logic = owner.logic_mut();
        logic.init.start(store, ctx);
        owner.root().complete(&key, Ok(()));
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Exhausted<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn exhaust(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        key: attempt::Completion<'_, 'd, route::KeyTag<ID>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        owner
            .root()
            .port()
            .activity
            .record_stop(port::Stop::Exhausted {
                key: key.id(),
                cause,
            });
        owner.root().complete(&key, Ok(()));
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Failed<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>, Failure = failure::Observed>,
{
    fn failed(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        key: attempt::Completion<'_, 'd, route::KeyTag<ID>>,
        cause: ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        owner.root().complete(&key, Err(cause));
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Lifecycle<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    type Drain = Application;

    fn transmit_failed(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        failure: conn::Transmit,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) -> Option<conn::Transmit> {
        owner
            .root()
            .port()
            .fail(conn, client::Failure::Transmit(failure), &mut regions.0);
        None
    }

    fn io_fail<'e>(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        error: Errno,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) {
        owner
            .root()
            .port()
            .fail(ctx.conn_id, client::Failure::Receive(error), ctx.region());
    }

    fn disconnect(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        ctx: turns::Retired<'_, 'd, Self::ConnState, route::KeyTag<ID>, Owner<'r, 's, 'd, P, ID>>,
    ) {
        let reason = ctx.reason();
        owner
            .root()
            .port()
            .schedule
            .routes
            .retire(ctx.into_drained());
        owner.root().retired(reason);
    }
    fn quarantine(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        owner
            .root()
            .port()
            .schedule
            .quarantine(conn, reason, &mut regions.0);
    }
    fn clear_send<'turn>(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let port = owner.root().port();
        port.schedule.clear_send(conn, ctx, &port.activity)
    }
    fn clear_recv<'turn>(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        _: close::Reason,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        owner.root().port().schedule.clear_recv(conn, ctx)
    }
    fn fence(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        _: &Self::ConnState,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        use cartel_core::reply::order::Depth;

        match owner.root().port().schedule.responses(conn) {
            Some(reps) if reps.depth(region) != Depth::ZERO => app::Fence::Pending,
            Some(_) | None => app::Fence::Ready,
        }
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Quiescence<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn quiesce<'a>(
        _: &Owner<'r, 's, 'd, P, ID>,
        barrier: app::Barrier<'a, 'd, route::KeyTag<ID>>,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'a, 'd, route::KeyTag<ID>> {
        barrier.ready()
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    policy::Scheduling<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn park<'turn>(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        work: Work<'turn, 'd>,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let port = owner.root().port();
        if let Some(_released) = port.schedule.maintain(work, &mut region.0) {
            port.activity.wake_all();
        }
        port.activity.flush(work);
    }
    fn shutdown(owner: &mut Owner<'r, 's, 'd, P, ID>) {
        owner.root().port().activity.wake_all();
    }
    fn progress(
        owner: &Owner<'r, 's, 'd, P, ID>,
        _: &region::Token<region::Identity<'d>>,
        _clock: clock::Clock<'_, 'd>,
    ) -> Progress<'d> {
        use cartel_core::reply::Work;

        let port = owner.root().port();
        match port.schedule.work() {
            Work::Pending => Progress::Runnable,
            Work::Idle if port.activity.is_pending() => Progress::Runnable,
            Work::Idle => Progress::Quiescent,
        }
    }
    fn inbound(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &Self::ConnState,
        base: Positive,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        use cartel_core::reply::order::Depth;

        match state.phase() {
            Phase::Init(_) | Phase::Advance(_) | Phase::Unsolicited => app::Inbound::Awaiting(base),
            Phase::Offline | Phase::Ready => match owner.root().port().schedule.responses(conn) {
                Some(reps) if reps.depth(region) != Depth::ZERO => app::Inbound::Awaiting(base),
                Some(_) | None => app::Inbound::Quiescent,
            },
        }
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, P, const ID: u8>
    protocol::Stored<'r, 'd, P, route::KeyTag<ID>, CONNECTOR_CONNECTION_LIMIT> for Logic<'s, 'd>
where
    's: 'b,
    'b: 'storage,
    'c: 'b,
    'd: 'c,
    P: Root<'d, ID, Port = Port<'s, 'storage, 'b, 'c, 'd, ID>>,
{
    fn blocked(
        owner: &Owner<'r, 's, 'd, P, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
    ) -> session::Blocking {
        let port = owner.root().port();
        if let Some(target) = port.schedule.routes.target(conn) {
            match state.builder.access.wait(conn, target) {
                Ok(()) => session::Blocking::Registered,
                Err(cause) => {
                    port.activity.record_stop(port::Stop::ParserWait(cause));
                    session::Blocking::Unsupported
                }
            }
        } else {
            session::Blocking::Unsupported
        }
    }

    fn decode<S>(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        _: conn::Id<'d, route::KeyTag<ID>>,
        storage: &mut <Self::Codec as codec::Stateful>::Storage<S>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) -> codec::Step<<Self::Response as response::Plan>::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let codec = owner.logic().codec;
        codec::Stateful::decode(&codec, storage, state, budget, limit, |_| {
            codec::Admission::Ready(|decoded, _: &mut Parser<'s, 'd>| {
                reply::deliver(owner.root().port(), decoded, ctx)
            })
        })
    }

    fn finish<'slot, 'id, S>(
        owner: &mut Owner<'r, 's, 'd, P, ID>,
        input: storage::Owner<
            'slot,
            'id,
            <Self::Codec as codec::Stateful>::Storage<S>,
            <Self::Codec as codec::Stateful>::State<'d>,
        >,
        budget: &resident::Budget<'d>,
        limit: Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) -> (
        storage::Owner<
            'slot,
            'id,
            <Self::Codec as codec::Stateful>::Storage<S>,
            <Self::Codec as codec::Stateful>::State<'d>,
        >,
        Result<
            codec::Finish<<Self::Response as response::Plan>::Delivery>,
            <Self::Response as response::Plan>::Delivery,
        >,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let codec = owner.logic().codec;
        codec::Stateful::finish(&codec, input, budget, limit, |_| {
            codec::Admission::Ready(|decoded, _: &mut Parser<'s, 'd>| {
                reply::deliver(owner.root().port(), decoded, ctx)
            })
        })
    }
}
