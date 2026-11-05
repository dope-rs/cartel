use codec::storage;
use core::convert::Infallible;

use cartel_core::frame::Buffer;
use codec::mode;
use codec::staging::Limit;
use dope::{
    core::{Errno, driver, driver::route, io::socket},
    manifold::{
        client::{
            app::{self, admit::Registered, connect::Full, failure::Controlled, request},
            attempt, close, conn,
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
use output::{Cancel, Flush, Sent};
use policy::{admit, output};

use crate::{
    Owner, Port,
    client::{self, CONNECTOR_CONNECTION_LIMIT, settings::init::Plan},
    port,
    protocol::parser::Parser,
};
mod pending;
pub(super) use pending::Pending;
pub(in crate::client) mod init;
pub(in crate::client) mod reply;

pub struct Logic<'s, 'd> {
    pub(in crate::client) codec: crate::protocol::Codec<'s, 'd>,
    pub(in crate::client) init: init::Init,
}

impl<'s, 'd> Logic<'s, 'd> {
    pub(crate) fn new<const ID: u8>(port: &Port<'s, '_, '_, '_, 'd, ID>, plan: Plan) -> Self {
        Self {
            codec: port.codec(),
            init: init::Init::new(plan),
        }
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    protocol::Spec<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    type Codec = crate::protocol::Codec<'s, 'd>;
    type Connect = Full;
    type ConnState = crate::protocol::ConnState;
    type Failure = Controlled;
    type Mode = mode::Stored;
    type Requests = request::Enabled;
    type Response = response::Resume<Pending<'s, 'd>, Pending<'s, 'd>>;
    type Send = Buffer;
    fn codec<'a>(owner: &'a Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>) -> &'a Self::Codec {
        &owner.logic().codec
    }
}
impl<'r, 's: 'r, 'storage: 'r, 'b: 'r, 'c: 'r, 'd: 'r, const ID: u8>
    policy::Output<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn sent() -> impl for<'a, 'x> Call<(
        &'a mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn::Id<'d, route::KeyTag<ID>>,
        egress::Pending,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        Sent::<_>::Ready(|owner: &mut State<Port<ID>, _, _, _>| owner.root().activity.wake_send())
    }

    fn flush() -> Option<
        impl for<'a, 'x, 'y, 'z> Call<(
            &'a mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
            &'x mut turns::Context<'y, 'z, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
            session::Input,
        )>,
    > {
        Some(Flush::<Infallible, _>::Context(
            |(owner, ctx, input): (
                &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
                &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
                session::Input,
            )| {
                let port = owner.root();
                match port.schedule.try_input(ctx.conn_id, input, ctx.region()) {
                    Ok(()) => port.activity.wake_send(),
                    Err(cause) => reply::fail(port, cause, ctx),
                }
            },
        ))
    }

    fn cancel() -> impl for<'a, 'x> Call<(
        &'a Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        Self::Send,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop,
            |owner: &State<Port<ID>, _, _, _>| owner.root().activity.wake_send(),
        )
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    policy::Requests<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn drain<'e>(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        reqs: &mut request::Requests<'_, '_, 'e, 'd, Self::Send, route::KeyTag<ID>>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let port = owner.root();
        let token = reqs.connection();
        let Err(cause) = port.schedule.drain(reqs) else {
            return;
        };
        reqs.close(close::Directive::cut_retry(cause.close_reason()));
        port.fail(token, cause, reqs.region());
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    admit::Register<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn register(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        entry: &conn::Activation<'_, 'd, route::KeyTag<ID>>,
    ) -> app::Admission {
        let port = owner.root();
        match port.schedule.activate(entry) {
            Ok(()) => app::Admission::Attached,
            Err(reason) => {
                port.activity
                    .record_stop(port::Stop::Failed(client::Failure::from_close(
                        reason.reason(),
                    )));
                app::Admission::Rejected(reason)
            }
        }
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    policy::Connected<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    type Activation = Registered;

    fn connect<'e>(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        _: attempt::Completion<'_, 'd, route::KeyTag<ID>>,
        _: socket::Addr,
        ctx: turns::Startup<'_, 'e, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) {
        let store = owner.root();
        let logic = owner.logic_mut();
        logic.init.start(store, ctx);
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    policy::Exhausted<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn exhaust(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        key: attempt::Completion<'_, 'd, route::KeyTag<ID>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        owner.root().activity.record_stop(port::Stop::Exhausted {
            key: key.id(),
            cause,
        });
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    policy::Lifecycle<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    type Drain = Application;

    fn transmit_failed(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        failure: conn::Transmit,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Option<conn::Transmit> {
        owner
            .root()
            .fail(conn, client::Failure::Transmit(failure), region);
        None
    }

    fn io_fail<'e>(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        error: Errno,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, route::KeyTag<ID>>,
    ) {
        owner
            .root()
            .fail(ctx.conn_id, client::Failure::Receive(error), ctx.region());
    }

    fn disconnect(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        ctx: turns::Retired<
            '_,
            'd,
            Self::ConnState,
            route::KeyTag<ID>,
            Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        >,
    ) {
        owner.root().schedule.retire(ctx.into_drained());
        owner.root().activity.wake_all();
    }
    fn quarantine(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
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
            .schedule
            .quarantine(conn, reason, &mut regions.0);
    }
    fn clear_send<'turn>(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        owner.root().schedule.clear_send(conn, ctx)
    }
    fn clear_recv<'turn>(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        _: close::Reason,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        owner.root().schedule.clear_recv(conn, ctx)
    }
    fn fence(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        _: &Self::ConnState,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        use cartel_core::reply::order::Depth;

        match owner.root().schedule.responses(conn) {
            Some(reps) if reps.depth(region) != Depth::ZERO => app::Fence::Pending,
            Some(_) | None => app::Fence::Ready,
        }
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    policy::Quiescence<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn quiesce<'a>(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        barrier: app::Barrier<'a, 'd, route::KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'a, 'd, route::KeyTag<ID>> {
        owner.root().schedule.quiesce(barrier, region)
    }
}
impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    policy::Scheduling<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn park<'turn>(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        work: Work<'turn, 'd>,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let port = owner.root();
        if let Some(_released) = port.schedule.maintain(work, &mut region.0) {
            port.activity.wake_all();
        }
        port.activity.flush(work);
    }
    fn shutdown(owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>) {
        owner.root().activity.wake_all();
    }
    fn progress(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        _: &region::Token<region::Identity<'d>>,
        _clock: clock::Clock<'_, 'd>,
    ) -> Progress<'d> {
        use cartel_core::reply::Work;

        let port = owner.root();
        match port.schedule.work() {
            Work::Pending => Progress::Runnable,
            Work::Idle if port.activity.is_pending() => Progress::Runnable,
            Work::Idle => Progress::Quiescent,
        }
    }
    fn inbound(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &Self::ConnState,
        base: Positive,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        use crate::protocol::Phase;
        use cartel_core::reply::order::Depth;

        match state.phase() {
            Phase::Init(_) => app::Inbound::Awaiting(base),
            Phase::Offline | Phase::Ready => match owner.root().schedule.responses(conn) {
                Some(reps) if reps.depth(region) != Depth::ZERO => app::Inbound::Awaiting(base),
                Some(_) | None => app::Inbound::Quiescent,
            },
        }
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    protocol::Stored<
        'r,
        'd,
        Port<'s, 'storage, 'b, 'c, 'd, ID>,
        route::KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Logic<'s, 'd>
{
    fn blocked(
        owner: &Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
    ) -> session::Blocking {
        let port = owner.root();
        if let Some(target) = port.schedule.routes.target(conn) {
            match state.builder.access.wait(conn, target) {
                Ok(()) => session::Blocking::Registered,
                Err(cause) => {
                    port.activity
                        .record_stop(port::Stop::Failed(client::Failure::ParserWait(cause)));
                    session::Blocking::Unsupported
                }
            }
        } else {
            session::Blocking::Unsupported
        }
    }

    fn decode<S>(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
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
                let store = owner.root();
                let logic = owner.logic_mut();
                reply::deliver(&logic.init, store, decoded, ctx)
            })
        })
    }

    fn finish<'slot, 'id, S>(
        owner: &mut Owner<'r, 's, 'storage, 'b, 'c, 'd, ID>,
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
                let store = owner.root();
                let logic = owner.logic_mut();
                reply::deliver(&logic.init, store, decoded, ctx)
            })
        })
    }
}
