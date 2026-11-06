use bindings::endpoint;
use cartel_core::reply;
use codec::mode;
use dope::{
    core::{Errno, driver, driver::route::KeyTag, io::socket},
    manifold::{
        client::{
            app::{self, admit::Registered, connect::Full, failure::Observed, request},
            attempt, close, conn,
            session::{self, app::drain::Application, codec, policy, protocol, response, turns},
        },
        service::bindings,
    },
    net::link::{egress, pool::change::opening},
};
use driver::schedule::{self, app::Work, clock};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer::{self, hold, write::consume::scope},
};
use policy::admit;
use port::{CONNECTOR_CONNECTION_LIMIT, Port, replies, requests};

use crate::{
    port,
    port::Disposition,
    protocol::{Codec, frame::Head},
    schema,
};

mod drain;
mod failure;
mod payloads;
mod pending;
mod ready;
mod state;
mod turn;

pub use pending::Pending;
pub(super) use state::State;

#[doc(hidden)]
#[repr(transparent)]
pub struct Session(pub(crate) Codec);

type Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, const ID: u8> = endpoint::State<
    'life,
    'd,
    Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    Session,
    KeyTag<ID>,
    CONNECTOR_CONNECTION_LIMIT,
>;

type Context<'turn, 'e, 'life, 'd, I, const ID: u8> =
    turns::Context<'turn, 'e, 'd, State<'life, 'd, I, ID>, requests::Frame<'d>, KeyTag<ID>>;

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    protocol::Spec<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    type Codec = crate::protocol::Codec;
    type Connect = Full;
    type ConnState = State<'life, 'd, I, ID>;
    type Failure = Observed;
    type Mode = mode::Plain;
    type Requests = request::Enabled;
    type Response = response::Resume<Pending<'d>, Pending<'d>>;
    type Send = requests::Frame<'d>;
    fn codec<'a>(
        owner: &'a Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    ) -> &'a crate::protocol::Codec {
        &owner.logic().0
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    protocol::Plain<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn reject<'e>(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        cause: scope::Error,
        ctx: &mut Context<'_, 'e, 'life, 'd, I, ID>,
    ) -> response::Delivery<Pending<'d>> {
        failure::Failure::in_context(owner.root(), ctx).apply(crate::Error::Input(cause));
        response::Delivery::Committed
    }

    fn deliver<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        head: Head<'input, 'd, R>,
        ctx: &mut Context<'_, '_, 'life, 'd, I, ID>,
    ) -> response::Delivery<Pending<'d>>
    where
        'd: 'input,
    {
        match head {
            Head::Message(message) => turn::Turn::new(owner.root(), ctx).response(message),
            Head::Fatal(error) => {
                failure::Failure::in_context(owner.root(), ctx).apply(error);
                response::Delivery::Committed
            }
        }
    }
}
impl<
    'life,
    'g: 'life,
    'r: 'life,
    's: 'life,
    'b: 'life,
    'c: 'life,
    'd: 'life,
    I: schema::QuerySet + 'life,
    const ID: u8,
>
    policy::Output<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn flush() -> Option<
        impl for<'a, 'x, 'y, 'z> Call<(
            &'a mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
            &'x mut Context<'y, 'z, 'life, 'd, I, ID>,
            session::Input,
        )>,
    > {
        Some(
            |(owner, ctx, input): (
                &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
                &mut Context<'_, '_, 'life, 'd, I, ID>,
                session::Input,
            )| {
                if let Err(error) = turn::Turn::new(owner.root(), ctx).try_flush(input) {
                    failure::Failure::in_context(owner.root(), ctx).apply(error);
                }
            },
        )
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Requests<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn drain(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        reqs: &mut request::Requests<'_, '_, '_, 'd, Self::Send, KeyTag<ID>>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        owner.root().request().drain(reqs);
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    admit::Register<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn register(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        entry: &conn::Activation<'_, 'd, KeyTag<ID>>,
    ) -> app::Admission {
        match owner.root().connection().activate(entry) {
            Ok(()) => app::Admission::Attached,
            Err(reason) => app::Admission::Rejected(reason),
        }
    }
}

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Connected<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    type Activation = Registered;

    fn connect(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        _: attempt::Completion<'_, 'd, KeyTag<ID>>,
        _: socket::Addr,
        ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) {
        turn::Turn::from_startup(owner.root(), ctx).connect();
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Failed<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn failed(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        key: attempt::Completion<'_, 'd, KeyTag<ID>>,
        cause: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let shared = &owner.root().shared;
        shared.signals.failed(key.id(), cause);
        shared.waiters().wake_all();
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Exhausted<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn exhaust(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        key: attempt::Completion<'_, 'd, KeyTag<ID>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let shared = &owner.root().shared;
        shared.signals.exhaust(key.id(), cause);
        shared.waiters().wake_all();
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Lifecycle<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    type Drain = Application;

    fn transmit_failed(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        failure: conn::Transmit,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Option<conn::Transmit> {
        let cause = replies::Failure::from_error(crate::Error::Transmit(failure));
        let mode = match cause.close_reason() {
            close::Reason::Capacity(close::Capacity::IdentityExhausted) => Disposition::Permanent,
            _ => Disposition::Reconnect,
        };
        let shared = &owner.root().shared;
        if let Some(cause) = shared.try_close(conn, cause, mode) {
            if matches!(mode, Disposition::Permanent) {
                shared.fail(cause.into_error());
            }
            owner.root().connection().wake(conn);
        }
        None
    }

    fn io_fail(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        error: Errno,
        ctx: &mut Context<'_, '_, 'life, 'd, I, ID>,
    ) {
        owner.root().shared.wake_close(
            ctx.conn_id,
            replies::Failure::from_error(crate::Error::Receive(error)),
            Disposition::Reconnect,
        );
    }
    fn disconnect(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        ctx: turns::Retired<
            '_,
            'd,
            Self::ConnState,
            KeyTag<ID>,
            Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        >,
    ) {
        let (link, state, region) = ctx.into_parts();
        if let Some(wake) = state.cancel(region) {
            owner.root().connection().wake(wake);
        }
        owner.root().connection().deactivate(link);
    }
    fn quarantine(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        owner.root().connection().fail(conn, reason);
    }
    fn clear_send<'turn>(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = ctx.parts();
        owner.root().request().retire(conn, work, region)
    }
    fn clear_recv<'turn>(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        reason: close::Reason,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = ctx.parts();
        let Some(cause) = owner.root().connection().try_fail(conn, reason) else {
            return egress::Cleared::Done;
        };
        let Some(reps) = owner.root().responses(conn) else {
            return egress::Cleared::Done;
        };
        match reps.fail(work, region, cause) {
            reply::Progress::Complete(_) => egress::Cleared::Done,
            reply::Progress::Pending(_) => egress::Cleared::Retry,
        }
    }
    fn fence(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        _: &State<'life, 'd, I, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        if owner.root().load(conn, region) == Some(0) {
            app::Fence::Ready
        } else {
            app::Fence::Pending
        }
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Quiescence<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn quiesce<'a>(
        _: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        barrier: app::Barrier<'a, 'd, KeyTag<ID>>,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'a, 'd, KeyTag<ID>> {
        barrier.ready()
    }
}
impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    policy::Scheduling<
        'life,
        'd,
        Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Session
{
    fn park<'turn>(
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        work: Work<'turn, 'd>,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        owner.root().maintain(work, &mut region.0);
        owner.root().shared.waiters().flush(work);
    }
    fn shutdown(owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>) {
        owner.root().shared.waiters().wake_all();
    }
    fn progress(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        _: &region::Token<region::Identity<'d>>,
        _clock: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        match owner.root().maint() {
            reply::Work::Idle if !owner.root().shared.waiters().is_pending() => {
                schedule::Progress::Quiescent
            }
            reply::Work::Pending | reply::Work::Idle => schedule::Progress::Runnable,
        }
    }
    fn inbound(
        owner: &Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        state: &State<'life, 'd, I, ID>,
        idle: Positive,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        let port = owner.root();
        let load = port.load(conn, region);
        if load == Some(0)
            && port
                .shared
                .ready
                .connection(conn)
                .is_some_and(|slot| slot.is_receiving())
        {
            return app::Inbound::Awaiting(idle);
        }
        state.inbound(load, idle)
    }
}
