use cartel_core::frame::Buffer;
use dope::{
    core::driver::route,
    manifold::client::{close, conn, session::turns},
};
use o3::buffer::write::consume::scope;

use crate::{
    client::{self, logic::Pending, ops::scripts::Invocation, settings::init::Plan},
    protocol,
};

pub(super) fn fail<'e, 's, 'b, 'c, 'd, const ID: u8>(
    port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
    cause: client::Failure<'d>,
    ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) {
    let policy = match ctx.state.phase() {
        protocol::Phase::Init(_) | protocol::Phase::Advance(_) | protocol::Phase::Unsolicited => {
            cause.start_policy()
        }
        protocol::Phase::Offline | protocol::Phase::Ready => close::Policy::Reconnect,
    };
    ctx.request(close::Directive::with_policy(cause.close_reason(), policy));
    port.fail(ctx.conn_id, cause, ctx.region());
}

pub(super) fn push<'e, 's, 'b, 'c, 'd, const ID: u8>(
    port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
    pending: Pending<'s, 'd>,
    ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) -> Option<Pending<'s, 'd>> {
    match port
        .schedule
        .try_reply(ctx.conn_id, pending.0, ctx.region(), &port.activity)
    {
        Ok(Some(frame)) => Some(Pending(frame, pending.1)),
        Ok(None) => None,
        Err(rejected) => {
            let (frame, cause) = rejected.into_parts();
            fail(port, cause, ctx);
            drop(frame);
            None
        }
    }
}

fn accepts_frame(status: &conn::CloseStatus) -> bool {
    matches!(
        status,
        conn::CloseStatus::Open
            | conn::CloseStatus::Closing(
                conn::ClosePhase::Withdrawing
                    | conn::ClosePhase::Requested
                    | conn::ClosePhase::Quarantining,
                _,
            )
    )
}

pub(super) fn reject<'d, const ID: u8>(
    port: &crate::Port<'_, '_, '_, '_, 'd, ID>,
    failure: protocol::Failure,
    ctx: &mut turns::Context<'_, '_, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) {
    let status = ctx.close_status();
    if accepts_frame(&status)
        || matches!(status, conn::CloseStatus::Closing(conn::ClosePhase::Retiring, rule)
            if matches!(rule.reason(), close::Reason::Remote))
    {
        fail(port, client::Failure::protocol(failure), ctx);
    }
}

pub(super) fn deliver<
    'e,
    's,
    'b,
    'c,
    'd,
    I: Invocation + Unpin,
    const ID: u8,
    const EXPECTED: i64,
    const COUNT: usize,
>(
    port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
    decoded: Result<protocol::Decoded<'s, 'd, 'd>, scope::Error>,
    ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) -> Option<Pending<'s, 'd>> {
    match decoded {
        Ok(protocol::Decoded::Rejected(failure)) => {
            reject(port, failure, ctx);
            None
        }
        _ if !accepts_frame(&ctx.close_status()) => None,
        Ok(protocol::Decoded::Frame(frame, lane)) => {
            let pending = Pending(frame, lane);
            match ctx.state.phase() {
                protocol::Phase::Init(step) => {
                    Plan::<I, EXPECTED, COUNT>::response(port, step, pending.0, ctx);
                    None
                }
                protocol::Phase::Offline
                | protocol::Phase::Advance(_)
                | protocol::Phase::Unsolicited => {
                    fail(port, client::Failure::UnexpectedResponse, ctx);
                    None
                }
                protocol::Phase::Ready => push(port, pending, ctx),
            }
        }
        Ok(protocol::Decoded::Retain(error)) => {
            let failure = match ctx.state.phase() {
                protocol::Phase::Offline => client::Failure::UnexpectedResponse,
                protocol::Phase::Init(_)
                | protocol::Phase::Advance(_)
                | protocol::Phase::Unsolicited
                | protocol::Phase::Ready => client::Failure::ResponseRetention(error),
            };
            fail(port, failure, ctx);
            None
        }
        Err(error) => {
            fail(port, client::Failure::Input(error), ctx);
            None
        }
    }
}
