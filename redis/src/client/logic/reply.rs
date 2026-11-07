use cartel_core::frame::Buffer;
use dope::{
    core::driver::route,
    manifold::client::{close, conn, session::turns},
};
use o3::buffer::write::consume::scope;

use crate::{
    client::{self, logic::Pending, settings::init::Plan},
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

pub(super) fn deliver<'e, 's, 'b, 'c, 'd, const ID: u8>(
    port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
    decoded: Result<protocol::Decoded<'s, 'd, 'd>, scope::Error>,
    ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) -> Option<Pending<'s, 'd>> {
    match ctx.close_status() {
        conn::CloseStatus::Open
        | conn::CloseStatus::Closing(
            conn::ClosePhase::Withdrawing
            | conn::ClosePhase::Requested
            | conn::ClosePhase::Quarantining,
            _,
        ) => {}
        conn::CloseStatus::Closing(conn::ClosePhase::Retiring, directive)
            if matches!(directive.reason(), close::Reason::Remote) =>
        {
            if let Ok(protocol::Decoded::Rejected(failure)) = decoded {
                fail(port, client::Failure::protocol(failure), ctx);
            }
            return None;
        }
        conn::CloseStatus::Closing(
            conn::ClosePhase::Retiring
            | conn::ClosePhase::Settled
            | conn::ClosePhase::Finalized
            | conn::ClosePhase::Claimed,
            _,
        ) => return None,
    }
    match decoded {
        Ok(protocol::Decoded::Frame(frame, lane)) => {
            let pending = Pending(frame, lane);
            match ctx.state.phase() {
                protocol::Phase::Init(step) => {
                    Plan::response(port, step, pending.0, ctx);
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
        Ok(protocol::Decoded::Rejected(failure)) => {
            fail(port, client::Failure::protocol(failure), ctx);
            None
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
