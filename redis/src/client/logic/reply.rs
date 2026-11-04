use cartel_core::{
    frame::Buffer,
    reply::{order, usage},
};
use dope::{
    core::driver::route,
    manifold::client::{close, conn, session::turns},
};
use o3::buffer::write::consume::scope;

use crate::{
    client,
    client::logic::{Pending, init},
    protocol,
};

pub(super) fn fail<'e, 's, 'b, 'c, 'd, const ID: u8>(
    port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
    cause: client::Failure<'d>,
    ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) {
    let policy = match ctx.state.phase() {
        protocol::Phase::Init(_) => cause.start_policy(),
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
    let Some(reps) = port.schedule.responses(ctx.conn_id) else {
        fail(port, client::Failure::UnexpectedResponse, ctx);
        return None;
    };
    match reps.select(ctx.region()).try_push(pending.0) {
        Ok(()) => {}
        Err(usage::InsertError::Front {
            value,
            front: order::Front::Detached,
        }) => drop(value),
        Err(usage::InsertError::Backpressure { value, .. }) => {
            return Some(Pending::new(value, pending.1));
        }
        Err(usage::InsertError::Capacity { cause, .. }) => {
            fail(port, client::Failure::ResponseCapacity(cause), ctx);
            return None;
        }
        Err(usage::InsertError::Front { front, .. }) => {
            fail(port, client::Failure::ResponseFront(front), ctx);
            return None;
        }
    }
    reps.complete(ctx.region());
    port.activity.wake_send();
    port.schedule
        .complete(ctx.conn_id, ctx.region(), &port.activity);
    None
}

pub(super) fn deliver<'e, 's, 'b, 'c, 'd, const ID: u8>(
    init: &init::Init,
    port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
    decoded: Result<protocol::Decoded<'s, 'd, 'd>, scope::Error>,
    ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
) -> Option<Pending<'s, 'd>> {
    match ctx.close_status() {
        conn::CloseStatus::Open
        | conn::CloseStatus::Closing(
            conn::ClosePhase::Requested | conn::ClosePhase::Quarantining,
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
            let pending = Pending::new(frame, lane);
            match ctx.state.phase() {
                protocol::Phase::Init(step) => {
                    init.response(port, step, pending.0, ctx);
                    None
                }
                protocol::Phase::Offline => {
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
                protocol::Phase::Init(_) | protocol::Phase::Ready => {
                    client::Failure::ResponseRetention(error)
                }
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
