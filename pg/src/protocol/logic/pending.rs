use cartel_core::reply::{order, usage};
use dope::{
    core::driver::route::KeyTag,
    manifold::client::session::{response, response::work, turns},
};

use crate::{
    port::{self, CONNECTOR_CONNECTION_LIMIT, replies, requests},
    protocol::{
        Reason,
        logic::{Owner, Session, State, drain, failure, state},
    },
    schema,
};

/// Linear response continuation moved into the connector connection slot.
#[doc(hidden)]
pub struct Pending<'d>(Kind<'d>);

enum Kind<'d> {
    Row(replies::Pending<'d>),
    Ready(state::Drain),
}

impl<'d> Pending<'d> {
    pub(super) const fn row(pending: replies::Pending<'d>) -> Self {
        Self(Kind::Row(pending))
    }

    pub(super) const fn ready(pending: state::Drain) -> Self {
        Self(Kind::Ready(pending))
    }

    fn settle_row<'life, I: schema::QuerySet, const ID: u8>(
        port: &'life port::Port<'life, '_, '_, '_, '_, '_, 'd, I, ID>,
        pending: replies::Pending<'d>,
        ctx: &mut turns::Context<
            '_,
            '_,
            'd,
            State<'life, 'd, I, ID>,
            requests::Frame<'d>,
            KeyTag<ID>,
        >,
    ) -> response::Settlement<Self> {
        let Some(queue) = port.responses(ctx.conn_id) else {
            failure::Failure::in_context(port, ctx)
                .apply(crate::Error::Protocol(Reason::RowUnknown));
            return response::Settlement::Ready;
        };
        match pending {
            replies::Pending::Row(row) => match queue.try_push(ctx.region(), row) {
                Ok(()) => response::Settlement::Ready,
                Err(usage::InsertError::Backpressure { value, .. }) => {
                    response::Settlement::Blocked(Self::row(replies::Pending::Row(value)))
                }
                Err(usage::InsertError::Capacity { cause, .. }) => {
                    failure::Failure::in_context(port, ctx)
                        .apply(crate::Error::ReplyCapacity(cause));
                    response::Settlement::Ready
                }
                Err(usage::InsertError::Front {
                    front: order::Front::Detached,
                    ..
                }) => response::Settlement::Ready,
                Err(usage::InsertError::Front { front, .. }) => {
                    failure::Failure::in_context(port, ctx).apply(crate::Error::ReplyFront(front));
                    response::Settlement::Ready
                }
            },
            replies::Pending::Failure(failed) => match queue.try_fail(ctx.region(), failed) {
                Ok(()) => response::Settlement::Ready,
                Err(usage::InsertError::Backpressure { value, .. }) => {
                    response::Settlement::Blocked(Self::row(replies::Pending::Failure(value)))
                }
                Err(usage::InsertError::Capacity { cause, .. }) => {
                    failure::Failure::in_context(port, ctx)
                        .apply(crate::Error::ReplyCapacity(cause));
                    response::Settlement::Ready
                }
                Err(usage::InsertError::Front {
                    front: order::Front::Detached,
                    ..
                }) => {
                    queue.complete(ctx.region());
                    response::Settlement::Ready
                }
                Err(usage::InsertError::Front { front, .. }) => {
                    failure::Failure::in_context(port, ctx).apply(crate::Error::ReplyFront(front));
                    response::Settlement::Ready
                }
            },
        }
    }
}

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    response::Action<
        'life,
        'd,
        port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        Session,
        KeyTag<ID>,
        CONNECTOR_CONNECTION_LIMIT,
    > for Pending<'d>
{
    fn settle<'e, A: work::Admission>(
        item: response::Item<Self>,
        owner: &mut Owner<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        work: &mut A,
        ctx: &mut turns::Context<
            '_,
            'e,
            'd,
            State<'life, 'd, I, ID>,
            requests::Frame<'d>,
            KeyTag<ID>,
        >,
    ) -> response::Settlement<Self> {
        let port = owner.root();
        match item.into_inner().0 {
            Kind::Row(pending) => {
                match work.admit_with(pending, |pending| Self::settle_row(port, pending, ctx)) {
                    work::Capacity::Available(settled) => settled,
                    work::Capacity::Exhausted(pending) => {
                        response::Settlement::Yield(Self::row(pending))
                    }
                }
            }
            Kind::Ready(pending) => match drain::Drain::new(port, ctx, pending).settle(work) {
                drain::Progress::Complete => response::Settlement::Ready,
                drain::Progress::Blocked => response::Settlement::Yield(Self::ready(pending)),
            },
        }
    }
}
