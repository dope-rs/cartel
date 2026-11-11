use cartel_core::reply::{order, usage};
use dope::{
    core::driver::route::KeyTag,
    manifold::client::session::{response, response::work, turns},
};

use crate::{
    port::{self, CONNECTOR_CONNECTION_LIMIT, replies, requests},
    protocol::{
        Reason,
        logic::{Owner, Session, State, drain, turn::Turn},
        runtime::tx::poll,
    },
    schema,
};

/// Linear response continuation moved into the connector connection slot.
#[doc(hidden)]
pub struct Pending<'d>(Kind<'d>);

enum Kind<'d> {
    Row(replies::Pending<'d>),
    Ready(poll::Status),
}

impl<'d> Pending<'d> {
    pub(super) const fn row(pending: replies::Pending<'d>) -> Self {
        Self(Kind::Row(pending))
    }

    pub(super) const fn ready(pending: poll::Status) -> Self {
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
        let Some(mut queue) = port.responses(ctx.conn_id, ctx.region()) else {
            Turn::new(port, ctx).fail(crate::Error::Protocol(Reason::RowUnknown));
            return response::Settlement::Ready;
        };
        match pending {
            replies::Pending::Row(row) => match queue.try_push(row) {
                Ok(()) => response::Settlement::Ready,
                Err(usage::InsertError::Backpressure { value, .. }) => {
                    response::Settlement::Blocked(Self::row(replies::Pending::Row(value)))
                }
                Err(usage::InsertError::Capacity { cause, .. }) => {
                    Turn::new(port, ctx).fail(crate::Error::ReplyCapacity(cause));
                    response::Settlement::Ready
                }
                Err(usage::InsertError::Front {
                    front: order::Front::Detached,
                    ..
                }) => response::Settlement::Ready,
                Err(usage::InsertError::Front { front, .. }) => {
                    Turn::new(port, ctx).fail(crate::Error::ReplyFront(front));
                    response::Settlement::Ready
                }
            },
            replies::Pending::Failure(failed) => match queue.try_fail(failed) {
                Ok(()) => response::Settlement::Ready,
                Err(usage::InsertError::Backpressure { value, .. }) => {
                    response::Settlement::Blocked(Self::row(replies::Pending::Failure(value)))
                }
                Err(usage::InsertError::Capacity { cause, .. }) => {
                    Turn::new(port, ctx).fail(crate::Error::ReplyCapacity(cause));
                    response::Settlement::Ready
                }
                Err(usage::InsertError::Front {
                    front: order::Front::Detached,
                    ..
                }) => {
                    queue.complete();
                    response::Settlement::Ready
                }
                Err(usage::InsertError::Front { front, .. }) => {
                    Turn::new(port, ctx).fail(crate::Error::ReplyFront(front));
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
            Kind::Ready(pending) => match Turn::new(port, ctx).try_settle(pending, work) {
                Ok(drain::Progress::Complete) => response::Settlement::Ready,
                Ok(drain::Progress::Blocked) => response::Settlement::Yield(Self::ready(pending)),
                Err(error) => {
                    Turn::new(port, ctx).fail(error);
                    response::Settlement::Ready
                }
            },
        }
    }
}
