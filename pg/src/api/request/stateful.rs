use crate::reply::Error;
use crate::{api, schema};
use api::{Response, request, task};
use cartel_core::deadline;
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::{abi, context, wait};
use std::{pin, task::Poll};
use task::lease::{Lease, Recover};

#[pin_project::pin_project]
pub(in crate::api) struct Stateful<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    command: task::Core<
        'app,
        'g,
        'r,
        's,
        'b,
        'c,
        'd,
        B,
        I,
        Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
        ID,
        (),
        conn::Id<'d, KeyTag<ID>>,
        Recover,
    >,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Stateful<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
        req: request::Request<'_>,
    ) -> Self {
        let core = client
            .raw_core_as::<api::ExtractUnit<ID>, conn::Id<'d, KeyTag<ID>>, Recover>(target, req);
        Self {
            command: core,
            deadline: client.port.lifecycle.deadline.guard(),
            waiter: wait::Waiter::new(),
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Stateful<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<
        Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, conn::Id<'d, KeyTag<ID>>, Recover, ID>,
        Error<'d, ID>,
    >;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut ctx) = call.into_parts();
        let mut this = this.project();
        let mut guard = match this.deadline.as_mut().get_mut() {
            Ok(guard) => pin::Pin::new(guard),
            Err(error) => {
                this.waiter.as_ref().unregister();
                return Poll::Ready(Err(this.command.no_timer(*error)));
            }
        };
        let phase = this.command.phase();
        if let Poll::Ready(time) = guard.as_mut().poll_expired(ctx.as_mut(), phase) {
            this.waiter.as_ref().unregister();
            return Poll::Ready(Err(this.command.expire(time)));
        }
        match this
            .command
            .poll_done(ctx.as_mut(), this.waiter.as_ref(), Lease::admit)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(task::Done::AdmissionFailed(error)) => {
                guard.as_mut().complete();
                Poll::Ready(Err(error))
            }
            Poll::Ready(task::Done::Response {
                outcome: Ok(()),
                committed: lease,
            }) => {
                guard.as_mut().complete();
                Poll::Ready(Ok(lease))
            }
            Poll::Ready(task::Done::Response {
                outcome: Err(error),
                committed: lease,
            }) => {
                lease.settle();
                guard.as_mut().complete();
                Poll::Ready(Err(error))
            }
        }
    }
}
