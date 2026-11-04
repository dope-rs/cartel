use crate::{api, protocol, reply::Error, schema};
use api::traits;
use dope_fiber::{abi, context, wait};
use std::task;
use traits::sealed::Sealed;

pub struct Status<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>(
    pub(in crate::api) api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
)
where
    I: schema::QuerySet + 'd;

#[pin_project::pin_project]
struct Wait<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8> {
    client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Wait<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<(), crate::reply::Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.project();
        let waiter = this.waiter.as_ref();
        let shared = &this.client.port.shared;
        if let Some(error) = shared.signals.failure() {
            waiter.unregister();
            return task::Poll::Ready(Err(error));
        }
        if shared.ready.count() == this.client.port.capacity() {
            waiter.unregister();
            return task::Poll::Ready(Ok(()));
        }
        match shared.ready.try_register(waiter, context.as_ref()) {
            Ok(()) => task::Poll::Pending,
            Err(error) => {
                waiter.unregister();
                task::Poll::Ready(Err(crate::reply::Error::Wait(error)))
            }
        }
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Copy
    for Status<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Status<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Status<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Status<'d, ID> for Status<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    fn is_failed(self) -> bool {
        self.0.port.shared.signals.is_failed()
    }

    fn is_ready(self) -> bool {
        self.0.port.shared.ready.count() != 0
    }

    fn live_count(self) -> usize {
        self.0.port.shared.ready.count()
    }

    fn wait_ready(self) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> {
        Wait {
            client: self.0,
            waiter: wait::Waiter::new(),
        }
    }

    fn set_policy(self, policy: protocol::PickPolicy) {
        self.0.port.shared.ready.set_policy(policy);
    }
}
