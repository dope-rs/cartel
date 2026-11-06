use crate::{api, port::replies, schema};
use crate::{api::results::source::Payload as _, reply::Error};
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::{abi, context, wait};
use std::{ops, pin, task::Poll};

use crate::api::{
    task,
    traits::{copy, sealed::Sealed},
};
use o3::base::cell::region;
use task::lease::{Lease, Recover};
use task::stream;

enum Finishing {
    Opening,
    Response,
}

pub struct Sink<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    pub(in crate::api) dispatched: task::Core<
        'app,
        'g,
        'r,
        's,
        'b,
        'c,
        'd,
        B,
        I,
        api::Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
        ID,
        (),
        conn::Id<'d, KeyTag<ID>>,
        Recover,
    >,
    pub(in crate::api) budget: task::Budget<'d>,
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Sink<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    copy::Sink<'d, ID> for Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    fn write<'a>(
        &'a mut self,
        chunk: &'a [u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> + 'a {
        let timer = self.budget.guard();
        Write {
            guard: self,
            chunk,
            deadline: timer,
            waiter: wait::Waiter::new(),
        }
    }

    fn cancel(mut self) {
        self.dispatched.abort(crate::Error::Closed);
    }

    fn finish(self) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> {
        let timer = self.budget.guard();
        Finish {
            guard: self,
            state: Finishing::Opening,
            deadline: timer,
            waiter: wait::Waiter::new(),
        }
    }
}

#[pin_project::pin_project]
struct Write<'borrow, 'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    guard: &'borrow mut Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    chunk: &'borrow [u8],
    #[pin]
    deadline: Result<timer::Alarm<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'borrow, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Write<'_, 'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<(), Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let guard = &mut **this.guard;
        let mut timer = match this.deadline.as_mut().get_mut() {
            Ok(timer) => pin::Pin::new(timer),
            Err(error) => {
                this.waiter.as_ref().unregister();
                return Poll::Ready(Err(guard.dispatched.no_timer(*error)));
            }
        };
        let phase = guard.dispatched.phase();
        if let Poll::Ready(cause) = guard
            .budget
            .poll_expired(timer.as_mut(), cx.as_mut(), phase)
        {
            this.waiter.as_ref().unregister();
            let error = guard.dispatched.expire(cause);
            return Poll::Ready(Err(error));
        }
        let next = match guard
            .dispatched
            .poll(cx.as_mut(), this.waiter.as_ref(), Lease::admit)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => Err(error),
            Poll::Ready(Ok(task::Progress::Admitted(lease))) => {
                let client = lease.client();
                let target = lease.target();
                let region = cx.as_mut().region_token();
                send(client, target, this.chunk, region)
            }
            Poll::Ready(Ok(task::Progress::Complete {
                outcome: next,
                committed: commit,
            })) => {
                commit.quarantine();
                match next {
                    Ok(()) => Err(crate::Error::Closed.into()),
                    Err(error) => Err(error),
                }
            }
        };
        timer.as_ref().clear();
        Poll::Ready(next)
    }
}

#[pin_project::pin_project]
struct Finish<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    guard: Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    state: Finishing,
    #[pin]
    deadline: Result<timer::Alarm<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Finish<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<(), Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let mut timer = match this.deadline.as_mut().get_mut() {
            Ok(timer) => pin::Pin::new(timer),
            Err(error) => {
                this.waiter.as_ref().unregister();
                return Poll::Ready(Err(this.guard.dispatched.no_timer(*error)));
            }
        };
        let phase = this.guard.dispatched.phase();
        if let Poll::Ready(cause) =
            this.guard
                .budget
                .poll_expired(timer.as_mut(), cx.as_mut(), phase)
        {
            this.waiter.as_ref().unregister();
            let error = this.guard.dispatched.expire(cause);
            return Poll::Ready(Err(error));
        }
        match this
            .guard
            .dispatched
            .poll(cx.as_mut(), this.waiter.as_ref(), Lease::admit)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                timer.as_ref().clear();
                Poll::Ready(Err(error))
            }
            Poll::Ready(Ok(task::Progress::Admitted(lease))) => {
                if matches!(*this.state, Finishing::Opening) {
                    let client = lease.client();
                    let target = lease.target();
                    let result = {
                        let region = cx.as_mut().region_token();
                        finish(client, target, region)
                    };
                    if let Err(error) = result {
                        timer.as_ref().clear();
                        return Poll::Ready(Err(error));
                    }
                    *this.state = Finishing::Response;
                }
                Poll::Pending
            }
            Poll::Ready(Ok(task::Progress::Complete {
                outcome: next,
                committed: commit,
            })) => {
                if matches!(*this.state, Finishing::Opening) {
                    commit.quarantine();
                } else {
                    commit.settle();
                }
                timer.as_ref().clear();
                Poll::Ready(next)
            }
        }
    }
}

pub struct Stream<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    state: stream::Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractOne<ID>, ID>,
}

/// One COPY OUT chunk retaining its exact driver resource lease.
pub struct Chunk<'b, 'c, 'd>(replies::Payload<'b, 'c, 'd>);

impl<'b, 'c, 'd> Chunk<'b, 'c, 'd> {
    fn optional<E>(
        data: Result<Option<replies::Payload<'b, 'c, 'd>>, E>,
    ) -> Result<Option<Self>, E> {
        Ok(data?.map(Self))
    }
}

impl AsRef<[u8]> for Chunk<'_, '_, '_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl ops::Deref for Chunk<'_, '_, '_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn new(
        state: stream::Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractOne<ID>, ID>,
    ) -> Self {
        Self { state }
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Stream<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    copy::Stream<'d, ID> for Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Chunk = Chunk<'b, 'c, 'd>;

    fn next_chunk(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Result<Option<Self::Chunk>, Error<'d, ID>>> + '_ {
        self.state.next(Chunk::optional)
    }
}

fn send<'d, B: ?Sized, I: schema::QuerySet, const ID: u8>(
    client: api::Client<'_, '_, '_, '_, '_, '_, 'd, B, I, ID>,
    target: conn::Id<'d, KeyTag<ID>>,
    data: &[u8],
    region: &mut region::Token<region::Identity<'d>>,
) -> Result<(), Error<'d, ID>> {
    api::check_open(client)?;
    let requests = client.port.request();
    let frame = requests
        .encode(|frame| frame.copy_data(data))
        .map_err(Error::from)?;
    requests
        .try_enqueue(target, frame, region)
        .map_err(Error::from)
}

fn finish<'d, B: ?Sized, I: schema::QuerySet, const ID: u8>(
    client: api::Client<'_, '_, '_, '_, '_, '_, 'd, B, I, ID>,
    target: conn::Id<'d, KeyTag<ID>>,
    region: &mut region::Token<region::Identity<'d>>,
) -> Result<(), Error<'d, ID>> {
    api::check_open(client)?;
    client
        .port
        .request()
        .try_sync(target, region)
        .map_err(Error::from)
}
