use crate::{api, port::replies, schema};
use crate::{api::results::source::Payload as _, reply::Error};
use cartel_core::deadline;
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

pub struct Sink<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0, R = api::Pooled>
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
        api::ExtractUnit<ID>,
        ID,
        R,
        crate::port::requests::Boundary,
        (),
        conn::Id<'d, KeyTag<ID>>,
        Recover,
    >,
    pub(in crate::api) budget: task::Budget<'d>,
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8, R> Sealed
    for Sink<'_, '_, '_, '_, '_, '_, '_, B, I, ID, R>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R>
    copy::Sink<'d, ID> for Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    fn write<'a>(
        &'a mut self,
        chunk: &'a [u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> + 'a {
        let timer = self.budget.guard().map(Some);
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
        let timer = self.budget.guard().map(Some);
        Finish {
            guard: self,
            state: Finishing::Opening,
            deadline: timer,
            waiter: wait::Waiter::new(),
        }
    }
}

#[pin_project::pin_project]
struct Write<'borrow, 'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8, R>
where
    I: schema::QuerySet + 'd,
{
    guard: &'borrow mut Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>,
    chunk: &'borrow [u8],
    #[pin]
    deadline: Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'borrow, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R>
    abi::Fiber<'d> for Write<'_, 'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    type Output = Result<(), Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let guard = &mut **this.guard;
        let mut timer = match this.deadline.as_mut().get_mut() {
            Ok(Some(timer)) => pin::Pin::new(timer),
            Ok(None) => unreachable!("complete"),
            Err(error) => {
                this.waiter.as_ref().unregister();
                return Poll::Ready(Err(guard.dispatched.abort(crate::Error::Deadline(*error))));
            }
        };
        if guard
            .budget
            .poll_expired(timer.as_mut(), cx.as_mut())
            .is_ready()
        {
            let cause = deadline::Elapsed::new(guard.dispatched.phase());
            this.waiter.as_ref().unregister();
            let error = guard.dispatched.abort(crate::Error::RequestTimeout(cause));
            this.deadline.set(Ok(None));
            return Poll::Ready(Err(error));
        }
        let next = match guard.dispatched.poll(
            cx.as_mut(),
            this.waiter.as_ref(),
            Lease::admit,
            task::Core::complete,
        ) {
            Poll::Pending => {
                let Some(lease) = guard.dispatched.admitted() else {
                    return Poll::Pending;
                };
                let client = lease.client();
                let target = lease.target();
                let region = cx.as_mut().region_token();
                send(client, target, this.chunk, region)
            }
            Poll::Ready(Err(error)) => Err(error),
            Poll::Ready(Ok((next, commit))) => {
                commit.quarantine();
                match next {
                    Ok(()) => Err(crate::Error::Closed.into()),
                    Err(error) => Err(error),
                }
            }
        };
        this.deadline.set(Ok(None));
        Poll::Ready(next)
    }
}

#[pin_project::pin_project]
struct Finish<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8, R>
where
    I: schema::QuerySet + 'd,
{
    guard: Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>,
    state: Finishing,
    #[pin]
    deadline: Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R>
    abi::Fiber<'d> for Finish<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    type Output = Result<(), Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let mut timer = match this.deadline.as_mut().get_mut() {
            Ok(Some(timer)) => pin::Pin::new(timer),
            Ok(None) => unreachable!("complete"),
            Err(error) => {
                this.waiter.as_ref().unregister();
                let error = this.guard.dispatched.abort(crate::Error::Deadline(*error));
                return Poll::Ready(Err(error));
            }
        };
        if this
            .guard
            .budget
            .poll_expired(timer.as_mut(), cx.as_mut())
            .is_ready()
        {
            let cause = deadline::Elapsed::new(this.guard.dispatched.phase());
            this.waiter.as_ref().unregister();
            let error = this
                .guard
                .dispatched
                .abort(crate::Error::RequestTimeout(cause));
            this.deadline.set(Ok(None));
            return Poll::Ready(Err(error));
        }
        match this.guard.dispatched.poll(
            cx.as_mut(),
            this.waiter.as_ref(),
            Lease::admit,
            task::Core::complete,
        ) {
            Poll::Pending => {
                let Some(lease) = this.guard.dispatched.admitted() else {
                    return Poll::Pending;
                };
                if matches!(*this.state, Finishing::Opening) {
                    let client = lease.client();
                    let target = lease.target();
                    let result = {
                        let region = cx.as_mut().region_token();
                        finish(client, target, region)
                    };
                    if let Err(error) = result {
                        this.deadline.set(Ok(None));
                        return Poll::Ready(Err(error));
                    }
                    *this.state = Finishing::Response;
                }
                Poll::Pending
            }
            Poll::Ready(Err(error)) => {
                this.deadline.set(Ok(None));
                Poll::Ready(Err(error))
            }
            Poll::Ready(Ok((next, commit))) => {
                if matches!(*this.state, Finishing::Opening) {
                    commit.quarantine();
                } else {
                    commit.settle();
                }
                this.deadline.set(Ok(None));
                Poll::Ready(next)
            }
        }
    }
}

pub struct Stream<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I,
    const ID: u8 = 0,
    R = api::Pooled,
> where
    I: schema::QuerySet + 'd,
{
    state: stream::Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractOne<ID>, ID, R>,
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

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R>
    Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    pub(in crate::api) fn new(
        state: stream::Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractOne<ID>, ID, R>,
    ) -> Self {
        Self { state }
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8, R> Sealed
    for Stream<'_, '_, '_, '_, '_, '_, '_, B, I, ID, R>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R>
    copy::Stream<'d, ID> for Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
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
        .encode(client.port.requests.acquire(), |frame| {
            frame.copy_data(data)
        })
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
