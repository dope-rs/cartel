use crate::port::Instance;
use crate::{api, port::replies};
use crate::{api::results::source::Payload as _, reply::Error};
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::{abi, context, wait};
use std::{ops, task::Poll};

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
    I: Instance<Queries: 'd> + 'd,
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

impl<B: ?Sized, I: Instance, const ID: u8, R> Sealed
    for Sink<'_, '_, '_, '_, '_, '_, '_, B, I, ID, R>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8, R>
    copy::Sink<'d, ID> for Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    fn write<'a>(
        &'a mut self,
        chunk: &'a [u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> + 'a {
        let timer = self.dispatched.try_alarm::<false>(&self.budget);
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
        let timer = self.dispatched.try_alarm::<false>(&self.budget);
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
    I: Instance<Queries: 'd> + 'd,
{
    guard: &'borrow mut Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>,
    chunk: &'borrow [u8],
    deadline: Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'borrow, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8, R>
    abi::Fiber<'d> for Write<'_, 'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    type Output = Result<(), Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.project();
        let guard = &mut **this.guard;
        guard.dispatched.check_deadline(
            &mut guard.budget,
            this.deadline,
            this.waiter.as_ref(),
            cx.as_mut(),
        )?;
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
        *this.deadline = Ok(None);
        Poll::Ready(next)
    }
}

#[pin_project::pin_project]
struct Finish<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8, R>
where
    I: Instance<Queries: 'd> + 'd,
{
    guard: Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>,
    state: Finishing,
    deadline: Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8, R>
    abi::Fiber<'d> for Finish<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    type Output = Result<(), Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.project();
        this.guard.dispatched.check_deadline(
            &mut this.guard.budget,
            this.deadline,
            this.waiter.as_ref(),
            cx.as_mut(),
        )?;
        let next = match this.guard.dispatched.poll(
            cx.as_mut(),
            this.waiter.as_ref(),
            Lease::admit,
            task::Core::complete,
        ) {
            Poll::Pending if matches!(*this.state, Finishing::Opening) => {
                let Some(lease) = this.guard.dispatched.admitted() else {
                    return Poll::Pending;
                };
                match finish(lease.client(), lease.target(), cx.as_mut().region_token()) {
                    Err(error) => Err(error),
                    Ok(()) => {
                        *this.state = Finishing::Response;
                        return Poll::Pending;
                    }
                }
            }
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => Err(error),
            Poll::Ready(Ok((next, commit))) => {
                if matches!(*this.state, Finishing::Opening) {
                    commit.quarantine();
                } else {
                    commit.settle();
                }
                next
            }
        };
        *this.deadline = Ok(None);
        Poll::Ready(next)
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
    I: Instance<Queries: 'd> + 'd,
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

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8, R>
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

impl<B: ?Sized, I: Instance, const ID: u8, R> Sealed
    for Stream<'_, '_, '_, '_, '_, '_, '_, B, I, ID, R>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8, R>
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

fn send<'d, B: ?Sized, I: Instance, const ID: u8>(
    client: api::Context<'_, '_, '_, '_, '_, '_, 'd, B, I, ID>,
    target: conn::Id<'d, KeyTag<ID>>,
    data: &[u8],
    region: &mut region::Token<region::Identity<'d>>,
) -> Result<(), Error<'d, ID>> {
    api::check_open(client)?;
    let requests = client.port.request();
    let frame = requests
        .encode(client.port.requests.acquire(), |frame| {
            crate::protocol::encode::Writer::with(frame, |out| out.copy_data(data))
        })
        .map_err(Error::from)?;
    requests
        .try_enqueue(target, frame, region)
        .map_err(Error::from)
}

fn finish<'d, B: ?Sized, I: Instance, const ID: u8>(
    client: api::Context<'_, '_, '_, '_, '_, '_, 'd, B, I, ID>,
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
