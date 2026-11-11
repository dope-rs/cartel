use crate::{api, port::replies, reply, reply::Error, schema};
use dope_fiber::{abi, wait};
use schema::result;
use std::marker;

use api::tx::control;
use api::{request, results, task, traits};
use cartel_core::reply::Batch;
use reply::group;
use results::copy::{Chunk, Sink, Stream};
use task::lease;
use traits::{copy, sealed::Sealed};

mod status;

pub use status::Status;

/// A route- and driver-lifetime-branded PostgreSQL operation view.
#[derive(Clone, Copy)]
pub struct Query<C, R = api::Pooled, S = ()> {
    pub(super) client: C,
    pub(super) target: R,
    pub(super) input: S,
}

impl<C, R> Query<C, R> {
    pub(crate) const fn from_parts(client: C, target: R) -> Self {
        Self {
            client,
            target,
            input: (),
        }
    }
}

impl<C, R, S> Sealed for Query<C, R, S> {}

impl<'d, C, R, S, const ID: u8> traits::IntoQuery<'d, ID> for Query<C, R, S> {
    type Query = Self;

    fn into_query(self) -> Self {
        self
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R, S>
    traits::Query<'d, ID> for Query<api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, R, S>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    S: request::Input<'d>,
{
    type Acquired =
        Query<api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, R, request::Acquired<'d>>;

    fn acquire(self) -> impl abi::Fiber<'d, Output = Result<Self::Acquired, Error<'d, ID>>>
    where
        Self: Copy,
    {
        use cartel_core::deadline;
        use dope_fiber::task::done::Wait;
        use std::{pin::Pin, task::Poll};

        let Self { client, target, .. } = self;
        let mut deadline = client.port.lifecycle.deadline.guard();
        Wait::new(move |mut cx, waiter| {
            let guard = match &mut deadline {
                Ok(guard) => guard,
                Err(error) => return Poll::Ready(Err(crate::Error::Deadline(*error).into())),
            };
            if Pin::new(&mut *guard).poll_expired(cx.as_mut()).is_ready() {
                return Poll::Ready(Err(crate::Error::RequestTimeout(deadline::Elapsed::new(
                    deadline::RequestPhase::Admission,
                ))
                .into()));
            }
            let cause = if let Err(error) = api::check_open(client) {
                error
            } else if let Some(frame) = client.port.requests.acquire() {
                return Poll::Ready(Ok(Query {
                    client,
                    target,
                    input: request::Acquired {
                        frame,
                        deadline: guard.take(),
                    },
                }));
            } else {
                match client
                    .port
                    .shared
                    .frame_waiters
                    .try_register(waiter, cx.as_ref())
                {
                    Ok(()) => return Poll::Pending,
                    Err(error) => Error::Wait(error),
                }
            };
            guard.complete();
            Poll::Ready(Err(cause))
        })
    }

    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Batch = Batch<'app, 'b, 'c, 'd, replies::Row<'d>>;
    type Diagnostics = group::Ledger<'r, 's>;
    type Stream<Q>
        = results::RunStream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R>
    where
        Q: schema::TypedQuery<Result = result::Rows>;

    fn run_one<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<'d, Output = Result<results::Row<Self::Payload, Q>, Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R, S>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        self.typed::<Q, api::ExtractFirst<_, ID>, _>(params, results::Row::<_, Q>::required)
    }

    fn run_group<Q, const N: usize>(
        self,
        params: [Q::Params<'_>; N],
    ) -> impl abi::Fiber<
        'd,
        Output = results::GroupResult<'d, results::Row<Self::Payload, Q>, Self::Diagnostics, N, ID>,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, N, R, S>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let (client, target, frame, clock) = self.into_parts();
        let next = api::Pending::group::<Q, I, ID, N>(client.port, params, frame);
        task::group::Group::new(client, target, next, clock(client.port.lifecycle.deadline))
    }

    fn run_first<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<'d, Output = Result<Option<results::Row<Self::Payload, Q>>, Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R, S>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        self.typed::<Q, api::ExtractFirst<_, ID>, _>(params, results::Row::<_, Q>::optional)
    }

    fn run_scalar<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Option<results::Row<Self::Payload, Q, true>>, Error<'d, ID>>,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R, S>
    where
        Q: schema::TypedQuery<Result = result::Rows<true>>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        self.typed::<Q, api::ExtractFirst<_, ID>, _>(params, results::Row::<_, Q, true>::optional)
    }

    fn run_all<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<'d, Output = Result<results::Collection<Self::Batch, Q>, Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R, S>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let batches = self.client.port.batches;
        abi::Map::new(
            self.typed::<Q, api::ExtractAll<ID>, _>(params, batches),
            results::Collection::<_, Q>::from_result,
        )
    }

    fn run_no_rows<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R, S>
    where
        Q: schema::TypedQuery<Row = (), Result = result::Command>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        self.typed::<Q, api::ExtractUnit<ID>, _>(params, ())
    }

    fn run_stream<Q>(self, params: Q::Params<'_>) -> <Self as traits::Query<'d, ID>>::Stream<Q>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let (client, target, frame, clock) = self.into_parts();
        let next = api::Pending::typed::<Q, I, api::ExtractOne<ID>, ID>(client.port, params, frame);
        results::RunStream {
            state: task::stream::Stream::new(
                task::Core::new(client, target, (), next),
                clock(client.port.lifecycle.deadline),
            ),
            marker: marker::PhantomData,
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R, S>
    traits::Unchecked<'d, ID> for Query<api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, R, S>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    S: request::Input<'d>,
{
    fn command(
        self,
        sql: &str,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R, S> {
        self.raw(request::Request::raw(sql))
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R, S>
    copy::Transfer<'d, ID> for Query<api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, R, S>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    S: request::Input<'d>,
{
    type Chunk = Chunk<'b, 'c, 'd>;
    type Sink = Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>;
    type Stream = Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>;

    fn copy_in(
        self,
        sql: &str,
        data: &[u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R, S> {
        self.raw(request::Request::raw_extra(
            sql,
            request::Extra::CopyIn { data },
        ))
    }

    fn copy_stream(self, sql: &str) -> Self::Sink {
        let (dispatched, budget) = self.raw_core::<api::ExtractUnit<ID>, lease::Recover>(
            request::Request::raw_extra(sql, request::Extra::CopyInOpen),
        );
        Sink {
            dispatched,
            budget: budget.into(),
        }
    }

    fn copy_out(self, sql: &str) -> <Self as copy::Transfer<'d, ID>>::Stream {
        let (core, clock) =
            self.raw_core::<api::ExtractOne<ID>, lease::Detached>(request::Request::raw(sql));
        Stream::new(task::stream::Stream::new(core, clock))
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R>
    traits::Notify<'d, ID> for Query<api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    type Listener = control::ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;

    fn dropped(self) -> u64 {
        self.client.port.shared.notifications.dropped()
    }

    fn next(self) -> impl abi::Fiber<'d, Output = Result<reply::Notice<'d>, reply::Error<'d, ID>>> {
        results::Next {
            client: self.client,
            waiter: wait::Waiter::new(),
        }
    }

    fn listen(
        self,
        channel: crate::Identifier,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Listener, reply::Error<'d, ID>>> {
        let target = self.target;
        let opening = self.stateful(request::Request::listen(&channel));
        dope_fiber::fiber!('d => async move {
            let lease = opening.await?;
            let target = match target.into() {
                Some(target) => target,
                None => api::Target::new(lease.target()),
            };
            Ok(control::ListenGuard::from_parts(
                lease.retarget(target)?,
                channel,
            ))
        })
    }
}
