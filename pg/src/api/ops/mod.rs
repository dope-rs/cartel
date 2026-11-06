use crate::{api, port::replies, reply, reply::Error, schema};
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use dope_fiber::{abi, wait};
use schema::result;
use std::marker;

use api::tx::control;
use api::{request, results, task, traits};
use reply::group;
use request::stateful;
use results::Storage;
use results::copy::{Chunk, Sink, Stream};
use task::lease;
use traits::{copy, sealed::Sealed};

mod status;

pub use status::Status;

/// A route- and driver-lifetime-branded PostgreSQL operation view.
pub struct Query<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    pub(super) client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    pub(super) target: Option<api::Target<'d, ID>>,
}

pub struct Unchecked<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>(
    pub(super) Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
)
where
    I: schema::QuerySet + 'd;

pub struct Transfer<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>(
    pub(super) Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
)
where
    I: schema::QuerySet + 'd;

pub struct Notify<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>(
    pub(super) Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
)
where
    I: schema::QuerySet + 'd;

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Copy
    for Query<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Query<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Copy
    for Unchecked<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Unchecked<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Copy
    for Transfer<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Transfer<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Copy
    for Notify<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Notify<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
    Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
where
    I: schema::QuerySet + 'd,
{
    pub(crate) const fn from_parts(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
    ) -> Self {
        Self { client, target }
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Query<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Query<'d, ID> for Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Results = api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;
    type Stream<Q>
        = results::RunStream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>;

    fn run_one<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<results::Row<<Self::Results as Storage>::Payload, Q>, Error<'d, ID>>,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        task::Map::new(
            self.client
                .typed::<Q, api::ExtractFirst<ID>>(self.target, params, ()),
            results::Row::<_, Q>::required,
        )
    }

    fn run_group<Q, const N: usize>(
        self,
        params: [Q::Params<'_>; N],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            [results::Row<<Self::Results as Storage>::Payload, Q>; N],
            results::GroupFailure<
                results::Row<<Self::Results as Storage>::Payload, Q>,
                Error<'d, ID>,
                <Self::Results as Storage>::Diagnostics,
                N,
            >,
        >,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, N>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        task::Map::new(
            self.client.group::<Q, N>(self.target, params),
            results::GroupFailure::required::<Q>,
        )
    }

    fn run_first<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            Option<results::Row<<Self::Results as Storage>::Payload, Q>>,
            Error<'d, ID>,
        >,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        task::Map::new(
            self.client
                .typed::<Q, api::ExtractFirst<ID>>(self.target, params, ()),
            results::Row::<_, Q>::optional,
        )
    }

    fn run_scalar<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            Option<results::Row<<Self::Results as Storage>::Payload, Q, true>>,
            Error<'d, ID>,
        >,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows<true>>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        task::Map::new(
            self.client
                .typed::<Q, api::ExtractFirst<ID>>(self.target, params, ()),
            results::Row::<_, Q, true>::optional,
        )
    }

    fn run_all<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<results::Collection<<Self::Results as Storage>::Batch, Q>, Error<'d, ID>>,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        task::Map::new(
            self.client.typed::<Q, api::ExtractAll<ID>>(
                self.target,
                params,
                self.client.port.batches,
            ),
            results::Collection::<_, Q>::from_result,
        )
    }

    fn run_no_rows<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
    where
        Q: schema::TypedQuery<Row = (), Result = result::Command>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        self.client
            .typed::<Q, api::ExtractUnit<ID>>(self.target, params, ())
    }

    fn run_stream<Q>(self, params: Q::Params<'_>) -> Self::Stream<Q>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        results::RunStream {
            state: self
                .client
                .stream::<Q, api::ExtractOne<ID>>(self.target, params),
            marker: marker::PhantomData,
        }
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Unchecked<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Unchecked<'d, ID> for Unchecked<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    fn command(
        self,
        sql: &str,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        self.0
            .client
            .raw::<api::ExtractUnit<ID>>(self.0.target, request::Request::raw(sql))
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Transfer<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    copy::Transfer<'d, ID> for Transfer<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Chunk = Chunk<'b, 'c, 'd>;
    type Sink = Sink<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;
    type Stream = Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;

    fn copy_in(
        self,
        sql: &str,
        data: &[u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        self.0.client.raw::<api::ExtractUnit<ID>>(
            self.0.target,
            request::Request::raw_extra(sql, request::Extra::CopyIn { data }),
        )
    }

    fn copy_stream(self, sql: &str) -> Self::Sink {
        Sink {
            dispatched: self
                .0
                .client
                .raw_core_as::<api::ExtractUnit<ID>, conn::Id<'d, KeyTag<ID>>, lease::Recover>(
                    self.0.target,
                    request::Request::raw_extra(sql, request::Extra::CopyInOpen),
                ),
            budget: task::Budget::new(self.0.client.port.lifecycle.deadline),
        }
    }

    fn copy_out(self, sql: &str) -> Self::Stream {
        Stream::new(
            self.0
                .client
                .stream_raw::<api::ExtractOne<ID>>(self.0.target, request::Request::raw(sql)),
        )
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Notify<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Notify<'d, ID> for Notify<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Diagnostics = group::Ledger<'r, 's>;
    type Listener = control::ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;

    fn dropped(self) -> u64 {
        self.0.client.port.shared.notifications.dropped()
    }

    fn next(self) -> impl abi::Fiber<'d, Output = Result<reply::Notice<'d>, reply::Error<'d, ID>>> {
        results::Next {
            client: self.0.client,
            waiter: wait::Waiter::new(),
        }
    }

    fn listen(
        self,
        channel: crate::Identifier,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Listener, reply::Error<'d, ID>>> {
        let client = self.0.client;
        let target = self.0.target;
        let opening = stateful::Stateful::new(client, target, request::Request::listen(&channel));
        dope_fiber::fiber!('d => async move {
            let lease = opening.await?;
            let target = match target {
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
