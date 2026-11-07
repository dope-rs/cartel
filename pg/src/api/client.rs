use crate::reply::group::Ledger;
use crate::{api, port, port::replies, schema};
use api::results::copy;
use api::traits::{copy::Transfer, sealed::Sealed};
use api::{Response, ops, request, results, task, traits, tx};
use cartel_core::reply::Batch;
use cartel_core::reply::recv;
use dope::runtime::client;
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use task::{
    group,
    lease::{self, Policy, Target},
    stream::Stream,
};

pub struct Client<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I: schema::QuerySet,
    const ID: u8 = 0,
> {
    pub(crate) port: &'app port::Port<'app, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    pub(crate) bound: client::Bound<'app, 'd, B>,
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Copy
    for Client<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Client<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(super) fn abort<T: Target<'d, ID>>(self, target: T, error: crate::Error) {
        if let Some(conn) = target.current(&self.port.shared.transaction()) {
            self.port
                .connection()
                .reconnect(conn, replies::Failure::from_error(error));
        }
    }

    pub(crate) fn reborrow<'short>(&'short self) -> Client<'short, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        'app: 'short,
    {
        Client {
            port: self.port,
            bound: self.bound.borrow(),
        }
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Sealed
    for Client<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd, B: ?Sized, I: schema::QuerySet, const ID: u8> results::Storage
    for Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Batch = Batch<'app, 'b, 'c, 'd, replies::Row<'d>>;
    type Diagnostics = Ledger<'r, 's>;
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Client<'d, ID> for Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Query = ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;
    type CopyChunk = copy::Chunk<'b, 'c, 'd>;
    type Transaction = tx::Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;

    fn query(self) -> Self::Query {
        ops::Query {
            client: self,
            target: None,
        }
    }

    fn unchecked(self) -> impl traits::Unchecked<'d, ID> {
        ops::Unchecked(self.query())
    }

    fn copy(self) -> impl Transfer<'d, ID, Chunk = Self::CopyChunk> {
        ops::Transfer(self.query())
    }

    fn notify(
        self,
    ) -> impl traits::Notify<
        'd,
        ID,
        Schema = Self::Schema,
        Payload = Self::Payload,
        Diagnostics = Self::Diagnostics,
    > {
        ops::Notify(self.query())
    }

    fn transaction(self) -> impl traits::Begin<'d, ID, Guard = Self::Transaction> {
        tx::Builder {
            client: self,
            isolation: tx::Isolation::Default,
            access_mode: tx::AccessMode::Default,
            deferral: tx::Deferral::Default,
            timeout: None,
        }
    }

    fn status(self) -> impl traits::Status<'d, ID> {
        ops::Status(self)
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn typed<Q, X>(
        self,
        target: Option<api::Target<'d, ID>>,
        params: Q::Params<'_>,
        context: X::Context,
    ) -> task::Call<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
    where
        Q: schema::TypedQuery,
        X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>> + api::Sync,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let reply = recv::Response::<
            replies::Row<'_>,
            X,
            replies::ReplyFailure<'_>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >::with(context);
        let next = api::Pending::typed::<Q, I, ID>(self.port, params, X::BOUNDARY);
        task::Call::from_core(
            task::Core::new(self, target, reply, next),
            self.port.lifecycle.deadline,
        )
    }

    pub(in crate::api) fn group<Q, const N: usize>(
        self,
        target: Option<api::Target<'d, ID>>,
        params: [Q::Params<'_>; N],
    ) -> group::Group<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, N>
    where
        Q: schema::TypedQuery,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let next = api::Pending::group::<Q, I, ID, N>(self.port, params);
        group::Group::new(self, target, next)
    }

    pub(in crate::api) fn raw<X>(
        self,
        target: Option<api::Target<'d, ID>>,
        req: request::Request<'_>,
    ) -> task::Call<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
    where
        X: recv::Extract<
                'app,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                Context = (),
            >,
    {
        task::Call::from_core(
            self.raw_core_as::<X, conn::Id<'d, KeyTag<ID>>, lease::Detached>(target, req),
            self.port.lifecycle.deadline,
        )
    }

    pub(in crate::api) fn raw_core_as<X, A, P>(
        self,
        target: Option<api::Target<'d, ID>>,
        req: request::Request<'_>,
    ) -> task::Core<
        'app,
        'g,
        'r,
        's,
        'b,
        'c,
        'd,
        B,
        I,
        Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>,
        ID,
        (),
        A,
        P,
    >
    where
        X: recv::Extract<
                'app,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                Context = (),
            >,
        A: Target<'d, ID>,
        P: Policy,
    {
        let reply = recv::Response::<
            replies::Row<'_>,
            X,
            replies::ReplyFailure<'_>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >::new();
        let next = api::Pending::raw(self.port, req);
        task::Core::new(self, target, reply, next)
    }

    pub(in crate::api) fn stream<Q, X>(
        self,
        target: Option<api::Target<'d, ID>>,
        params: Q::Params<'_>,
    ) -> Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
    where
        Q: schema::TypedQuery,
        X: recv::Extract<
                'app,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                Context = (),
            > + api::Sync,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let reply = recv::Stream::<
            replies::Row<'_>,
            X,
            replies::ReplyFailure<'_>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >::new();
        let next = api::Pending::typed::<Q, I, ID>(self.port, params, X::BOUNDARY);
        Stream::new(self, target, reply, next)
    }

    pub(in crate::api) fn stream_raw<X>(
        self,
        target: Option<api::Target<'d, ID>>,
        req: request::Request<'_>,
    ) -> Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
    where
        X: recv::Extract<
                'app,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                Context = (),
            >,
    {
        let reply = recv::Stream::<
            replies::Row<'_>,
            X,
            replies::ReplyFailure<'_>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >::new();
        let next = api::Pending::raw(self.port, req);
        Stream::new(self, target, reply, next)
    }
}
