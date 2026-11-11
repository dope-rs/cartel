use crate::{api, port, port::replies, reply::Error, schema};
use api::{ops, request, task, traits, tx};
use cartel_core::reply::recv;
use dope::runtime::client;
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use dope_fiber::abi;
use std::marker::PhantomData;
use task::lease::{self, Policy, Target};

pub struct Context<
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
    for Context<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> Clone
    for Context<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(super) fn abort<T: Target<'d, ID>>(self, target: T, error: crate::Error) {
        if let Some(conn) = target.current(&self.port.shared.transaction()) {
            self.port
                .connection()
                .reconnect(conn, replies::Failure::from_error(error));
        }
    }

    pub(crate) fn reborrow<'short>(
        &'short self,
    ) -> Context<'short, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        'app: 'short,
    {
        Context {
            port: self.port,
            bound: self.bound.borrow(),
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Client<'d, ID> for ops::Query<Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>>
{
    type Transaction = tx::Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;

    fn transaction(self) -> impl traits::Begin<'d, ID, Guard = Self::Transaction> {
        tx::Builder {
            client: self.client,
            isolation: tx::Isolation::Default,
            access_mode: tx::AccessMode::Default,
            deferral: tx::Deferral::Default,
            timeout: None,
        }
    }

    fn status(self) -> impl traits::Status<'d, ID> {
        ops::Status(self.client)
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8, R, S>
    ops::Query<Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, R, S>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    pub(in crate::api) fn into_parts(
        self,
    ) -> (
        Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        R,
        impl FnOnce(&o3::buffer::pool::Pool) -> Option<o3::buffer::pool::Span>,
        impl FnOnce(cartel_core::deadline::Source<'d>) -> S::Clock,
    )
    where
        S: request::Input<'d>,
    {
        let (frame, clock) = self.input.into_parts();
        (self.client, self.target, frame, clock)
    }

    pub(in crate::api) fn typed<Q, X, T>(
        self,
        params: Q::Params<'_>,
        context: X::Context,
    ) -> impl abi::Fiber<'d, Output = Result<T, Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, X, ID, R, T, S>
    where
        S: request::Input<'d>,
        Q: schema::TypedQuery,
        X: recv::Extract<
                'app,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                Output = Result<T, Error<'d, ID>>,
            > + api::Sync,
        I: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup,
    {
        let (client, target, frame, clock) = self.into_parts();
        let next = api::Pending::typed::<Q, I, X, ID>(client.port, params, frame);
        task::Call::new(
            task::Core::<B, I, X, ID, R, PhantomData<X>>::new(client, target, context, next),
            clock(client.port.lifecycle.deadline).into(),
            |_, outcome| outcome,
        )
    }

    pub(in crate::api) fn raw(
        self,
        req: request::Request<'_>,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R, S>
    where
        S: request::Input<'d>,
    {
        let (core, clock) = self.raw_core::<api::ExtractUnit<ID>, lease::Detached>(req);
        task::Call::new(core, clock.into(), |_, outcome| outcome)
    }

    pub(in crate::api) fn stateful(
        self,
        req: request::Request<'_>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            lease::Lease<
                'app,
                'g,
                'r,
                's,
                'b,
                'c,
                'd,
                B,
                I,
                conn::Id<'d, KeyTag<ID>>,
                lease::Recover,
                ID,
            >,
            Error<'d, ID>,
        >,
    > + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R, S>
    where
        S: request::Input<'d>,
    {
        let (core, clock) = self.raw_core::<api::ExtractUnit<ID>, lease::Recover>(req);
        task::Call::new(core, clock.into(), |core, outcome| {
            let (outcome, lease) = core.complete(outcome)?;
            match outcome {
                Ok(()) => Ok(lease),
                Err(error) => {
                    lease.settle();
                    Err(error)
                }
            }
        })
    }

    pub(in crate::api) fn raw_core<X, P>(
        self,
        req: request::Request<'_>,
    ) -> (
        task::Core<
            'app,
            'g,
            'r,
            's,
            'b,
            'c,
            'd,
            B,
            I,
            X,
            ID,
            R,
            port::requests::Boundary,
            (),
            conn::Id<'d, KeyTag<ID>>,
            P,
        >,
        S::Clock,
    )
    where
        S: request::Input<'d>,
        X: recv::Extract<
                'app,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                Context = (),
            >,
        P: Policy,
    {
        let (client, target, frame, clock) = self.into_parts();
        let next = api::Pending::raw(client.port, req, frame);
        (
            task::Core::new(client, target, (), next),
            clock(client.port.lifecycle.deadline),
        )
    }
}
