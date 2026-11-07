use crate::{api, catalogs};
use api::{exec, stmt};
use dope_fiber::context;
use o3::mem;
use std::{pin, result, task};
use stmt::target::Target;

mod active;
pub(super) mod call;
pub(super) use active::Active;

pub(super) type Lease<'query, 'connection, 'd, P, A> =
    mem::Lease<Active<'query, 'connection, 'd, P, A>, mem::lease::Open>;

pub(super) enum State<S, A> {
    Start(S),
    Active(A),
}

pub(crate) struct Machine<
    'query,
    'connection: 'query,
    'd: 'connection,
    I: catalogs::Catalog,
    P,
    T: Target<I>,
> {
    start: exec::Start<'query, 'connection, 'd, I>,
    params: P,
    target: T,
}

impl<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog, P: api::Bind, T: Target<I>>
    Machine<'query, 'connection, 'd, I, P, T>
{
    pub(super) fn new(
        start: exec::Start<'query, 'connection, 'd, I>,
        target: T,
        params: P,
    ) -> Self {
        Self {
            start,
            params,
            target,
        }
    }

    pub(super) fn poll(
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        self.start.poll(context)
    }

    pub(super) fn prepare(
        self,
        cleaned: result::Result<(), crate::Error>,
    ) -> result::Result<Active<'query, 'connection, 'd, P, T::Access>, crate::Error> {
        let Self {
            start,
            params,
            target,
        } = self;
        cleaned?;
        start.prepare(params, target)
    }
}

impl<I: catalogs::Catalog, P, T: Target<I>> Unpin for Machine<'_, '_, '_, I, P, T> {}
