use crate::{api, catalogs};
use api::{exec, stmt};
use dope_fiber::context;
use o3::mem;
use std::{pin, result};
use stmt::target::Target;

mod active;
pub(super) mod call;
mod ready;
pub(super) use active::Active;
pub(super) use ready::Ready;

pub(super) type Lease<'query, 'connection, 'd, P, A> =
    mem::Lease<Active<'query, 'connection, 'd, P, A>, mem::lease::Open>;

pub(super) enum Next<S, R> {
    Pending(S),
    Ready(result::Result<R, crate::Error>),
}

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
        self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Next<Self, Active<'query, 'connection, 'd, P, T::Access>> {
        let Self {
            start,
            params,
            target,
        } = self;
        match start.poll(context) {
            Next::Pending(start) => Next::Pending(Self {
                start,
                params,
                target,
            }),
            Next::Ready(Ok(ready)) => Next::Ready(ready.bind(target, params)),
            Next::Ready(Err(error)) => Next::Ready(Err(error)),
        }
    }
}

impl<I: catalogs::Catalog, P, T: Target<I>> Unpin for Machine<'_, '_, '_, I, P, T> {}
