use crate::{api, catalogs};
use api::{exec, tx};
use dope_fiber::abi;
use dope_runtime::client;
use phase::Phase;
use std::{marker, mem, ptr};

mod lease;
use lease::Lease;
mod phase;
pub(crate) mod raw;

/// An exclusive database capability bound to its installed application scope.
///
#[repr(transparent)]
pub struct Connection<'app, 'local: 'app, 'd: 'local, I: catalogs::Catalog = catalogs::Empty> {
    guard: Lease<'app, 'local, 'd, I>,
}

impl<'app, 'local: 'app, 'd: 'local, I: catalogs::Catalog> Connection<'app, 'local, 'd, I> {
    const LAYOUT: () = {
        let word = mem::size_of::<usize>();
        assert!(mem::size_of::<Self>() == word);
        assert!(mem::align_of::<Self>() == mem::align_of::<usize>());
        assert!(mem::size_of::<Option<Self>>() == word);
    };

    const fn new(guard: Lease<'app, 'local, 'd, I>) -> Self {
        Self { guard }
    }

    pub fn query(&mut self) -> exec::Query<'_, 'local, 'd, I> {
        self.guard.state().query()
    }

    pub fn transaction(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Result<tx::Transaction<'_, 'local, 'd, I>, crate::Error>>
    + use<'_, 'app, 'local, 'd, I> {
        self.guard.state().transaction()
    }
}

impl<'query, 'app: 'query, 'local: 'app, 'd: 'local, I: catalogs::Catalog>
    api::IntoQuery<'query, 'local, 'd, I> for &'query mut Connection<'app, 'local, 'd, I>
{
    fn into_query(self) -> exec::Query<'query, 'local, 'd, I> {
        self.query()
    }
}

/// Installed database application owned by one executor generation.
pub struct Installed<'local, 'd: 'local, I: catalogs::Catalog> {
    phase: ptr::NonNull<Phase<'local, 'd, I>>,
    ownership: marker::PhantomData<(Phase<'local, 'd, I>, marker::PhantomPinned)>,
}

impl<'local, 'd: 'local, I: catalogs::Catalog> client::Provider<'d> for Installed<'local, 'd, I> {
    type Client<'app>
        = Result<Connection<'app, 'local, 'd, I>, crate::Error>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        Lease::try_new(context).map(Connection::new)
    }
}

const _: () = Connection::<catalogs::Empty>::LAYOUT;

const _: () = {
    assert!(mem::size_of::<Installed<'_, '_, catalogs::Empty>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Installed<'_, '_, catalogs::Empty>>() == mem::align_of::<usize>());
};
