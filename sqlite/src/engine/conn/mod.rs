use crate::{api, catalogs};
use api::{exec, tx};
use dope_fiber::abi;
use dope_runtime::client;
use phase::{Borrow, Live, Phase};
use std::{marker, mem, ptr};

mod phase;
pub(crate) mod raw;

/// An exclusive database capability bound to its installed application scope.
///
#[repr(transparent)]
pub struct Connection<'app, 'local: 'app, 'd: 'local, I: catalogs::Catalog = catalogs::Empty> {
    app: client::Bound<'app, 'd, Installed<'local, 'd, I>, ptr::NonNull<Live<'local, 'd, I>>>,
}

impl<'app, 'local: 'app, 'd: 'local, I: catalogs::Catalog> Connection<'app, 'local, 'd, I> {
    const LAYOUT: () = {
        let word = mem::size_of::<usize>();
        assert!(mem::size_of::<Self>() == word);
        assert!(mem::align_of::<Self>() == mem::align_of::<usize>());
        assert!(mem::size_of::<Option<Self>>() == word);
    };

    fn try_new(
        context: client::Context<'_, 'app, 'd, Installed<'local, 'd, I>>,
    ) -> Result<Self, crate::Error> {
        let (app, bound) = context.into_parts();
        // SAFETY: the provider context retains this exact pinned application.
        let phase = unsafe { &*app.get_ref().phase.as_ptr() };
        let live = match phase {
            Phase::Live(live) => live,
            Phase::Closing(_) | Phase::Retiring(_) | Phase::Empty => {
                return Err(crate::Error::LocalConnectionClosed);
            }
        };
        match live.borrow.get() {
            Borrow::Vacant => {}
            Borrow::Borrowed => return Err(crate::Error::LocalConnectionBorrowed),
            Borrow::Closed | Borrow::Closing => return Err(crate::Error::LocalConnectionClosed),
        }
        live.borrow.set(Borrow::Borrowed);
        Ok(Self {
            app: bound.bind(ptr::NonNull::from(live)),
        })
    }

    fn state(&mut self) -> &mut api::State<'local, 'd, I> {
        // SAFETY: this guard exclusively owns the borrowed phase until drop.
        unsafe { &mut *self.app.as_ref().state.get() }
    }

    pub fn query(&mut self) -> exec::Query<'_, 'local, 'd, I> {
        self.state().query()
    }

    pub fn transaction(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Result<tx::Transaction<'_, 'local, 'd, I>, crate::Error>>
    + use<'_, 'app, 'local, 'd, I> {
        self.state().transaction()
    }
}

impl<I: catalogs::Catalog> Drop for Connection<'_, '_, '_, I> {
    fn drop(&mut self) {
        // SAFETY: Installed stays pinned for the application lease.
        let live = unsafe { self.app.as_ref() };
        live.borrow.set(live.borrow.get().returned());
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
        Connection::try_new(context)
    }
}

const _: () = Connection::<catalogs::Empty>::LAYOUT;

const _: () = {
    assert!(mem::size_of::<Installed<'_, '_, catalogs::Empty>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Installed<'_, '_, catalogs::Empty>>() == mem::align_of::<usize>());
};
