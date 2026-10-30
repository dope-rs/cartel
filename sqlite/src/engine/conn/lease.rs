use crate::{
    api, catalogs,
    conn::{
        Installed,
        phase::{Borrow, Live, Phase},
    },
};
use dope_runtime::client;
use std::{mem, ptr};

pub(super) struct Lease<'app, 'local: 'app, 'd: 'local, I: catalogs::Catalog> {
    app: client::Bound<'app, 'd, Installed<'local, 'd, I>, ptr::NonNull<Live<'local, 'd, I>>>,
}

impl<'app, 'local: 'app, 'd: 'local, I: catalogs::Catalog> Lease<'app, 'local, 'd, I> {
    const LAYOUT: () = {
        let word = mem::size_of::<usize>();
        assert!(mem::size_of::<Self>() == word);
        assert!(mem::align_of::<Self>() == mem::align_of::<usize>());
        assert!(mem::size_of::<Option<Self>>() == word);
    };

    pub(in crate::engine::conn) fn try_new(
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

    pub(in crate::engine::conn) fn state(&mut self) -> &mut api::State<'local, 'd, I> {
        // SAFETY: this guard exclusively owns the borrowed phase until drop.
        unsafe { &mut *self.app.as_ref().state.get() }
    }
}

impl<I: catalogs::Catalog> Drop for Lease<'_, '_, '_, I> {
    fn drop(&mut self) {
        // SAFETY: Installed stays pinned for the application lease.
        let live = unsafe { self.app.as_ref() };
        live.borrow.set(live.borrow.get().returned());
    }
}

const _: () = Lease::<catalogs::Empty>::LAYOUT;
