use crate::{
    catalogs, config, conn, database, retry,
    runtime::{self, heap, slot},
};
use dope_core::driver::storage;
use std::{cell, marker, mem, pin};

/// Pinned SQLite resources owned by one generative driver domain.
pub struct State<'d, I: catalogs::Catalog> {
    retry: retry::raw::Configured<'d>,
    slot: cell::UnsafeCell<slot::Local<I>>,
    phase: runtime::raw::Span,
    _retire: heap::raw::Retained,
    proof: marker::PhantomData<(cell::Cell<&'d I>, marker::PhantomPinned)>,
    value_bytes: i32,
    statement_sql_bytes: i32,
    // The idle heap token has no destructor or backing borrow after _retire drops.
    database: cell::UnsafeCell<Option<(database::Target<'d>, heap::raw::Local)>>,
}

pub(super) struct Settings {
    pub(super) config: config::Config,
    pub(super) value_bytes: i32,
    pub(super) statement_sql_bytes: i32,
}

impl<'d, I: catalogs::Catalog> State<'d, I> {
    pub(in crate::runtime) fn new(
        context: &mut storage::Context<'_, 'd>,
        layout: runtime::Layout<'d, I>,
    ) -> Result<Self, runtime::Error> {
        let (memory, sqlite, collect, stmts, storage, phase) =
            runtime::raw::Memory::new(&layout)?;
        let (settings, database, aside) = layout.into_parts();
        let Settings {
            config,
            value_bytes: values,
            statement_sql_bytes: sql,
        } = settings;
        let retry = retry::raw::Configured::new(config, || {
            context
                .timer()
                .try_alarm()
                .map_err(runtime::Error::TimerCapacity)
        })?;
        let (slot, heap, retained) =
            slot::Local::new(memory, sqlite, collect, stmts, storage, aside)?;
        Ok(Self {
            retry,
            slot: cell::UnsafeCell::new(slot),
            phase,
            _retire: retained,
            proof: marker::PhantomData,
            value_bytes: values,
            statement_sql_bytes: sql,
            database: cell::UnsafeCell::new(Some((database, heap))),
        })
    }

    pub fn config(self: pin::Pin<&Self>) -> &config::Config {
        self.get_ref().retry.config()
    }

    pub(crate) const fn sql_bytes(&self) -> i32 {
        self.statement_sql_bytes
    }

    pub(crate) const fn value_bytes(&self) -> i32 {
        self.value_bytes
    }

    pub(crate) fn try_lease<'local>(
        self: pin::Pin<&'local Self>,
        report: &'local mut crate::open::Completion,
    ) -> Result<
        (
            database::Target<'d>,
            slot::Vacant<'local, 'd, I>,
            conn::raw::Vacant<'local, 'd, I>,
        ),
        crate::open::Error,
    >
    where
        'd: 'local,
    {
        let report = report.admit()?;
        let local = self.get_ref();
        // SAFETY: this thread-bound take invokes no callbacks and retains no field borrow.
        let (database, heap) = unsafe { &mut *local.database.get() }
            .take()
            .ok_or(crate::open::Error::AlreadyStarted)?;
        // SAFETY: the target is consumed once and never restored, so it grants the
        // sole retry loan. Config stays outside every mutable slot projection.
        let retry = unsafe { local.retry.loan() };
        // SAFETY: the same consumed target grants the only mutable local projection.
        // Pin preserves both owners' addresses through their returned lease.
        let slot = unsafe { &mut *local.slot.get() };
        let lease = slot.lease(heap, retry, report)?;
        // SAFETY: Memory reserved this exact catalog's phase layout. The consumed
        // target grants its only initialization, and local retains the backing.
        let phase = unsafe { conn::raw::Vacant::new(&local.phase) };
        Ok((database, lease, phase))
    }
}

const _: () = {
    let fields = mem::size_of::<retry::raw::Configured<'_>>()
        + mem::size_of::<cell::UnsafeCell<slot::Local<catalogs::Empty>>>()
        + mem::size_of::<runtime::raw::Span>()
        + mem::size_of::<heap::raw::Retained>()
        + mem::size_of::<marker::PhantomData<(cell::Cell<&catalogs::Empty>, marker::PhantomPinned)>>(
        )
        + 2 * mem::size_of::<i32>()
        + mem::size_of::<cell::UnsafeCell<Option<(database::Target<'_>, heap::raw::Local)>>>();
    let align = mem::align_of::<State<'_, catalogs::Empty>>();
    assert!(mem::size_of::<State<'_, catalogs::Empty>>() == fields.next_multiple_of(align));
};
