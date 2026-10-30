use crate::{
    catalogs, config, conn, database, retry,
    runtime::{self, heap, release, slot},
};
use dope_core::driver::storage;
use heap::layout::Layout;
use std::{cell, marker, mem, pin, ptr};

/// Pinned SQLite resources owned by one generative driver domain.
pub struct State<'d, I: catalogs::Catalog> {
    retry: retry::raw::Configured<'d>,
    slot: cell::UnsafeCell<slot::Local<I>>,
    phase: runtime::raw::Span,
    _retire: Retire,
    proof: marker::PhantomData<(cell::Cell<&'d I>, marker::PhantomPinned)>,
    value_bytes: i32,
    statement_sql_bytes: i32,
    database: cell::UnsafeCell<Option<database::Target<'d>>>,
}

pub(super) struct Settings {
    pub(super) config: config::Config,
    pub(super) value_bytes: i32,
    pub(super) statement_sql_bytes: i32,
}

/// Drops the arena proof before reclaiming its backing allocation.
struct Resources {
    heap: heap::raw::Retained,
    memory: runtime::raw::Memory,
}

struct Retire(ptr::NonNull<Resources>);

impl<'d, I: catalogs::Catalog> State<'d, I> {
    pub(crate) const RETENTION_BYTES: usize = mem::size_of::<Resources>().div_ceil(64) * 64;

    pub(in crate::runtime) fn new(
        context: &mut storage::Context<'_, 'd>,
        settings: Settings,
        sqlite: Layout,
        database: database::Target<'d>,
        stmts: usize,
        aside: crate::memory::Lookaside,
    ) -> Result<Self, runtime::Error> {
        let Settings {
            config,
            value_bytes: values,
            statement_sql_bytes: sql,
        } = settings;
        let (memory, sqlite, collect, stmts, storage, phase) = runtime::raw::Memory::new(
            sqlite,
            config.result_bytes(),
            stmts,
            aside.bytes(),
            conn::raw::Vacant::<'_, 'd, I>::LAYOUT,
        )?;
        let retry = retry::raw::Configured::new(config, || {
            context
                .timer()
                .try_alarm()
                .map_err(runtime::Error::TimerCapacity)
        })?;
        let (slot, heap) = slot::Local::new(sqlite, collect, stmts, storage, aside)?;
        Ok(Self {
            retry,
            slot: cell::UnsafeCell::new(slot),
            phase,
            _retire: Resources::place(memory, heap),
            proof: marker::PhantomData,
            value_bytes: values,
            statement_sql_bytes: sql,
            database: cell::UnsafeCell::new(Some(database)),
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
        let database = unsafe { &mut *local.database.get() }
            .take()
            .ok_or(crate::open::Error::AlreadyStarted)?;
        // SAFETY: the target is consumed once and never restored, so it grants the
        // sole retry loan. Config stays outside every mutable slot projection.
        let retry = unsafe { local.retry.loan() };
        // SAFETY: the same consumed target grants the only mutable local projection.
        // Pin preserves both owners' addresses through their returned lease.
        let slot = unsafe { &mut *local.slot.get() };
        let lease = slot.lease(retry, report)?;
        // SAFETY: Memory reserved this exact catalog's phase layout. The consumed
        // target grants its only initialization, and local retains the backing.
        let phase = unsafe { conn::raw::Vacant::new(&local.phase) };
        Ok((database, lease, phase))
    }
}

impl Resources {
    fn place(mut memory: runtime::raw::Memory, heap: heap::raw::Retained) -> Retire {
        let pointer = memory.base().cast::<Self>();
        // SAFETY: Memory reserves this aligned prefix for its unique retirement owner.
        unsafe { pointer.as_ptr().write(Self { heap, memory }) };
        Retire(pointer)
    }
}

impl Drop for Retire {
    fn drop(&mut self) {
        // SAFETY: this unique non-Copy owner moves its initialized prefix once in Drop.
        let Resources { heap, memory } = unsafe { self.0.as_ptr().read() };
        heap.arm(release::Release::new(memory));
    }
}

const _: () = {
    assert!(mem::align_of::<Resources>() <= 64);
    let fields = mem::size_of::<retry::raw::Configured<'_>>()
        + mem::size_of::<cell::UnsafeCell<slot::Local<catalogs::Empty>>>()
        + mem::size_of::<runtime::raw::Span>()
        + mem::size_of::<Retire>()
        + mem::size_of::<marker::PhantomData<(cell::Cell<&catalogs::Empty>, marker::PhantomPinned)>>(
        )
        + 2 * mem::size_of::<i32>()
        + mem::size_of::<cell::UnsafeCell<Option<database::Target<'_>>>>();
    let align = mem::align_of::<State<'_, catalogs::Empty>>();
    assert!(mem::size_of::<State<'_, catalogs::Empty>>() == fields.next_multiple_of(align));
};
