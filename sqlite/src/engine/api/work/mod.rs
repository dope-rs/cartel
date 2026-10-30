use crate::{api, config, limits, sys};
use api::work;
use cleanup::Pending;
pub(crate) use depth::{Count, Depth, Permit};
use guard::Guard;
use health::Health;
pub(crate) use meter::Meter;
use std::{mem, ptr, result};
use work::meter::{Permission, Tick};

pub(crate) mod call;
pub(crate) mod cleanup;
mod depth;
pub(crate) mod guard;
mod health;
mod meter;
pub(crate) mod raw;

pub(crate) struct Work {
    meter: Meter,
    health: Health,
    pub(crate) pending: Pending,
    pub(crate) depth: Depth,
}

/// The consumed cleanup owner has terminalized its source before issuing this receipt.
pub(in crate::engine::api::work) struct Failed<'work>(&'work Work);

impl Work {
    pub(crate) const fn new() -> Self {
        Self {
            meter: Meter::new(),
            health: Health::new(),
            pending: Pending::new(),
            depth: Depth::new(),
        }
    }

    pub(crate) fn reset(&self) {
        self.meter.reset();
        self.health.reset();
        self.pending.reset();
        self.depth.reset();
    }

    fn fail_cleanup(&self) -> Failed<'_> {
        self.pending.fail();
        self.depth.reset();
        self.health.fail();
        self.meter.reset();
        Failed(self)
    }

    pub(crate) fn finalized(&self, error: sys::Finalized) {
        self.health.finalized(error);
    }

    pub(crate) fn revert(&self, depth: Count) {
        self.pending.savepoint(depth);
    }

    pub(crate) const fn control(&self) -> &Meter {
        &self.meter
    }

    pub(crate) fn begin<R>(
        &self,
        primary: limits::Progress,
    ) -> result::Result<Guard<'_, R>, crate::Error> {
        self.health.check()?;
        self.meter.begin_call(primary)?;
        Ok(Guard::new(self))
    }

    /// The owning native lease delivers retained errors only after all work has stopped.
    pub(crate) fn take_errors(&self) -> Option<(crate::Error, Option<crate::Error>)> {
        self.health.take()
    }
}

impl Failed<'_> {
    fn retain(self, error: crate::Error) {
        self.0.health.cleanup(error);
    }
}

const _: () = assert!(mem::size_of::<Work>() <= 11 * mem::size_of::<usize>());

pub(crate) struct Hook<'a>(&'a work::Work);

impl<'a> Hook<'a> {
    pub(crate) fn new(work: &'a work::Work) -> Self {
        Self(work)
    }

    fn tick(&self) -> Tick {
        self.0.meter.tick()
    }

    fn as_ptr(&self) -> *mut std::ffi::c_void {
        ptr::from_ref(self.0).cast_mut().cast()
    }

    pub(crate) fn progress(&self, db: &sys::Connection, interval: i32) {
        unsafe {
            libsqlite3_sys::sqlite3_progress_handler(
                db.as_ptr(),
                interval,
                Some(progress),
                self.as_ptr(),
            );
        }
    }

    /// The admitted connection remains open until its exclusive owner drops.
    /// The native setter only rejects an invalid handle; registration and
    /// advisory statement expiration cannot allocate or invoke callbacks.
    pub(crate) fn authorizer(&self, db: &sys::Connection) {
        unsafe {
            libsqlite3_sys::sqlite3_set_authorizer(db.as_ptr(), Some(authorize), self.as_ptr());
        }
    }

    /// Refreshes callbacks before a forgotten borrow is reclaimed by its pinned owner.
    pub(crate) fn refresh(&self, db: &sys::Connection) {
        self.progress(db, config::Budget::PROGRESS_INTERVAL);
        self.authorizer(db);
    }
}

unsafe extern "C" fn progress(context: *mut std::ffi::c_void) -> std::ffi::c_int {
    let hook = Hook(unsafe { &*context.cast() });
    match hook.tick() {
        Tick::Continue => 0,
        Tick::Stop => 1,
    }
}

unsafe extern "C" fn authorize(
    context: *mut std::ffi::c_void,
    action: std::ffi::c_int,
    _: *const std::ffi::c_char,
    second: *const std::ffi::c_char,
    _: *const std::ffi::c_char,
    _: *const std::ffi::c_char,
) -> std::ffi::c_int {
    let work = unsafe { &*context.cast::<work::Work>() };
    let denied = match action {
        libsqlite3_sys::SQLITE_PRAGMA
        | libsqlite3_sys::SQLITE_ATTACH
        | libsqlite3_sys::SQLITE_DETACH
        | libsqlite3_sys::SQLITE_CREATE_VTABLE
        | libsqlite3_sys::SQLITE_DROP_VTABLE => true,
        libsqlite3_sys::SQLITE_FUNCTION => {
            second.is_null()
                || unsafe { std::ffi::CStr::from_ptr(second) }
                    .to_bytes()
                    .eq_ignore_ascii_case(b"load_extension")
        }
        libsqlite3_sys::SQLITE_TRANSACTION | libsqlite3_sys::SQLITE_SAVEPOINT => {
            match work.meter.permission() {
                Permission::Denied => true,
                Permission::Granted => false,
            }
        }
        action => !(libsqlite3_sys::SQLITE_CREATE_INDEX..=libsqlite3_sys::SQLITE_RECURSIVE)
            .contains(&action),
    };
    if denied {
        libsqlite3_sys::SQLITE_DENY
    } else {
        libsqlite3_sys::SQLITE_OK
    }
}
