use crate::{
    api::{self, stmt},
    catalogs, config, sys,
};
use api::{exec, work};
pub(crate) use controls::Controls;
use guard::open;
pub(crate) use progress::Progress;
use std::{marker, mem, ptr, result};
use stmt::access::Access;
use work::guard;

pub(crate) mod access;
mod binder;
mod bound;
mod control;
mod controls;
mod progress;
mod storage;
pub(crate) use binder::Binder;
pub(crate) use bound::Bound;
pub(crate) use control::Control;
pub(crate) use storage::Storage;
pub(crate) mod target;

pub(crate) enum Inventory {
    Empty,
    Populated,
}

pub(crate) struct Engine<'connection, I: catalogs::Catalog> {
    statements: Set<'connection, I>,
    connection: &'connection crate::sys::Connection,
}

impl<'connection, I: catalogs::Catalog> Engine<'connection, I> {
    pub(crate) fn transaction(&self) -> sys::Tx {
        self.connection.transaction()
    }

    pub(crate) fn controls(&mut self) -> Controls<'_, 'connection> {
        Controls::new(&mut self.statements)
    }

    pub(crate) fn retire(self) -> Retirement<'connection> {
        self.statements.retire()
    }
}

pub(crate) struct Prepare<'connection, I: catalogs::Catalog> {
    statements: Prefix<'connection, I>,
    connection: &'connection crate::sys::Connection,
    capacity: config::Capacity,
    used: usize,
}

impl<'connection, I: catalogs::Catalog> Prepare<'connection, I> {
    /// # Safety
    /// The sole opening admission retains this database and its fresh, empty
    /// catalog storage together until every prepared statement is released.
    pub(crate) unsafe fn new(
        db: &'connection crate::sys::Connection,
        storage: &'connection mut Storage<I>,
        bytes: config::Capacity,
    ) -> Self {
        Self {
            statements: Prefix {
                slots: storage.slots(),
                live: 0,
                marker: marker::PhantomData,
            },
            connection: db,
            capacity: bytes,
            used: 0,
        }
    }

    pub(crate) fn advance(
        mut self,
        guard: &guard::Guard<'connection, open::Open>,
    ) -> result::Result<Progress<Self, Engine<'connection, I>>, (Self, crate::Error)> {
        let result = guard.work().control().run(|| self.insert(guard));
        match result {
            Ok(true) => {
                // SAFETY: insertion completed the exact catalog's entire backing slice.
                let stmts = unsafe { self.statements.finish() };
                Ok(Progress::Ready(Engine {
                    statements: stmts,
                    connection: self.connection,
                }))
            }
            Ok(false) => Ok(Progress::Pending(self)),
            Err(error) => Err((self, error)),
        }
    }

    fn insert(
        &mut self,
        guard: &guard::Guard<'connection, open::Open>,
    ) -> result::Result<bool, crate::Error> {
        let index = self.statements.live;
        let (sql, params, columns) = if let Some(control) = Control::ALL.get(index) {
            (control.sql(), 0, 0)
        } else {
            // SAFETY: the branded storage has exactly the controls and I's entries;
            // this preparation still owns its next vacant position.
            let meta = unsafe { I::STATEMENTS.get_unchecked(index - Control::ALL.len()) };
            (meta.sql(), meta.parameters(), meta.columns())
        };
        let call = guard.call(self.connection);
        let stmt = call.check(Prepared::new(
            call.inner(),
            guard.work(),
            sql,
            params,
            columns,
        ))?;
        let bytes = stmt.bytes();
        let required = self.used + bytes;
        let limit = self.capacity.get();
        if required > limit {
            return Err(crate::Error::StatementBytes {
                required,
                available: limit,
            });
        }
        // SAFETY: pending preparation retains its bounded initialized prefix and
        // the next empty slot; completion consumes it before another insertion.
        let slot = unsafe { self.statements.slots.get_unchecked_mut(index) };
        *slot = Slot {
            statement: Some(stmt),
        };
        self.used = required;
        self.statements.live = index + 1;
        Ok(self.statements.live == self.statements.slots.len())
    }

    pub(crate) fn retire_one(&mut self) -> Inventory {
        self.statements.retire_one()
    }
}

struct Set<'connection, I: catalogs::Catalog> {
    slots: &'connection mut [Slot<'connection>],
    marker: marker::PhantomData<I>,
}

struct Prefix<'connection, I: catalogs::Catalog> {
    slots: &'connection mut [Slot<'connection>],
    live: usize,
    marker: marker::PhantomData<I>,
}

/// Consumed catalog ownership; no statement can be queried after this transition.
pub(crate) struct Retirement<'connection> {
    slots: &'connection mut [Slot<'connection>],
}

pub(super) struct Slot<'connection> {
    statement: Option<Prepared<'connection>>,
}

impl Slot<'_> {
    pub(super) const EMPTY: Self = Self { statement: None };

    pub(super) fn clear(&mut self) {
        self.statement = None;
    }
}

impl<'connection, I: catalogs::Catalog> Set<'connection, I> {
    fn lease<Q: catalogs::TypedQuery>(
        &mut self,
        key: catalogs::Statement<I, Q>,
    ) -> &mut Prepared<'connection>
    where
        I: catalogs::HasQuery<Q>,
    {
        let slot = key.slot();
        let index = config::CONTROL_STATEMENT_ITEMS + slot;
        // SAFETY: admission binds this key to I, and this completed owner retains
        // every initialized control and catalog slot until consuming retirement.
        unsafe {
            self.slots
                .get_unchecked_mut(index)
                .statement
                .as_mut_slice()
                .get_unchecked_mut(0)
        }
    }

    fn retire(mut self) -> Retirement<'connection> {
        Retirement {
            slots: mem::take(&mut self.slots),
        }
    }
}

impl<I: catalogs::Catalog> Drop for Set<'_, I> {
    fn drop(&mut self) {
        Retirement {
            slots: mem::take(&mut self.slots),
        }
        .retire_all();
    }
}

impl<'connection, I: catalogs::Catalog> Prefix<'connection, I> {
    /// # Safety
    /// Every slot in this exact retained backing has been initialized.
    unsafe fn finish(mut self) -> Set<'connection, I> {
        let slots = mem::take(&mut self.slots);
        self.live = 0;
        Set {
            slots,
            marker: marker::PhantomData,
        }
    }

    fn retire_one(&mut self) -> Inventory {
        // SAFETY: preparation advances only this backing's exact initialized prefix.
        let live = unsafe { self.slots.get_unchecked_mut(..self.live) };
        let Some((slot, rest)) = live.split_last_mut() else {
            return Inventory::Empty;
        };
        self.live = rest.len();
        slot.clear();
        match self.live {
            0 => Inventory::Empty,
            _ => Inventory::Populated,
        }
    }
}

impl<I: catalogs::Catalog> Drop for Prefix<'_, I> {
    fn drop(&mut self) {
        let slots = mem::take(&mut self.slots);
        // SAFETY: this owner retains precisely its initialized prefix and backing.
        let slots = unsafe { slots.get_unchecked_mut(..self.live) };
        Retirement { slots }.retire_all();
    }
}

impl Retirement<'_> {
    pub(crate) fn retire_one(&mut self) -> Inventory {
        if let Some((slot, rest)) = mem::take(&mut self.slots).split_last_mut() {
            self.slots = rest;
            slot.clear();
        }
        self.inventory()
    }

    pub(crate) fn retire_all(&mut self) {
        for slot in mem::take(&mut self.slots).iter_mut().rev() {
            slot.clear();
        }
    }

    pub(crate) const fn inventory(&self) -> Inventory {
        if self.slots.is_empty() {
            Inventory::Empty
        } else {
            Inventory::Populated
        }
    }
}

impl Drop for Retirement<'_> {
    fn drop(&mut self) {
        self.retire_all();
    }
}

pub(crate) fn bytes(items: usize) -> Option<usize> {
    let bytes = items.checked_mul(mem::size_of::<Slot<'_>>())?;
    let rounded = bytes.checked_add(storage::ALIGNMENT - 1)?;
    Some((rounded / storage::ALIGNMENT) * storage::ALIGNMENT)
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Set<'static, catalogs::Empty>>() == 2 * word);
    assert!(mem::size_of::<Engine<'static, catalogs::Empty>>() == 3 * word);
    assert!(mem::size_of::<Prefix<'static, catalogs::Empty>>() == 3 * word);
    assert!(mem::size_of::<Prepare<'static, catalogs::Empty>>() == 6 * word);
    assert!(mem::size_of::<Retirement<'static>>() == 2 * word);
    assert!(mem::size_of::<Slot<'static>>() == 4 * word);
    assert!(mem::size_of::<Prepared<'static>>() == 4 * word);
};

pub(crate) struct Prepared<'connection> {
    statement: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    work: &'connection work::Work,
    parameters: usize,
    columns: usize,
    connection: marker::PhantomData<&'connection crate::sys::Connection>,
}

impl<'connection> Prepared<'connection> {
    pub(crate) const fn as_non_null(&self) -> ptr::NonNull<libsqlite3_sys::sqlite3_stmt> {
        self.statement
    }

    pub(in crate::engine::api) fn transaction(&self) -> sys::Tx {
        // SAFETY: this prepared owner retains its live statement and original database.
        unsafe { sys::statement_tx(self.statement) }
    }
    pub(crate) fn new(
        db: &'connection crate::sys::Connection,
        work: &'connection work::Work,
        sql: &str,
        params: usize,
        columns: usize,
    ) -> result::Result<Self, crate::Error> {
        let sql = crate::sys::Sql::new(sql)?;
        sql.prepare(
            db,
            libsqlite3_sys::SQLITE_PREPARE_PERSISTENT,
            Err,
            |stmt, tail| {
                let mut prepared = Self {
                    statement: stmt,
                    work,
                    parameters: 0,
                    columns: 0,
                    connection: marker::PhantomData,
                };
                tail.check()?;
                // SAFETY: this prepared owner retains the live statement and database.
                prepared.parameters = unsafe { sys::statement_parameters(stmt, params) }?;
                prepared.columns = prepared.columns::<true>();
                if columns != usize::MAX && prepared.columns != columns {
                    return Err(crate::Error::ColumnCount {
                        expected: columns,
                        actual: prepared.columns,
                    });
                }
                Ok(prepared)
            },
        )
    }

    /// Samples only fresh persistent preparations, before binding or stepping.
    /// Persistent preparation excludes lookaside; every counted allocation is
    /// owned by the retained Core arena admitted at no more than i32::MAX bytes.
    fn bytes(&self) -> usize {
        let bytes = unsafe {
            libsqlite3_sys::sqlite3_stmt_status(
                self.statement.as_ptr(),
                libsqlite3_sys::SQLITE_STMTSTATUS_MEMUSED,
                0,
            )
        };
        bytes as usize
    }

    fn params(&self) -> usize {
        self.parameters
    }

    fn columns<const CURRENT: bool>(&self) -> usize {
        if !CURRENT {
            return self.columns;
        }
        // SAFETY: this owner retains the live statement and original database.
        unsafe { sys::statement_columns(self.statement) }
    }

    pub(crate) fn step<A: Access>(
        &mut self,
        access: &A,
        txn: exec::Tx<'_>,
    ) -> result::Result<Step, crate::Error> {
        let code = unsafe { libsqlite3_sys::sqlite3_step(self.statement.as_ptr()) };
        match code {
            libsqlite3_sys::SQLITE_ROW => Ok(Step::Row),
            libsqlite3_sys::SQLITE_DONE => Ok(Step::Done),
            code => {
                // SAFETY: this owner retains the failed statement and connection.
                // The admitted native build repeats this step failure on reset;
                // its built-in cleanup cannot replace it with a new error.
                unsafe { libsqlite3_sys::sqlite3_reset(self.statement.as_ptr()) };
                Err(txn.error(self, access, sys::Action::Step, code))
            }
        }
    }

    fn reset<A: Access>(
        &mut self,
        access: &A,
        txn: exec::Tx<'_>,
    ) -> result::Result<(), crate::Error> {
        let code = unsafe { libsqlite3_sys::sqlite3_reset(self.statement.as_ptr()) };
        self.clear();
        if code == libsqlite3_sys::SQLITE_OK {
            Ok(())
        } else {
            Err(txn.error(self, access, sys::Action::Reset, code))
        }
    }

    fn clear(&mut self) {
        // SAFETY: this owner retains its admitted live, nonnull statement.
        // libsqlite3-sys 0.38.1's native clear only fails its null-handle check.
        unsafe { libsqlite3_sys::sqlite3_clear_bindings(self.statement.as_ptr()) };
    }

    fn dispose(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>, work: &work::Work) {
        if let Some(fault) = sys::finalize(stmt) {
            work.finalized(fault);
        }
    }
}

impl Drop for Prepared<'_> {
    fn drop(&mut self) {
        Self::dispose(self.statement, self.work);
    }
}

pub(crate) enum Step {
    Row,
    Done,
}
