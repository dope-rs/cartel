use crate::api::{batches, stmt, work};
use crate::retry;
use crate::runtime::slot::{Held, Opening, lease::Lease};
use crate::runtime::{self, Error, heap};
use crate::{catalogs, config, sys};
use heap::scopes::{Conflict, Scope};
use o3::mem::lease::Open;
use std::{mem, ptr};

pub(crate) struct Local<I: catalogs::Catalog> {
    owned: Owned<I>,
    arena: runtime::raw::Span,
    work: work::Work,
    lookaside: runtime::raw::Span,
    lookaside_layout: crate::memory::Lookaside,
}

/// The pinned resource owner retains cleanup even when an installation is forgotten.
pub(super) struct Owned<I: catalogs::Catalog> {
    connection: Option<crate::sys::Connection>,
    statements: stmt::Storage<I>,
    phase: Phase,
}

enum Phase {
    Idle(heap::raw::Local),
    Active(Scope),
    Done,
}

impl<I: catalogs::Catalog> Local<I> {
    pub(crate) fn new(
        sqlite: heap::raw::Span,
        collect: runtime::raw::Span,
        stmts: runtime::raw::Span,
        aside: runtime::raw::Span,
        layout: crate::memory::Lookaside,
    ) -> Result<(Self, heap::raw::Retained), Error> {
        let (sqlite, retained) = heap::raw::Local::from_span(sqlite);
        let stmts = stmt::Storage::new(stmts)?;
        Ok((
            Self {
                owned: Owned {
                    connection: None,
                    statements: stmts,
                    phase: Phase::Idle(sqlite),
                },
                arena: collect,
                work: work::Work::new(),
                lookaside: aside,
                lookaside_layout: layout,
            },
            retained,
        ))
    }

    pub(crate) fn lease<'slot, 'd: 'slot>(
        &'slot mut self,
        retry: retry::raw::Loan<'slot, 'd>,
        report: &'slot mut crate::open::Completion,
    ) -> Result<Vacant<'slot, 'd, I>, crate::Error> {
        self.owned.enter()?;
        let arena = batches::Arena::from_span(&mut self.arena);
        let owned = ptr::NonNull::from(&mut self.owned);
        let lease = Lease::new(owned, retry, &self.work, report);
        Ok(Vacant {
            connection: &mut self.owned.connection,
            statements: &mut self.owned.statements,
            arena,
            lookaside: &mut self.lookaside,
            lookaside_layout: self.lookaside_layout,
            lease,
        })
    }
}

impl<I: catalogs::Catalog> Drop for Local<I> {
    fn drop(&mut self) {
        self.owned.retire(&self.work);
    }
}

impl<I: catalogs::Catalog> Owned<I> {
    fn enter(&mut self) -> Result<(), crate::Error> {
        let phase = mem::replace(&mut self.phase, Phase::Done);
        let Phase::Idle(heap) = phase else {
            self.phase = phase;
            return Err(crate::Error::LocalAllocatorOccupied);
        };
        match Scope::enter(heap) {
            Ok(scope) => {
                self.phase = Phase::Active(scope);
                Ok(())
            }
            Err((heap, Conflict)) => {
                self.phase = Phase::Idle(heap);
                Err(crate::Error::LocalAllocatorOccupied)
            }
        }
    }

    /// Borrowed statement owners have already retired before their Reclaim is returned.
    pub(super) fn finish(&mut self) {
        self.connection = None;
        if let Phase::Active(scope) = mem::replace(&mut self.phase, Phase::Done) {
            drop(scope);
        }
    }
    /// No borrower can execute after this pinned owner begins destruction.
    /// Forgotten owners leave initialized statement slots and callback contexts here.
    fn retire(&mut self, work: &work::Work) {
        if let Phase::Active(_) = self.phase {
            if let Some(db) = &self.connection {
                work.refresh(db);
            }
            self.statements.retire_all(work);
            self.finish();
        }
    }
}

pub(crate) struct Vacant<'slot, 'd, I: catalogs::Catalog> {
    connection: &'slot mut Option<crate::sys::Connection>,
    statements: &'slot mut stmt::Storage<I>,
    arena: batches::Arena<'slot>,
    lookaside: &'slot mut runtime::raw::Span,
    lookaside_layout: crate::memory::Lookaside,
    lease: Lease<'slot, 'd, I, Open>,
}

impl<'slot, 'd, I: catalogs::Catalog> Vacant<'slot, 'd, I> {
    pub(super) fn insert(self, value: crate::sys::Connection) -> Occupied<'slot, 'd, I> {
        let Self {
            connection: db,
            statements: stmts,
            arena,
            lookaside: aside,
            lookaside_layout: layout,
            lease,
        } = self;
        let db = db.insert(value);
        Occupied {
            connection: db,
            statements: stmts,
            arena,
            lookaside: aside,
            lookaside_layout: layout,
            lease,
        }
    }

    pub(crate) fn open(self, db: sys::Connection) -> Result<Opening<'slot, 'd, I>, crate::Error> {
        let config = self.lease.config();
        let work = self.lease.work();
        let guard = work.begin(config.work.open.progress)?;
        work.progress(&db, config::Budget::PROGRESS_INTERVAL);
        Ok(Opening {
            held: Held::new(guard, self.insert(db)),
        })
    }
}

pub(super) struct Occupied<'slot, 'd, I: catalogs::Catalog> {
    pub(super) connection: &'slot crate::sys::Connection,
    pub(super) statements: &'slot mut stmt::Storage<I>,
    pub(super) arena: batches::Arena<'slot>,
    pub(super) lookaside: &'slot mut runtime::raw::Span,
    pub(super) lookaside_layout: crate::memory::Lookaside,
    pub(super) lease: Lease<'slot, 'd, I, Open>,
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Phase>() == 2 * word);
    let previous = mem::size_of::<Option<crate::sys::Connection>>()
        + mem::size_of::<stmt::Storage<catalogs::Empty>>()
        + 2 * mem::size_of::<runtime::raw::Span>()
        + mem::size_of::<work::Work>()
        + mem::size_of::<heap::raw::Local>()
        + mem::size_of::<crate::memory::Lookaside>();
    let align = mem::align_of::<Local<catalogs::Empty>>();
    assert!(mem::size_of::<Local<catalogs::Empty>>() == previous.next_multiple_of(align) + word);
};
