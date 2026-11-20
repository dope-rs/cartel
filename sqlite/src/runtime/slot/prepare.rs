use crate::api::{stmt, work};
use crate::retry::lease;
use crate::runtime::slot::{Ready, lease::Lease};
use crate::{catalogs, runtime};
use o3::mem::lease::Open;
use work::guard::{self, open};

pub(crate) struct Stage<'slot, 'd, I: catalogs::Catalog, S> {
    pub(super) statements: S,
    pub(super) opening: Owner<'slot, 'd, I>,
}

pub(super) struct Owner<'slot, 'd, I: catalogs::Catalog> {
    pub(super) arena: &'slot mut runtime::raw::Span,
    pub(super) guard: guard::Guard<'slot, open::Open>,
    pub(super) lease: Lease<'slot, 'd, I, Open>,
}

pub(crate) type Prepare<'s, 'd, I> = Stage<'s, 'd, I, stmt::Prepare<'s, I>>;
pub(crate) type Complete<'s, 'd, I> = Stage<'s, 'd, I, stmt::Engine<'s, I>>;
pub(crate) type Failed<'s, 'd, I> = Stage<'s, 'd, I, stmt::Retirement<'s>>;

impl<'s, 'd, I: catalogs::Catalog, S> Stage<'s, 'd, I, S> {
    pub(crate) fn retry(
        &mut self,
    ) -> (
        &guard::Guard<'s, open::Open>,
        lease::Lease<'_, 'd, lease::Open>,
    ) {
        (&self.opening.guard, self.opening.lease.open())
    }
}

impl<'s, 'd, I: catalogs::Catalog> Prepare<'s, 'd, I> {
    pub(crate) fn advance(
        self,
    ) -> Result<stmt::Progress<Self, Complete<'s, 'd, I>>, (Self, crate::Error)> {
        match self.statements.advance(&self.opening.guard) {
            Ok(stmt::Progress::Pending(statements)) => {
                Ok(stmt::Progress::Pending(Self { statements, ..self }))
            }
            Ok(stmt::Progress::Ready(statements)) => Ok(stmt::Progress::Ready(Stage {
                statements,
                opening: self.opening,
            })),
            Err((statements, error)) => Err((Self { statements, ..self }, error)),
        }
    }

    pub(crate) fn retire(mut self) -> Failed<'s, 'd, I> {
        Stage {
            statements: self.statements.retire(),
            opening: self.opening,
        }
    }
}

impl<'s, 'd, I: catalogs::Catalog> Complete<'s, 'd, I> {
    pub(crate) fn ready(self) -> Ready<'s, 'd, I> {
        drop(self.opening.guard);
        Ready {
            engine: self.statements,
            arena: self.opening.arena,
            lease: self.opening.lease.ready(),
        }
    }

    pub(crate) fn commit(
        &mut self,
        _: crate::open::schema::Transaction,
    ) -> Result<(), crate::Error> {
        // Charge the fixed control before its irreversible native completion.
        let guard = &self.opening.guard;
        let meter = guard.work().control();
        guard.charge(Ok(()))?;
        let result = meter.run(|| self.statements.controls().execute(stmt::Control::Commit));
        match result {
            Ok(()) => Ok(()),
            Err(error) => guard.check(Err(error)),
        }
    }

    pub(crate) fn retire(self) -> Failed<'s, 'd, I> {
        Stage {
            statements: self.statements.retire(),
            opening: self.opening,
        }
    }
}

impl<I: catalogs::Catalog> Failed<'_, '_, I> {
    pub(crate) fn retire_one(&mut self) -> stmt::Inventory {
        self.statements.retire_one()
    }
}
