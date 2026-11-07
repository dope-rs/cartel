use crate::api::{batches, stmt, work};
use crate::catalogs;
use crate::retry::lease;
use crate::runtime::slot::Ready;
use crate::runtime::slot::lease::Lease;
use o3::mem::lease::Open;
use work::guard::{self, open};

pub(crate) struct Prepare<'slot, 'd, I: catalogs::Catalog> {
    pub(super) prepare: stmt::Prepare<'slot, I>,
    pub(super) arena: batches::Arena<'slot>,
    pub(super) guard: guard::Guard<'slot, open::Open>,
    pub(super) lease: Lease<'slot, 'd, I, Open>,
}

impl<'slot, 'd, I: catalogs::Catalog> Prepare<'slot, 'd, I> {
    pub(crate) fn advance(
        self,
    ) -> Result<stmt::Progress<Self, Ready<'slot, 'd, I>>, (Self, crate::Error)> {
        let (prepare, failure) = match self.prepare.advance(&self.guard) {
            Ok(stmt::Progress::Pending(prepare)) => (prepare, None),
            Err((prepare, error)) => (prepare, Some(error)),
            Ok(stmt::Progress::Ready(engine)) => {
                drop(self.guard);
                return Ok(stmt::Progress::Ready(Ready {
                    engine,
                    arena: self.arena,
                    lease: self.lease.ready(),
                }));
            }
        };
        let next = Self { prepare, ..self };
        match failure {
            None => Ok(stmt::Progress::Pending(next)),
            Some(error) => Err((next, error)),
        }
    }

    pub(crate) fn retire_one(&mut self) -> stmt::Inventory {
        self.prepare.retire_one()
    }

    pub(crate) fn retry(
        &mut self,
    ) -> (
        &guard::Guard<'slot, open::Open>,
        lease::Lease<'_, 'd, lease::Open>,
    ) {
        (&self.guard, self.lease.open())
    }
}
