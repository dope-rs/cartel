use crate::api::{batches, stmt, work};
use crate::catalogs;
use crate::retry::lease;
use crate::runtime::slot::lease::Lease;
use crate::runtime::slot::{Held, Ready, ready};
use o3::mem::lease::Open;
use work::guard::{self, open};

#[repr(transparent)]
pub(crate) struct Prepare<'slot, 'd, I: catalogs::Catalog> {
    pub(super) held: Held<Fields<'slot, I>, Lease<'slot, 'd, I, Open>>,
}

pub(super) struct Fields<'slot, I: catalogs::Catalog> {
    pub(super) prepare: stmt::Prepare<'slot, I>,
    pub(super) arena: batches::Arena<'slot>,
    pub(super) guard: guard::Guard<'slot, open::Open>,
}

impl<'slot, 'd, I: catalogs::Catalog> Prepare<'slot, 'd, I> {
    pub(crate) fn advance(
        self,
    ) -> Result<stmt::Progress<Self, Ready<'slot, 'd, I>>, (Self, crate::Error)> {
        let Held { value, lease } = self.held;
        let Fields {
            prepare,
            arena,
            guard,
        } = value;
        let (prepare, failure) = match prepare.advance(&guard) {
            Ok(stmt::Progress::Pending(prepare)) => (prepare, None),
            Err((prepare, error)) => (prepare, Some(error)),
            Ok(stmt::Progress::Ready(engine)) => {
                drop(guard);
                return Ok(stmt::Progress::Ready(Ready {
                    held: Held::new(ready::Fields { engine, arena }, lease.ready()),
                }));
            }
        };
        let next = Self {
            held: Held::new(
                Fields {
                    prepare,
                    arena,
                    guard,
                },
                lease,
            ),
        };
        match failure {
            None => Ok(stmt::Progress::Pending(next)),
            Some(error) => Err((next, error)),
        }
    }

    pub(crate) fn retire_one(&mut self) -> stmt::Inventory {
        self.held.value.prepare.retire_one()
    }

    pub(crate) fn retry(&mut self) -> lease::Lease<'_, 'd, lease::Open> {
        self.held.lease.open()
    }
}
