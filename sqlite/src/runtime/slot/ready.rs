use crate::api::{batches, exec, stmt, work};
use crate::runtime::slot::lease;
use crate::{catalogs, sys};
use dope_fiber::abi;
use std::{mem, result};
use stmt::{access::Access, target::Target};
use work::cleanup;

pub(crate) struct Ready<'slot, 'd: 'slot, I: catalogs::Catalog = catalogs::Empty> {
    pub(super) engine: stmt::Engine<'slot, I>,
    pub(super) arena: batches::Arena<'slot>,
    pub(super) lease: lease::Lease<'slot, 'd, I, lease::Ready>,
}

/// Consumes executable access while retaining statement and connection retirement.
pub(crate) struct Retirement<'slot, 'd: 'slot, I: catalogs::Catalog> {
    statements: stmt::Retirement<'slot>,
    lease: lease::Lease<'slot, 'd, I, lease::Ready>,
}

impl<'slot, 'd: 'slot, I: catalogs::Catalog> Ready<'slot, 'd, I> {
    pub(crate) fn query_in<'a>(&'a mut self, txn: exec::Tx<'a>) -> exec::Query<'a, 'slot, 'd, I> {
        let (start, arena) = self.start(txn);
        exec::Query::new(start, arena)
    }

    pub(crate) fn control<'a, T: Target<I>>(
        &'a mut self,
        control: T,
        txn: exec::Tx<'a>,
    ) -> impl abi::Fiber<
        'd,
        Output = result::Result<<T::Access as Access>::Output<()>, crate::Error>,
    > + use<'a, 'slot, 'd, I, T> {
        let (start, _) = self.start(txn);
        exec::Command::new(start.run(control, ()))
    }

    fn start<'a>(
        &'a mut self,
        txn: exec::Tx<'a>,
    ) -> (exec::Start<'a, 'slot, 'd, I>, &'a mut batches::Arena<'slot>) {
        (
            exec::Start::new(&mut self.engine, self.lease.work(), self.lease.call(), txn),
            &mut self.arena,
        )
    }

    pub(crate) fn work(&self) -> &'slot work::Work {
        self.lease.work()
    }

    pub(crate) fn pending(&self) -> cleanup::Status {
        self.work().pending.status()
    }

    pub(crate) fn cleanup(&mut self) {
        cleanup::Run::new(
            &mut self.engine,
            self.lease.work(),
            self.lease.config().work.cleanup,
        )
        .park();
    }

    pub(crate) fn retire(self) -> Retirement<'slot, 'd, I> {
        Retirement {
            statements: self.engine.retire(),
            lease: self.lease,
        }
    }

    pub(crate) fn rollback(&self) {
        self.work().pending.transaction();
    }

    pub(crate) fn tx(&self) -> sys::Tx {
        self.engine.transaction()
    }
}

impl<I: catalogs::Catalog> Retirement<'_, '_, I> {
    pub(crate) fn work(&self) -> &work::Work {
        self.lease.work()
    }

    pub(crate) fn retire_one(&mut self) -> stmt::Inventory {
        self.statements.retire_one()
    }

    pub(crate) fn inventory(&self) -> stmt::Inventory {
        self.statements.inventory()
    }

    pub(crate) fn force(&mut self) {
        self.work().reset();
        self.statements.retire_all();
    }
}

const _: () = {
    assert!(mem::size_of::<Retirement<'_, '_, catalogs::Empty>>() == 6 * mem::size_of::<usize>());
};
