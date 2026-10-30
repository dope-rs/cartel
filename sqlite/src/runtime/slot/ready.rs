use crate::api::{batches, exec, stmt, work};
use crate::retry::lease::{Cleanup, Ref};
use crate::runtime::slot::{Held, lease};
use crate::{catalogs, sys};
use dope_core::driver::schedule::clock;
use dope_fiber::abi;
use std::{mem, result, task};
use stmt::{access::Access, target::Target};
use work::cleanup;

#[repr(transparent)]
pub(crate) struct Ready<'slot, 'd: 'slot, I: catalogs::Catalog> {
    pub(super) held: Held<Fields<'slot, I>, lease::Lease<'slot, 'd, I, lease::Ready>>,
}

/// Consumes executable access while retaining statement and connection retirement.
#[repr(transparent)]
pub(crate) struct Retirement<'slot, 'd: 'slot, I: catalogs::Catalog> {
    held: Held<stmt::Retirement<'slot>, lease::Lease<'slot, 'd, I, lease::Ready>>,
}

pub(super) struct Fields<'slot, I: catalogs::Catalog> {
    pub(super) engine: stmt::Engine<'slot, I>,
    pub(super) arena: batches::Arena<'slot>,
}

impl<'slot, 'd: 'slot, I: catalogs::Catalog> Ready<'slot, 'd, I> {
    pub(crate) fn query<'a>(&'a mut self, txn: exec::Tx<'a>) -> exec::Query<'a, 'slot, 'd, I> {
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
        let Held { value, lease } = &mut self.held;
        (
            exec::Start::new(&mut value.engine, lease.work(), lease.cleanup(), txn),
            &mut value.arena,
        )
    }

    pub(crate) fn work(&self) -> &'slot work::Work {
        self.held.lease.work()
    }

    pub(crate) fn pending(&self) -> cleanup::Status {
        self.work().pending.status()
    }

    pub(crate) const fn retry_ref(&self) -> Ref<'_, 'd, Cleanup> {
        self.held.lease.cleanup_ref()
    }

    pub(crate) fn poll_cleanup(&mut self, now: clock::Moment<'d>) -> task::Poll<cleanup::Activity> {
        let Held { value, lease } = &mut self.held;
        cleanup::Run::new(&mut value.engine, lease.work(), lease.cleanup()).poll_park(now)
    }

    pub(crate) fn retire(self) -> Retirement<'slot, 'd, I> {
        let Held { value, lease } = self.held;
        Retirement {
            held: Held::new(value.engine.retire(), lease),
        }
    }

    pub(crate) fn rollback(&self) {
        self.work().pending.transaction();
    }

    pub(crate) fn transaction(&self) -> sys::Tx {
        self.held.value.engine.transaction()
    }
}

impl<I: catalogs::Catalog> Retirement<'_, '_, I> {
    pub(crate) fn work(&self) -> &work::Work {
        self.held.lease.work()
    }

    pub(crate) fn retire_one(&mut self) -> stmt::Inventory {
        self.held.value.retire_one()
    }

    pub(crate) fn inventory(&self) -> stmt::Inventory {
        self.held.value.inventory()
    }

    pub(crate) fn force(&mut self) {
        self.work().reset();
        self.held.value.retire_all();
    }
}

const _: () = {
    assert!(mem::size_of::<Retirement<'_, '_, catalogs::Empty>>() == 6 * mem::size_of::<usize>());
};
