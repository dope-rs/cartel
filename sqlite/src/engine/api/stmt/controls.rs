use crate::{api, catalogs, config};
use api::{exec, stmt};
use std::{mem, result};
use stmt::access;

pub(crate) struct Controls<'a, 'connection> {
    slots: &'a mut [stmt::Slot<'connection>],
}

impl<'a, 'connection> Controls<'a, 'connection> {
    pub(super) fn new<I: catalogs::Catalog>(stmts: &'a mut stmt::Set<'connection, I>) -> Self {
        Self {
            slots: &mut *stmts.slots,
        }
    }

    pub(crate) fn bind<P: api::Bind>(
        self,
        control: stmt::Control,
        params: P,
        config: &config::Config,
    ) -> result::Result<stmt::Bound<'a, 'connection, P>, crate::Error> {
        stmt::Bound::new(Self::lease(self.slots, control), params, config, 0)
    }

    /// The retained control slot contains a fixed transaction program, which
    /// cannot produce a row, including after native reprepare.
    pub(crate) fn execute(&mut self, control: stmt::Control) -> result::Result<(), crate::Error> {
        let access = access::Control::new(control.retry());
        let stmt = Self::lease(&mut *self.slots, control);
        match stmt.step(&access, exec::Tx::NONE) {
            Ok(_) => stmt.reset(&access, exec::Tx::NONE),
            Err(error) => {
                stmt.clear();
                Err(error)
            }
        }
    }

    fn lease<'b>(
        slots: &'b mut [stmt::Slot<'connection>],
        control: stmt::Control,
    ) -> &'b mut stmt::Prepared<'connection> {
        // SAFETY: construction borrows a complete catalog, including every
        // control; this loan excludes retirement for its entire lifetime.
        unsafe {
            slots
                .get_unchecked_mut(control.index())
                .statement
                .as_mut_slice()
                .get_unchecked_mut(0)
        }
    }
}

const _: () = assert!(mem::size_of::<Controls<'static, 'static>>() == 2 * mem::size_of::<usize>());
