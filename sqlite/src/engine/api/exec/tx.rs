use crate::{
    api::stmt::{
        self,
        access::{Access, Authority, Retry},
    },
    sys,
};
use std::{cell, mem, result};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Tx<'transaction>(Option<&'transaction cell::Cell<Option<sys::Fault>>>);

impl<'transaction> Tx<'transaction> {
    pub(crate) const NONE: Self = Self(None);

    pub(crate) const fn new(failed: &'transaction cell::Cell<Option<sys::Fault>>) -> Self {
        Self(Some(failed))
    }

    pub(crate) fn check(self) -> result::Result<(), crate::Error> {
        let Some(failed) = self.0 else {
            return Ok(());
        };
        match failed.get() {
            Some(fault) => Err(fault.transaction()),
            None => Ok(()),
        }
    }

    pub(crate) fn rollback(self) -> result::Result<(), crate::Error> {
        if let Some(fault) = self.0.and_then(cell::Cell::get)
            && fault.is_aborted()
        {
            return Err(fault.transaction());
        }
        Ok(())
    }

    /// Retains native failures and the rollback owed by an invalid control scope.
    pub(crate) fn error<A: Access>(
        self,
        stmt: &stmt::Prepared<'_>,
        access: &A,
        action: sys::Action,
        code: i32,
    ) -> crate::Error {
        let code = sys::Code::new(code);
        let fault = sys::Fault::new(action, code);
        let busy = code.primary() == libsqlite3_sys::SQLITE_BUSY;
        let control = matches!(A::AUTHORIZATION, Authority::Control);
        if self.0.is_none() && !busy && !control {
            return fault.error();
        }
        let native = stmt.transaction();
        let abort = match native {
            sys::Tx::Idle => true,
            sys::Tx::Active if control && !busy => {
                stmt.work().pending.transaction();
                true
            }
            sys::Tx::Active => false,
        };
        if let Some(state) = self.0 {
            if abort {
                state.set(Some(fault.abort()));
            } else if busy && matches!(access.retry(), Retry::Denied) {
                state.set(Some(fault));
            }
        }
        if busy {
            access.busy(fault, native)
        } else {
            fault.error()
        }
    }
}

const _: () = assert!(
    mem::size_of::<Tx<'static>>()
        == mem::size_of::<Option<&'static cell::Cell<Option<sys::Fault>>>>()
);
