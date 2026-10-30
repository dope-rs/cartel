use crate::sys;
use cartel_core::deadline;

#[derive(Clone, Copy)]
pub(crate) struct Fault {
    action: sys::Action,
    code: sys::Code,
    aborted: bool,
}

impl Fault {
    pub(crate) const fn new(action: sys::Action, code: sys::Code) -> Self {
        Self {
            action,
            code,
            aborted: false,
        }
    }

    pub(crate) const fn abort(mut self) -> Self {
        self.aborted = true;
        self
    }

    pub(crate) const fn is_aborted(self) -> bool {
        self.aborted
    }

    /// Finalization reports the last step error again after destroying its statement.
    pub(super) fn is_repeated<T>(&self, result: &Result<T, crate::Error>) -> bool {
        match result {
            Err(
                crate::Error::Sqlite { action, code }
                | crate::Error::Busy { action, code }
                | crate::Error::Locked { action, code },
            ) => *action == sys::Action::Step && *code == self.code,
            _ => false,
        }
    }

    pub(crate) fn error(self) -> crate::Error {
        match self.code.primary() {
            libsqlite3_sys::SQLITE_BUSY => self.busy(),
            libsqlite3_sys::SQLITE_LOCKED => crate::Error::Locked {
                action: self.action,
                code: self.code,
            },
            _ => crate::Error::Sqlite {
                action: self.action,
                code: self.code,
            },
        }
    }

    pub(crate) const fn busy(self) -> crate::Error {
        crate::Error::Busy {
            action: self.action,
            code: self.code,
        }
    }

    pub(crate) const fn transaction(self) -> crate::Error {
        if self.aborted {
            crate::Error::TransactionAborted {
                action: self.action,
                code: self.code,
            }
        } else {
            crate::Error::TransactionBusy {
                action: self.action,
                code: self.code,
            }
        }
    }

    pub(crate) const fn timeout(self) -> crate::Error {
        crate::Error::RequestTimeout {
            elapsed: deadline::Elapsed::new(deadline::RequestPhase::Contention),
            action: self.action,
            code: self.code,
        }
    }
}

const _: () = {
    assert!(size_of::<Fault>() <= size_of::<usize>());
    assert!(size_of::<Option<Fault>>() <= size_of::<usize>());
};
