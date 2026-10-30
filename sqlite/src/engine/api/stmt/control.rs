use crate::{api::stmt::access, config};

#[derive(Clone, Copy)]
pub(crate) enum Control {
    Begin,
    Commit,
    Rollback,
    Savepoint,
    Release,
    RollbackTo,
}

impl Control {
    pub(crate) const MAX_SQL_BYTES: usize = Self::RollbackTo.sql().len();

    pub(super) const ALL: [Self; config::CONTROL_STATEMENT_ITEMS] = [
        Self::Begin,
        Self::Commit,
        Self::Rollback,
        Self::Savepoint,
        Self::Release,
        Self::RollbackTo,
    ];

    pub(super) const fn index(self) -> usize {
        self as usize
    }

    pub(super) const fn sql(&self) -> &str {
        match self {
            Self::Begin => "BEGIN DEFERRED",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
            Self::Savepoint => "SAVEPOINT \"__cartel\"",
            Self::Release => "RELEASE \"__cartel\"",
            Self::RollbackTo => "ROLLBACK TO \"__cartel\"",
        }
    }

    pub(crate) const fn retry(self) -> access::Retry {
        match self {
            Self::Commit => access::Retry::Allowed,
            Self::Begin | Self::Rollback | Self::Savepoint | Self::Release | Self::RollbackTo => {
                access::Retry::Denied
            }
        }
    }
}
