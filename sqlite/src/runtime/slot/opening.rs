use crate::api::{batches, stmt, work};
use crate::retry::lease;
use crate::runtime::slot::{Prepare, lease::Lease};
use crate::sys;
use crate::{catalogs, config};
use o3::mem::lease::Open;
use work::call::Call;
use work::guard::{self, open};

pub(crate) struct Opening<'slot, 'd, I: catalogs::Catalog> {
    pub(super) guard: guard::Guard<'slot, open::Open>,
    pub(super) connection: &'slot sys::Connection,
    pub(super) statements: &'slot mut stmt::Storage<I>,
    pub(super) arena: batches::Arena<'slot>,
    pub(super) lookaside: &'slot mut crate::runtime::raw::Span,
    pub(super) lookaside_layout: crate::memory::Lookaside,
    pub(super) lease: Lease<'slot, 'd, I, Open>,
}

impl<'slot, 'd, I: catalogs::Catalog> Opening<'slot, 'd, I> {
    pub(crate) fn config(&self) -> &'slot config::Config {
        self.lease.config()
    }

    pub(crate) fn retry(
        &mut self,
    ) -> (
        &guard::Guard<'slot, open::Open>,
        lease::Lease<'_, 'd, lease::Open>,
    ) {
        (&self.guard, self.lease.open())
    }

    pub(crate) fn call<'a>(&'a self) -> Call<'a, 'a, 'slot> {
        let db: &'a sys::Connection = self.connection;
        self.guard.call(db)
    }

    pub(crate) fn authorize(&self) {
        self.guard.work().authorizer(self.connection)
    }

    pub(crate) fn install(&mut self) -> Result<(), crate::Error> {
        self.connection
            .lookaside(self.lookaside.as_mut_ptr(), self.lookaside_layout)
    }

    pub(crate) fn limit(&self, category: i32, value: i32) -> i32 {
        self.connection.limit(category, value)
    }

    pub(crate) fn prepare(self) -> Prepare<'slot, 'd, I> {
        let config = self.config();
        let params = config.param_limit();
        let db = self.connection;
        db.limit(libsqlite3_sys::SQLITE_LIMIT_VARIABLE_NUMBER, params);
        Prepare {
            // SAFETY: the one-time Local grant retains this catalog's empty
            // storage and its original live connection through the lease.
            prepare: unsafe { stmt::Prepare::new(db, self.statements, config.stmt_bytes()) },
            arena: self.arena,
            guard: self.guard,
            lease: self.lease,
        }
    }
}
