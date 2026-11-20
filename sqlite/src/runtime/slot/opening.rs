use crate::api::{stmt, work};
use crate::retry::lease;
use crate::runtime::slot::{Prepare, lease::Lease};
use crate::sys;
use crate::{catalogs, config, runtime};
use o3::mem::lease::Open;
use work::call::Call;
use work::guard::{self, open};

pub(crate) struct Opening<'slot, 'd, I: catalogs::Catalog> {
    pub(super) guard: guard::Guard<'slot, open::Open>,
    pub(super) connection: &'slot sys::Connection,
    pub(super) statements: &'slot mut stmt::Storage<I>,
    pub(super) arena: &'slot mut runtime::raw::Span,
    pub(super) lookaside: &'slot mut runtime::raw::Span,
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

    pub(crate) fn authorize<const SCHEMA: bool>(&self) {
        self.guard.work().authorizer::<SCHEMA>(self.connection)
    }

    pub(crate) fn parameters(&self) {
        self.connection.limit(
            libsqlite3_sys::SQLITE_LIMIT_VARIABLE_NUMBER,
            self.config().param_limit(),
        );
    }

    pub(crate) fn begin_immediate(
        &self,
        _: crate::open::schema::Transaction,
    ) -> Result<(), crate::Error> {
        self.guard
            .work()
            .control()
            .run(|| self.call().exec("BEGIN IMMEDIATE"))
    }

    pub(crate) fn schema(&self) -> crate::open::Schema<'_> {
        crate::open::Schema::new(self.connection, &self.guard, self.config())
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
        let db = self.connection;
        Prepare {
            // SAFETY: the one-time Local grant retains this catalog's empty
            // storage and its original live connection through the lease.
            statements: unsafe { stmt::Prepare::new(db, self.statements, config.stmt_bytes()) },
            opening: super::prepare::Owner {
                arena: self.arena,
                guard: self.guard,
                lease: self.lease,
            },
        }
    }
}
