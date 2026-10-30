use crate::api::{stmt, work};
use crate::retry::lease;
use crate::runtime::slot::local::Occupied;
use crate::runtime::slot::{Held, Prepare, prepare};
use crate::sys;
use crate::{catalogs, config};
use work::call::Call;
use work::guard::{self, open};

#[repr(transparent)]
pub(crate) struct Opening<'slot, 'd, I: catalogs::Catalog> {
    pub(super) held: Held<guard::Guard<'slot, open::Open>, Occupied<'slot, 'd, I>>,
}

impl<'slot, 'd, I: catalogs::Catalog> Opening<'slot, 'd, I> {
    pub(crate) const fn config(&self) -> &'slot config::Config {
        self.held.lease.lease.config()
    }

    pub(crate) fn retry(&mut self) -> lease::Lease<'_, 'd, lease::Open> {
        self.held.lease.lease.open()
    }

    pub(crate) fn call<'a>(&'a self) -> Call<'a, 'a, 'slot> {
        let db: &'a sys::Connection = self.held.lease.connection;
        self.held.value.call(db)
    }

    pub(crate) fn authorize(&self) {
        work::Hook::new(self.held.value.work()).authorizer(self.held.lease.connection)
    }

    pub(crate) fn install(&mut self) -> Result<(), crate::Error> {
        let slot = &mut self.held.lease;
        slot.connection
            .lookaside(slot.lookaside.as_mut_ptr(), slot.lookaside_layout)
    }

    pub(crate) fn limit(&self, category: i32, value: i32) -> i32 {
        self.held.lease.connection.limit(category, value)
    }

    pub(crate) fn prepare(self) -> Prepare<'slot, 'd, I> {
        let config = self.config();
        let params = config.param_limit();
        let db = self.held.lease.connection;
        db.limit(libsqlite3_sys::SQLITE_LIMIT_VARIABLE_NUMBER, params);
        let Held {
            value: guard,
            lease:
                Occupied {
                    connection: db,
                    statements: slots,
                    arena,
                    lookaside: _,
                    lookaside_layout: _,
                    lease,
                },
        } = self.held;
        Prepare {
            held: Held::new(
                prepare::Fields {
                    // SAFETY: the one-time Local grant retains this catalog's empty
                    // storage and its original live connection through the lease.
                    prepare: unsafe { stmt::Prepare::new(db, slots, config.stmt_bytes()) },
                    arena,
                    guard,
                },
                lease,
            ),
        }
    }
}
