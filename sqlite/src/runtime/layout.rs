use crate::{
    catalogs, config, database, open,
    runtime::{self, heap, local},
};
use dope_core::driver::settings::{claim, errors};
use dope_core::driver::{
    settings::{self, demands, demands::slots::Slots},
    storage,
};
use dope_runtime::topology;
use heap::layout;
use std::marker;

/// Exact SQLite storage factory for one driver domain and query catalog.
pub struct Layout<'sql, I: catalogs::Catalog> {
    settings: local::Settings,
    sqlite: layout::Layout,
    database: database::Target<'sql>,
    statement_bytes: usize,
    lookaside: crate::memory::Lookaside,
    marker: marker::PhantomData<I>,
}

impl<'sql, I: catalogs::Catalog> Layout<'sql, I> {
    pub(super) const fn dimensions(&self) -> (layout::Layout, usize, usize, usize) {
        (
            self.sqlite,
            self.settings.config.result_bytes(),
            self.statement_bytes,
            self.lookaside.bytes(),
        )
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        local::Settings,
        database::Target<'sql>,
        crate::memory::Lookaside,
    ) {
        (self.settings, self.database, self.lookaside)
    }
}

impl<'sql> database::Target<'sql> {
    pub fn plan<I: catalogs::Catalog>(
        self,
        limits: crate::Limits,
        memory: crate::Memory,
    ) -> Result<topology::Plan<Layout<'sql, I>, open::Plan<I>>, database::Error> {
        catalogs::validate::<I>().map_err(database::Error::Catalog)?;
        let config = config::Config::from_parts(limits, memory)?;
        let (plan, sqlite) = memory.admit::<I>().map_err(database::Error::Runtime)?;
        if plan.targets.retained > sqlite.capacity() {
            return Err(database::Error::Runtime(runtime::Error::SqliteCapacity));
        }
        let values = sqlite.capacity();
        if values > i32::MAX as usize {
            return Err(database::Error::Runtime(runtime::Error::SqliteMemory {
                actual: values,
            }));
        }
        let actual = plan.statements.sql_bytes.max(self.sql_bytes());
        if actual > i32::MAX as usize {
            return Err(database::Error::Runtime(
                runtime::Error::StatementSqlBytes { actual },
            ));
        }
        let sql = actual as i32;
        let layout = Layout {
            settings: local::Settings {
                config,
                value_bytes: values as i32,
                statement_sql_bytes: sql,
            },
            sqlite,
            database: self,
            statement_bytes: plan.statements.slots_bytes,
            lookaside: memory.lookaside,
            marker: marker::PhantomData,
        };
        Ok(topology::Plan::new(layout, open::Plan::new()))
    }
}

impl<I: catalogs::Catalog> settings::Resource for Layout<'_, I> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let timers = match self.settings.config.work.alarm() {
            config::Alarm::Omit => Slots::ZERO,
            config::Alarm::Reserve => Slots::fixed::<1>(),
        };
        needs.include(
            claim::Claim::new(settings::Routes::EMPTY)
                .schedule(demands::Schedule::new(Slots::ZERO, timers)),
        )
    }
}

impl<I: catalogs::Catalog> storage::Plan for Layout<'_, I> {
    type Output<'p, 'd>
        = local::State<'d, I>
    where
        Self: 'd,
        'd: 'p;
}

impl<I: catalogs::Catalog> storage::Create for Layout<'_, I> {
    type Error = runtime::Error;

    fn try_build<'p, 'd>(
        self,
        mut context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        local::State::new(&mut context, self)
    }
}
