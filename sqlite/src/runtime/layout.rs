use crate::{
    catalogs, config, database,
    runtime::{self, heap, local},
};
use dope_core::driver::settings::{claim, errors};
use dope_core::driver::{
    settings::{self, demands, demands::slots::Slots},
    storage,
};
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

impl runtime::Runtime {
    pub(crate) fn layout<'sql, I: catalogs::Catalog>(
        &self,
        config: config::Config,
        memory: crate::Memory,
        database: database::Target<'sql>,
    ) -> Result<Layout<'sql, I>, runtime::Error> {
        let (plan, sqlite) = memory.admit::<I>()?;
        if plan.targets.retained > sqlite.capacity() {
            return Err(runtime::Error::SqliteCapacity);
        }
        let values = sqlite.capacity();
        if values > i32::MAX as usize {
            return Err(runtime::Error::SqliteMemory { actual: values });
        }
        let actual = plan.statements.sql_bytes.max(database.sql_bytes());
        if actual > i32::MAX as usize {
            return Err(runtime::Error::StatementSqlBytes { actual });
        }
        let sql = actual as i32;
        Ok(Layout {
            settings: local::Settings {
                config,
                value_bytes: values as i32,
                statement_sql_bytes: sql,
            },
            sqlite,
            database,
            statement_bytes: plan.statements.slots_bytes,
            lookaside: memory.lookaside,
            marker: marker::PhantomData,
        })
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
        let Self {
            settings,
            sqlite,
            database,
            statement_bytes: stmts,
            lookaside: aside,
            marker: marker::PhantomData,
        } = self;
        local::State::new(&mut context, settings, sqlite, database, stmts, aside)
    }
}
