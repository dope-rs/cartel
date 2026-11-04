use crate::{catalogs, database, options, retry, runtime, sys};
use retry::lease;
use runtime::{local, slot};
use std::pin;

pub(super) struct Opening<'local, 'd: 'local, I: catalogs::Catalog> {
    resources: slot::Opening<'local, 'd, I>,
    value_bytes: i32,
    statement_sql_bytes: i32,
    storage: database::Kind,
    options: options::OpenOptions,
    commands: sys::Commands<'d>,
    phase: Phase,
}

enum Phase {
    Configure(Configure),
    Settings(Settings),
    Verify(Verify),
    Commands,
}

#[derive(Clone, Copy)]
enum Configure {
    Limits,
    TemporaryStore,
    MainCache,
    TemporaryCache,
    MainSpill,
    TemporarySpill,
}

#[derive(Clone, Copy)]
enum Settings {
    Journal,
    JournalLimit,
    Mapped,
    Synchronous,
    Checkpoint,
    JournalCheck,
}

#[derive(Clone, Copy)]
enum Verify {
    Mapped,
    TemporaryStore,
    MainCache,
    TemporaryCache,
}

pub(super) enum Progress {
    Pending,
    Ready,
}

impl<'local, 'd: 'local, I: catalogs::Catalog> Opening<'local, 'd, I> {
    pub(super) fn new(
        local: pin::Pin<&'local local::State<'d, I>>,
        report: &'local mut crate::open::Completion,
    ) -> Result<(Self, crate::conn::raw::Vacant<'local, 'd, I>), crate::open::Error> {
        let (database, lease, vacant) = local.try_lease(report)?;
        let (location, options, commands) = database.into_parts();
        let flags = match &location {
            database::Location::File(_) => options.flags(),
            database::Location::Memory => 0,
        };
        let storage = location.kind();
        let db = match &location {
            database::Location::File(path) => sys::Connection::open(&path.0, flags)?,
            database::Location::Memory => sys::Connection::open(
                c":memory:",
                libsqlite3_sys::SQLITE_OPEN_READWRITE | libsqlite3_sys::SQLITE_OPEN_CREATE,
            )?,
        };
        let opened = lease.open(db)?;
        Ok((
            Self {
                resources: opened,
                value_bytes: local.value_bytes(),
                statement_sql_bytes: local.sql_bytes(),
                storage,
                options,
                commands,
                phase: Phase::Configure(Configure::Limits),
            },
            vacant,
        ))
    }

    pub(super) fn retry(&mut self) -> lease::Lease<'_, 'd, lease::Open> {
        self.resources.retry()
    }

    pub(super) fn advance(&mut self) -> Result<Progress, crate::open::Error> {
        match self.phase {
            Phase::Configure(step) => return Ok(self.configure(step)?),
            Phase::Settings(step) => return Ok(self.settings(step)?),
            Phase::Verify(step) => return Ok(self.verify(step)?),
            Phase::Commands => {}
        }
        let Some((sql, tail)) = self.commands.split_first() else {
            return Ok(Progress::Ready);
        };
        self.resources.call().execute(sql)?;
        self.commands = tail;
        Ok(Progress::Pending)
    }

    pub(super) fn prepare(self) -> slot::Prepare<'local, 'd, I> {
        self.resources.prepare()
    }

    fn configure(&mut self, step: Configure) -> Result<Progress, crate::Error> {
        let call = self.resources.call();
        self.phase = Phase::Configure(match step {
            Configure::Limits => {
                self.limits()?;
                Configure::TemporaryStore
            }
            Configure::TemporaryStore => {
                call.exec("PRAGMA temp_store=FILE")?;
                Configure::MainCache
            }
            Configure::MainCache => {
                call.pragma("cache_size", i64::from(-self.resources.config().main_kib()))?;
                Configure::TemporaryCache
            }
            Configure::TemporaryCache => {
                call.pragma(
                    "temp.cache_size",
                    i64::from(-self.resources.config().temp_kib()),
                )?;
                Configure::MainSpill
            }
            Configure::MainSpill => {
                call.exec("PRAGMA cache_spill=ON")?;
                Configure::TemporarySpill
            }
            Configure::TemporarySpill => {
                call.exec("PRAGMA temp.cache_spill=ON")?;
                self.phase = Phase::Settings(Settings::Journal);
                return Ok(Progress::Pending);
            }
        });
        Ok(Progress::Pending)
    }

    fn settings(&mut self, step: Settings) -> Result<Progress, crate::Error> {
        let call = self.resources.call();
        self.phase = Phase::Settings(match step {
            Settings::Journal => {
                match self.storage {
                    database::Kind::File => self.options.set_journal(&call)?,
                    database::Kind::Memory => {}
                }
                Settings::JournalLimit
            }
            Settings::JournalLimit => {
                match self.storage {
                    database::Kind::File => self.options.retain(&call)?,
                    database::Kind::Memory => {}
                }
                Settings::Mapped
            }
            Settings::Mapped => {
                self.options.set_mapped(&call, self.storage)?;
                Settings::Synchronous
            }
            Settings::Synchronous => {
                self.options.sync(&call)?;
                Settings::Checkpoint
            }
            Settings::Checkpoint => {
                self.options.checkpoint(&call)?;
                Settings::JournalCheck
            }
            Settings::JournalCheck => {
                self.options.verify(&call)?;
                self.phase = Phase::Verify(Verify::Mapped);
                return Ok(Progress::Pending);
            }
        });
        Ok(Progress::Pending)
    }

    fn verify(&mut self, step: Verify) -> Result<Progress, crate::Error> {
        let call = self.resources.call();
        match step {
            Verify::Mapped => {
                let found = match self.storage {
                    database::Kind::File => call.query_i64("PRAGMA mmap_size")?,
                    database::Kind::Memory => 0,
                };
                let expected = self.options.mapped(self.storage)?;
                if found != expected {
                    return Err(crate::Error::Mapped {
                        expected,
                        actual: found,
                    });
                }
                self.phase = Phase::Verify(Verify::TemporaryStore);
            }
            Verify::TemporaryStore => {
                let actual = call.query_i64("PRAGMA temp_store")?;
                if actual != 1 {
                    return Err(crate::Error::TemporaryStore { actual });
                }
                self.phase = Phase::Verify(Verify::MainCache);
            }
            Verify::MainCache => {
                let expected = i64::from(-self.resources.config().main_kib());
                let actual = call.query_i64("PRAGMA cache_size")?;
                if actual != expected {
                    return Err(crate::Error::MainCache { expected, actual });
                }
                self.phase = Phase::Verify(Verify::TemporaryCache);
            }
            Verify::TemporaryCache => {
                let expected = i64::from(-self.resources.config().temp_kib());
                let actual = call.query_i64("PRAGMA temp.cache_size")?;
                if actual != expected {
                    return Err(crate::Error::TemporaryCache { expected, actual });
                }
                self.resources.authorize();
                self.phase = Phase::Commands;
            }
        }
        Ok(Progress::Pending)
    }

    fn limits(&mut self) -> Result<(), crate::Error> {
        self.resources.install()?;
        let max = self
            .resources
            .limit(libsqlite3_sys::SQLITE_LIMIT_SQL_LENGTH, -1);
        if self.statement_sql_bytes > max {
            return Err(crate::Error::StatementSqlLimit {
                requested: self.statement_sql_bytes,
                available: max,
            });
        }
        self.resources
            .limit(libsqlite3_sys::SQLITE_LIMIT_LENGTH, self.value_bytes);
        self.resources
            .limit(libsqlite3_sys::SQLITE_LIMIT_ATTACHED, 0);
        self.resources
            .limit(libsqlite3_sys::SQLITE_LIMIT_WORKER_THREADS, 0);
        self.resources.limit(
            libsqlite3_sys::SQLITE_LIMIT_VDBE_OP,
            self.resources.config().program_ops(),
        );
        Ok(())
    }
}
