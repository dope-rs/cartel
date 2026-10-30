use crate::database::{self, Operation};
use o3::base::num::bounded;
use std::{mem, num, time};

#[derive(PartialEq, Eq)]
pub struct Config {
    query: Query,
    sqlite: Sqlite,
    pub(crate) work: Work,
    program_ops: num::NonZero<i32>,
}

#[derive(PartialEq, Eq)]
struct Query {
    params: num::NonZero<i32>,
    param_bytes: num::NonZero<usize>,
    scan_rows: num::NonZero<usize>,
    row_bytes: num::NonZero<i32>,
    result_rows: num::NonZero<usize>,
    result_bytes: usize,
}

#[derive(PartialEq, Eq)]
struct Sqlite {
    stmt_bytes: Capacity,
    main_kib: num::NonZero<i32>,
    temp_kib: num::NonZero<i32>,
}

enum Pager {
    Main,
    Temporary,
}

pub(crate) type Capacity = bounded::NonZeroUsize<1, { isize::MAX as usize }>;

pub(crate) const CONTROL_STATEMENT_ITEMS: usize = 6;
pub(crate) const MAX_SAVEPOINTS: usize = usize::MAX >> 2;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Work {
    pub open: Budget,
    pub call: Budget,
    pub cleanup: Budget,
    pub savepoints: bounded::NonZeroUsize<1, MAX_SAVEPOINTS>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Budget {
    pub(crate) progress: crate::limits::Progress,
    pub(crate) contention: Contention,
}

impl Budget {
    pub(crate) const PROGRESS_INTERVAL: i32 = 1_000;
}

/// Scheduler bounds for SQLite lock contention without OS-thread sleeps.
/// The fiber retains its statement until progress or its retry/time ceiling.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Contention {
    pub(crate) window: time::Duration,
    delay: Delay,
    pub(crate) retries: num::NonZero<u32>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Delay {
    Yield,
    Sleep(time::Duration),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Alarm {
    Omit,
    Reserve,
}

impl Delay {
    const fn new(interval: time::Duration) -> Self {
        if interval.is_zero() {
            Self::Yield
        } else {
            Self::Sleep(interval)
        }
    }
}

impl Contention {
    pub(crate) const fn delay(self) -> Delay {
        self.delay
    }
}

impl Work {
    pub(crate) const fn alarm(&self) -> Alarm {
        match (
            self.open.contention.delay,
            self.call.contention.delay,
            self.cleanup.contention.delay,
        ) {
            (Delay::Yield, Delay::Yield, Delay::Yield) => Alarm::Omit,
            _ => Alarm::Reserve,
        }
    }
}

impl Config {
    pub(crate) fn from_parts(
        limits: crate::limits::Limits,
        memory: crate::Memory,
    ) -> Result<Self, database::Error> {
        let items = limits.parameters.items;
        if items > i32::MAX as usize {
            return Err(database::Error::ParameterItems { actual: items });
        }
        let params = match num::NonZero::<i32>::new(items as i32) {
            Some(params) => params,
            None => return Err(database::Error::ParameterItemsZero),
        };
        let Some(pbytes) = num::NonZero::new(limits.parameters.bytes) else {
            return Err(database::Error::ParameterBytesZero);
        };
        let Some(scan) = num::NonZero::new(limits.scan.rows) else {
            return Err(database::Error::ScanRowsZero);
        };
        let rowbytes = limits.scan.row_bytes;
        if rowbytes > i32::MAX as usize {
            return Err(database::Error::RowBytes { actual: rowbytes });
        }
        let row = match num::NonZero::<i32>::new(rowbytes as i32) {
            Some(row) => row,
            None => return Err(database::Error::RowBytesZero),
        };
        let results = memory.results.rows;
        let results = match num::NonZero::new(results) {
            Some(results) if results.get() <= scan.get() => results,
            _ => {
                return Err(database::Error::CollectRows {
                    actual: results,
                    maximum: scan.get(),
                });
            }
        };
        let bytes = memory.results.bytes;
        if bytes < rowbytes {
            return Err(database::Error::CollectBytes {
                actual: bytes,
                required: rowbytes,
            });
        }
        let stmts =
            Capacity::new(memory.statement_bytes).ok_or(database::Error::StatementBytes {
                actual: memory.statement_bytes,
                maximum: isize::MAX as usize,
            })?;
        let main_kib = Self::pager_kib(memory.cache.main_bytes, Pager::Main)?;
        let temp_kib = Self::pager_kib(memory.cache.temporary_bytes, Pager::Temporary)?;
        let Some(depth) = bounded::NonZeroUsize::new(limits.savepoints) else {
            return Err(database::Error::Savepoints {
                actual: limits.savepoints,
                maximum: MAX_SAVEPOINTS,
            });
        };
        Ok(Self {
            query: Query {
                params,
                param_bytes: pbytes,
                scan_rows: scan,
                row_bytes: row,
                result_rows: results,
                result_bytes: bytes,
            },
            sqlite: Sqlite {
                stmt_bytes: stmts,
                main_kib,
                temp_kib,
            },
            work: Work {
                open: budget(limits.work.open, Operation::Open)?,
                call: budget(limits.work.call, Operation::Call)?,
                cleanup: budget(limits.work.cleanup, Operation::Cleanup)?,
                savepoints: depth,
            },
            program_ops: limits.program.sqlite_ops(),
        })
    }

    pub const fn params(&self) -> usize {
        self.query.params.get() as usize
    }

    pub(crate) const fn param_limit(&self) -> i32 {
        self.query.params.get()
    }

    pub const fn param_bytes(&self) -> usize {
        self.query.param_bytes.get()
    }

    pub const fn scan_rows(&self) -> usize {
        self.query.scan_rows.get()
    }

    pub const fn row_bytes(&self) -> num::NonZero<i32> {
        self.query.row_bytes
    }

    pub const fn result_rows(&self) -> usize {
        self.query.result_rows.get()
    }

    pub const fn result_bytes(&self) -> usize {
        self.query.result_bytes
    }

    pub(crate) const fn stmt_bytes(&self) -> Capacity {
        self.sqlite.stmt_bytes
    }

    pub(crate) const fn main_kib(&self) -> i32 {
        self.sqlite.main_kib.get()
    }

    pub(crate) const fn temp_kib(&self) -> i32 {
        self.sqlite.temp_kib.get()
    }

    pub(crate) const fn program_ops(&self) -> i32 {
        self.program_ops.get()
    }

    fn pager_kib(bytes: usize, pager: Pager) -> Result<num::NonZero<i32>, database::Error> {
        let kib = bytes / 1024;
        if kib > i32::MAX as usize {
            return Err(match pager {
                Pager::Main => database::Error::MainCacheInteger { actual: bytes },
                Pager::Temporary => database::Error::TemporaryCacheInteger { actual: bytes },
            });
        }
        let Some(kib) = num::NonZero::<i32>::new(kib as i32) else {
            return Err(match pager {
                Pager::Main => database::Error::MainCacheBytes { actual: bytes },
                Pager::Temporary => database::Error::TemporaryCacheBytes { actual: bytes },
            });
        };
        Ok(kib)
    }
}

fn budget(source: crate::limits::Budget, op: Operation) -> Result<Budget, database::Error> {
    if source.busy.interval > source.busy.window {
        return Err(database::Error::Contention {
            operation: op,
            interval: source.busy.interval,
            window: source.busy.window,
        });
    }
    Ok(Budget {
        progress: source.progress,
        contention: Contention {
            window: source.busy.window,
            delay: Delay::new(source.busy.interval),
            retries: source.busy.retries,
        },
    })
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Capacity>() == word);
    assert!(mem::size_of::<num::NonZero<i32>>() == mem::size_of::<i32>());
    assert!(mem::size_of::<Delay>() == 2 * word);
    assert!(mem::size_of::<Contention>() == 5 * word);
    assert!(mem::size_of::<Budget>() == 6 * word);
    assert!(mem::size_of::<Work>() == 19 * word);
    assert!(mem::size_of::<Config>() == 27 * word);
};
