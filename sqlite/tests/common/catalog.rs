use crate::common::plan;
use cartel_sqlite::{
    Access, Budget, Busy, Cache, Catalog, Entry, Limits, Lookaside, Memory, OpenOptions,
    Parameters, Program, Progress, Results, Scan, TypedQuery, Work, database, runtime::Runtime,
};
use std::{num::NonZero, path, time};

pub struct Malformed<const TAIL: bool, const UTF8: bool = false>;

impl<const TAIL: bool, const UTF8: bool> TypedQuery for Malformed<TAIL, UTF8> {
    const SQL: &str = if TAIL {
        if UTF8 {
            UNICODE_TAIL_COMMANDS[0]
        } else {
            TAIL_COMMANDS[0]
        }
    } else {
        PARAMETER_COMMANDS[0]
    };
    const PARAMETERS: usize = 0;
    const COLUMNS: usize = 0;
}

impl<const TAIL: bool, const UTF8: bool> Catalog for Malformed<TAIL, UTF8> {
    const STATEMENTS: &[Entry<'_>] = &[Entry::of::<Self>()];
}

pub struct Unauthorized<const SAVEPOINT: bool>;

impl<const SAVEPOINT: bool> TypedQuery for Unauthorized<SAVEPOINT> {
    const SQL: &str = if SAVEPOINT {
        "SAVEPOINT \"__cartel\""
    } else {
        "COMMIT"
    };
    const PARAMETERS: usize = 0;
    const COLUMNS: usize = 0;
}

impl<const SAVEPOINT: bool> Catalog for Unauthorized<SAVEPOINT> {
    const STATEMENTS: &[Entry<'_>] = &[Entry::of::<Self>()];
}

pub fn try_limits() -> Result<Limits, plan::Error> {
    Ok(Limits {
        parameters: Parameters {
            items: 1,
            bytes: 64,
        },
        scan: Scan {
            rows: 4,
            row_bytes: 4 << 10,
        },
        savepoints: 16,
        program: Program::new(NonZero::new(100_000).ok_or(plan::Error::Program)?),
        work: Work::uniform(Budget {
            progress: Progress::new(NonZero::new(100).ok_or(plan::Error::Progress)?),
            busy: Busy {
                window: time::Duration::from_secs(1),
                interval: time::Duration::from_millis(1),
                retries: NonZero::<u32>::MIN.saturating_add(1_023),
            },
        }),
    })
}

pub fn try_memory() -> Result<Memory, plan::Error> {
    Ok(Memory {
        sqlite_bytes: 4 << 20,
        results: Results {
            rows: 4,
            bytes: 4 << 10,
        },
        statement_bytes: 128 << 10,
        cache: Cache {
            main_bytes: 1 << 20,
            temporary_bytes: 1 << 20,
        },
        lookaside: Lookaside::try_new(128, 64).ok_or(plan::Error::Lookaside)?,
    })
}

pub fn open<'sql>(
    runtime: Runtime,
    path: &path::Path,
) -> Result<database::Target<'sql>, plan::Error> {
    OpenOptions::new()
        .open(runtime, path)
        .map_err(plan::Error::Client)
}

pub fn readonly<'sql>(
    runtime: Runtime,
    path: &path::Path,
) -> Result<database::Target<'sql>, plan::Error> {
    let mut options = OpenOptions::new();
    options.access(Access::Read);
    options.open(runtime, path).map_err(plan::Error::Client)
}

pub const SEED_COMMANDS: &[&str] = &[
    "CREATE TABLE seed (id INTEGER PRIMARY KEY)",
    "INSERT INTO seed (id) VALUES (1)",
];

pub const STAGED_COMMANDS: &[&str] = &[
    "CREATE TABLE staged (value INTEGER NOT NULL); \t\r\n",
    "INSERT INTO staged (value) VALUES (2)",
    "INSERT INTO staged (value) VALUES (3)",
];

pub const BROKEN_COMMANDS: &[&str] = &["CREATE TABLE items (id INTEGER PRIMARY KEY)", "NOT SQL"];

pub const TAIL_COMMANDS: &[&str] = &["SELECT 1; SELECT 2"];
pub const UNICODE_TAIL_COMMANDS: &[&str] = &["SELECT '한'; SELECT 2"];

pub const STABLE_COMMANDS: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY)",
    "INSERT INTO items (id) VALUES (1)",
];

pub const DUPLICATE_COMMANDS: &[&str] =
    &[STABLE_COMMANDS[0], STABLE_COMMANDS[1], STABLE_COMMANDS[1]];

pub const EXPENSIVE_COMMANDS: &[&str] = &[
    "CREATE TABLE bounded (id INTEGER PRIMARY KEY)",
    "WITH RECURSIVE count(x) AS (
        VALUES(0) UNION ALL SELECT x + 1 FROM count WHERE x < 1000000
    ) INSERT INTO bounded SELECT x FROM count",
];

pub const SINGLE_WORK_COMMANDS: &[&str] = &[
    "CREATE TABLE aggregate_work (id INTEGER PRIMARY KEY)",
    "WITH RECURSIVE count(x) AS (
        VALUES(0) UNION ALL SELECT x + 1 FROM count WHERE x < 1000
    ) INSERT INTO aggregate_work SELECT x FROM count",
];

pub const AGGREGATE_WORK_COMMANDS: &[&str] = &[
    SINGLE_WORK_COMMANDS[0],
    SINGLE_WORK_COMMANDS[1],
    "WITH RECURSIVE count(x) AS (
        VALUES(0) UNION ALL SELECT x + 1 FROM count WHERE x < 1000
    ) INSERT INTO aggregate_work SELECT x + 1001 FROM count",
];

pub const SHORT_WORK_COMMANDS: &[&str] =
    &["CREATE TABLE IF NOT EXISTS short_work (id INTEGER PRIMARY KEY)"; 32_768];

pub const OVERSIZED_COMMANDS: &[&str] = &[
    "CREATE TABLE local_install (payload BLOB NOT NULL)",
    "INSERT INTO local_install (payload) VALUES (zeroblob(16777216))",
];

pub const CONTENDED_COMMANDS: &[&str] = &["CREATE TABLE added (id INTEGER PRIMARY KEY)"];

pub const NUL_COMMANDS: &[&str] = &["SELECT\0 1"];
pub const EMPTY_COMMANDS: &[&str] = &["   "];
pub const PARAMETER_COMMANDS: &[&str] = &["CREATE TABLE parameter AS SELECT ?1"];
pub const ROW_COMMANDS: &[&str] = &["SELECT 1"];
pub const FORBIDDEN_COMMANDS: &[&[&str]] = &[&["BEGIN"], &["PRAGMA foreign_keys = OFF"]];

cartel_sqlite::catalog! {
    pub Commands;
}

cartel_sqlite::catalog! {
    pub Seed;

    statements! {
        COUNT(0, 1) = "SELECT count(*) FROM seed",
    }
}

cartel_sqlite::catalog! {
    pub Staged;

    statements! {
        COUNT(0, 1) = "SELECT count(*) FROM staged; \t\r\n",
        SUM(0, 1) = "SELECT sum(value) FROM staged",
        MAX(0, 1) = "SELECT max(value) FROM staged",
    }
}

cartel_sqlite::catalog! {
    pub Stable;

    statements! {
        COUNT(0, 1) = "SELECT count(*) FROM items",
        INSERT(1, 0) = "INSERT INTO items (id) VALUES (?1)",
    }
}

cartel_sqlite::catalog! {
    pub Bounded;

    statements! {
        COUNT(0, 1) = "SELECT count(*) FROM bounded",
    }
}

cartel_sqlite::catalog! {
    pub Aggregate;

    statements! {
        COUNT(0, 1) = "SELECT count(*) FROM aggregate_work",
    }
}

cartel_sqlite::catalog! {
    pub Compact;

    statements! {
        COUNT(0, 1) = "SELECT count(*) FROM local_install",
    }
}
