use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Limits, Lookaside, Memory, OpenOptions, Parameters, Program, Progress,
    Results, Scan, Work, database, runtime::Runtime,
};
use std::{num::NonZero, path, time};

pub const RELATIONS_COMMANDS: &[&str] = &[
    "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
    "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent INTEGER NOT NULL REFERENCES parents(id)
            )",
];

pub const BROKEN_COMMANDS: &[&str] = &[
    "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
    "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent INTEGER NOT NULL REFERENCES parents(id)
                    DEFERRABLE INITIALLY DEFERRED
            )",
    "INSERT INTO children (id, parent) VALUES (1, 7)",
];

cartel_sqlite::catalog! {
    pub Relations;

    statements! {
        INSERT_PARENT(1, 0) = "INSERT INTO parents (id) VALUES (?1)",
        INSERT_CHILD(2, 0) = "INSERT INTO children (id, parent) VALUES (?1, ?2)",
        COUNT_CHILDREN(0, 1) = "SELECT count(*) FROM children",
    }
}

pub fn try_limits() -> Result<Limits, plan::Error> {
    Ok(Limits {
        parameters: Parameters {
            items: 2,
            bytes: 64,
        },
        scan: Scan {
            rows: 2,
            row_bytes: 128,
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
            rows: 2,
            bytes: 128,
        },
        statement_bytes: 64 << 10,
        cache: Cache {
            main_bytes: 1 << 20,
            temporary_bytes: 1 << 20,
        },
        lookaside: Lookaside::try_new(128, 32).ok_or(plan::Error::Lookaside)?,
    })
}

pub fn file_database<'sql>(
    runtime: Runtime,
    path: &path::Path,
) -> Result<database::Target<'sql>, plan::Error> {
    OpenOptions::new()
        .open(runtime, path)
        .map_err(plan::Error::Client)
}
