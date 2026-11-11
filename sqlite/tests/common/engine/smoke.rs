use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Limits, Lookaside, Memory, OpenOptions, Parameters, Program, Progress,
    Results, Scan, Table, Work, runtime::Runtime,
};
use std::{num::NonZero, time::Duration};

cartel_sqlite::catalog! {
pub SmokeDatabase;

pub const SQLITE_BYTES: usize = 4 << 20;

pub fn try_limits() -> Result<Limits, plan::Error> {
    Ok(Limits {
        parameters: Parameters {
            items: 8,
            bytes: 4 << 10,
        },
        scan: Scan {
            rows: 16,
            row_bytes: 4 << 10,
        },
        savepoints: 16,
        program: Program::new(NonZero::new(100_000).ok_or(plan::Error::Program)?),
        work: Work::uniform(Budget {
            progress: Progress::new(NonZero::new(100).ok_or(plan::Error::Progress)?),
            busy: Busy {
                window: Duration::from_secs(1),
                interval: Duration::from_millis(1),
                retries: NonZero::<u32>::MIN.saturating_add(1_023),
            },
        }),
    })
}

pub fn try_memory() -> Result<Memory, plan::Error> {
    Ok(Memory {
        sqlite_bytes: SQLITE_BYTES,
        results: Results {
            rows: 16,
            bytes: 64 << 10,
        },
        statement_bytes: 128 << 10,
        cache: Cache {
            main_bytes: 1 << 20,
            temporary_bytes: 1 << 20,
        },
        lookaside: Lookaside::try_new(128, 128).ok_or(plan::Error::Lookaside)?,
    })
}

#[derive(Table)]
#[table_name("users")]
pub struct User {
    pub(crate) id: i64,
    pub(crate) name: String,
}

#[query]
pub fn user_by_id(id: i64) -> User {
    User::filter(|u| u.id == id).one()
}

#[query]
pub fn user_names(min_id: i64) -> Vec<String> {
    User::filter(|u| u.id >= min_id).select(|u| u.name).all()
}

#[query]
pub fn maybe_user(id: i64) -> Option<User> {
    User::filter(|u| u.id == id).first()
}

#[query]
pub fn rename(id: i64, name: String) {
    User::filter(|u| u.id == id).update(|u| u.name = name)
}

pub const COMMANDS: &[&str] = &[
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
    ];

pub fn database<'sql>(runtime: Runtime) -> Result<cartel_sqlite::database::Target<'sql>, plan::Error> {
    OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)
}
}
