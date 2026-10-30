use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Limits, Lookaside, Memory, Parameters, Program, Progress, Results, Scan,
    Work,
};
use std::{num::NonZero, time::Duration};

cartel_sqlite::catalog! {
pub LayoutDatabase;

pub const COMMAND_WORDS: usize = 16;

pub const PLAIN_WORDS: usize = 16;

pub const ARENA_WORDS: usize = 20;

pub const COLLECT_WORDS: usize = 20;

pub const SCAN_WORDS: usize = 20;

pub const CAPABILITY_WORDS: usize = 3;

pub const SAVEPOINT_WORDS: usize = 4;

pub const BEGIN_WORDS: usize = 11;

pub const TRANSITION_WORDS: usize = 13;

pub const COMMANDS: &[&str] = &[
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        "INSERT INTO items (id) VALUES (1)",
    ];

statements! {
    INSERT_ITEM(1, 0) = "INSERT INTO items (id) VALUES (?1)",
    SELECT_ITEM(0, 1) = "SELECT id FROM items LIMIT 1",
    SELECT_TEXT(0, 1) = "SELECT 'value'",
    SELECT_PAIR(0, 2) = "SELECT 7, 'value'",
}

pub fn try_limits() -> Result<Limits, plan::Error> {
    Ok(Limits {
        parameters: Parameters {
            items: 1,
            bytes: 64,
        },
        scan: Scan {
            rows: 8,
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
        sqlite_bytes: 4 << 20,
        results: Results {
            rows: 8,
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
}
