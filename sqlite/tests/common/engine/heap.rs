use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Limits, Lookaside, Memory, OpenOptions, Parameters, Program, Progress,
    Results, Scan, Work, runtime::Runtime,
};
use std::{num::NonZero, time::Duration};

cartel_sqlite::catalog! {
pub HeapDatabase;

pub const COMMANDS: &[&str] = &["CREATE TABLE payloads (data BLOB NOT NULL)"];

statements! {
    INSERT_PAYLOAD(0, 0) = "INSERT INTO payloads(data) VALUES(randomblob(65536))",
    INSERT_SMALL(0, 0) = "INSERT INTO payloads(data) VALUES(X'00')",
    MATERIALIZE(0, 1) = "SELECT zeroblob(262143 + length(data)) FROM payloads LIMIT 1",
    LOCAL_BLOB(0, 1) = "SELECT zeroblob(262143 + changes())",
    GROW(1, 1) = "WITH RECURSIVE count(value) AS (\
        VALUES(1) UNION ALL SELECT value+1 FROM count WHERE value < ?1\
        ) SELECT length(group_concat(printf('%01024d', value))) FROM count",
    CHECK(0, 1) = "SELECT 1",
}

pub const SQLITE_BYTES: usize = 512 << 10;

pub fn try_settings() -> Result<(Limits, Memory), plan::Error> {
    Ok((
        Limits {
            parameters: Parameters {
                items: 1,
                bytes: 64,
            },
            scan: Scan {
                rows: 1,
                row_bytes: 128 << 10,
            },
            savepoints: 16,
            program: Program::new(NonZero::new(100_000).ok_or(plan::Error::Program)?),
            work: Work::uniform(Budget {
                progress: Progress::new(NonZero::new(1_000).ok_or(plan::Error::Progress)?),
                busy: Busy {
                    window: Duration::from_millis(10),
                    interval: Duration::from_millis(1),
                    retries: NonZero::<u32>::MIN.saturating_add(7),
                },
            }),
        },
        Memory {
            sqlite_bytes: SQLITE_BYTES,
            results: Results {
                rows: 1,
                bytes: 128 << 10,
            },
            statement_bytes: 64 << 10,
            cache: Cache {
                main_bytes: 1 << 10,
                temporary_bytes: 1 << 10,
            },
            lookaside: Lookaside::try_new(128, 32).ok_or(plan::Error::Lookaside)?,
        },
    ))
}

pub fn open<'sql>(runtime: Runtime) -> Result<cartel_sqlite::database::Target<'sql>, plan::Error> {
    OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)
}
}
