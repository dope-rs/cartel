use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Checkpoint, Journal, Limits, Lookaside, Memory, OpenOptions, Parameters,
    Program, Progress, Results, Scan, Synchronous, Wal, Work, runtime::Runtime,
};
use std::{num::NonZero, path::Path, time::Duration};

cartel_sqlite::catalog! {
pub WalDatabase;

pub const COMMANDS: &[&str] = &[
            "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)",
            "INSERT INTO items (id) VALUES (0)",
        ];

statements! {
        INSERT_ITEM(1, 0) = "INSERT INTO items (id) VALUES (?1)",
        COUNT_ITEMS(0, 1) = "SELECT count(*) FROM items",
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
                    window: Duration::from_secs(2),
                    interval: Duration::from_millis(1),
                    retries: NonZero::<u32>::MIN.saturating_add(4_095),
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

pub fn database<'sql>(runtime: Runtime, path: &Path) -> Result<cartel_sqlite::database::Target<'sql>, plan::Error> {
        let mut wal = Wal::new();
        wal.checkpoint(Checkpoint::Manual)
            .retain(Some(1 << 20));
        let mut options = OpenOptions::new();
        options
            .journal(Journal::Wal(wal))
            .synchronous(Synchronous::Normal)
            .mapped_bytes(1 << 20);
        options.open(runtime, path).map_err(plan::Error::Client)
    }
}
