use crate::common::database::COMMANDS;
use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Limits, Lookaside, Memory, OpenOptions, Parameters, Program, Progress,
    Results, Scan, Table, Work, database, runtime::Runtime,
};
use std::{num::NonZero, time::Duration};

mod keyword;

pub use keyword::Keyword;

pub fn try_settings() -> Result<(Limits, Memory), plan::Error> {
    Ok((
        Limits {
            parameters: Parameters {
                items: 64,
                bytes: 1 << 20,
            },
            scan: Scan {
                rows: 1_024,
                row_bytes: 1 << 20,
            },
            savepoints: 2,
            program: Program::new(NonZero::new(1_000_000).ok_or(plan::Error::Program)?),
            work: Work::uniform(Budget {
                progress: Progress::new(NonZero::new(10_000).ok_or(plan::Error::Progress)?),
                busy: Busy {
                    window: Duration::from_secs(1),
                    interval: Duration::from_millis(1),
                    retries: NonZero::<u32>::MIN.saturating_add(1_023),
                },
            }),
        },
        Memory {
            sqlite_bytes: 32 << 20,
            results: Results {
                rows: 1_024,
                bytes: 16 << 20,
            },
            statement_bytes: 2 << 20,
            cache: Cache {
                main_bytes: 8 << 20,
                temporary_bytes: 8 << 20,
            },
            lookaside: Lookaside::try_new(128, 256).ok_or(plan::Error::Lookaside)?,
        },
    ))
}

#[derive(Table, PartialEq)]
#[table_name("users")]
pub struct User {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) age: i32,
    pub(crate) score: f64,
    pub(crate) active: bool,
    pub(crate) nickname: Option<String>,
    pub(crate) avatar: Option<Vec<u8>>,
}

#[derive(Table, PartialEq)]
#[table_name("posts")]
pub struct Post {
    pub(crate) id: i64,
    pub(crate) author_id: i64,
    pub(crate) title: String,
    pub(crate) body: String,
    pub(crate) likes: i64,
}

#[derive(Table, PartialEq)]
#[table_name("comments")]
pub struct Comment {
    pub(crate) id: i64,
    pub(crate) post_id: i64,
    pub(crate) author_id: i64,
    pub(crate) text: String,
}

#[derive(Table, PartialEq)]
#[table_name("tags")]
pub struct Tag {
    pub(crate) id: i64,
    pub(crate) post_id: i64,
    pub(crate) label: String,
}

#[derive(Table, PartialEq)]
pub struct Widget {
    pub(crate) id: i64,
    pub(crate) bucket: i32,
    pub(crate) value: i64,
}

#[derive(Table, PartialEq)]
pub struct Item {
    pub(crate) a: i32,
    pub(crate) b: i32,
    pub(crate) payload: String,
}

#[derive(Table)]
#[table_name("q1_0")]
pub struct Correlated {
    pub(crate) id: i64,
    pub(crate) value: i64,
}

pub fn open<'sql>(runtime: Runtime) -> Result<database::Target<'sql>, plan::Error> {
    OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)
}
