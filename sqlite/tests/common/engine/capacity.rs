use crate::common::plan;
use cartel_sqlite::{
    Budget, Busy, Cache, Connection, Limits, Lookaside, Memory, OpenOptions, Parameters, Program,
    Progress, Results, Scan, Table, Work, dsl::query, runtime::Runtime,
};
use dope_runtime::{executor, executor::session};
use dope_test::fibers;
use std::{num::NonZero, time::Duration};

cartel_sqlite::catalog! {
pub CapacityDatabase;

#[derive(Table)]
#[table_name("entries")]
pub struct Entry<T = String> {
    pub(crate) id: i64,
    pub(crate) name: T,
}

#[query]
pub fn all_entries() -> Vec<Entry> {
    Entry::filter(|_entry| true).rows()
        .order_by(|entry| entry.id)
        .all()
}

#[query]
pub fn scan_entries() -> query::Stream<Entry> {
    Entry::filter(|_entry| true).rows()
        .order_by(|entry| entry.id)
        .stream()
}

#[query]
pub fn entries_named(name: String) -> Vec<Entry> {
    Entry::filter(|entry| entry.name == name).rows().all()
}

pub const COMMANDS: &[&str] = &["CREATE TABLE entries (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"];

statements! {
    INSERT_ENTRY(2, 0) = "INSERT INTO entries (id, name) VALUES (?1, ?2)",
    RECURSIVE_SUM(0, 1) = "WITH RECURSIVE count(x) AS (\
        VALUES(0) UNION ALL SELECT x + 1 FROM count WHERE x < 1000000\
        ) SELECT sum(x) FROM count",
    EXACT_ROW(0, 2) = "SELECT 7, 'abc'",
    WIDE_INTEGER(0, 1) = "SELECT 2147483648",
    ALL_COLUMNS(0, 2) = "SELECT * FROM entries",
    ADD_COLUMN(0, 0) = "ALTER TABLE entries ADD COLUMN extra INTEGER",
}

pub fn try_settings(
    params: usize,
    scan: usize,
    row_size: usize,
    rows: usize,
    bytes: usize,
    quanta: u64,
) -> Result<(Limits, Memory), plan::Error> {
    Ok((
        Limits {
            parameters: Parameters {
                items: 8,
                bytes: params,
            },
            scan: Scan {
                rows: scan,
                row_bytes: row_size,
            },
            savepoints: 16,
            program: Program::new(NonZero::new(1_000_000).ok_or(plan::Error::Program)?),
            work: Work::uniform(Budget {
                progress: Progress::new(NonZero::new(quanta).ok_or(plan::Error::Progress)?),
                busy: Busy {
                    window: Duration::from_millis(1),
                    interval: Duration::from_millis(1),
                    retries: NonZero::<u32>::MIN,
                },
            }),
        },
        Memory {
            sqlite_bytes: 4 << 20,
            results: Results {
                rows,
                bytes,
            },
            statement_bytes: 128 << 10,
            cache: Cache {
                main_bytes: 1 << 20,
                temporary_bytes: 1 << 20,
            },
            lookaside: Lookaside::try_new(128, 64).ok_or(plan::Error::Lookaside)?,
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

pub struct NativeParams;

impl cartel_sqlite::TypedQuery for NativeParams {
    const SQL: &str = "SELECT ?1";
    const PARAMETERS: usize = usize::MAX;
    const COLUMNS: usize = 1;
}

impl cartel_sqlite::Catalog for NativeParams {
    const STATEMENTS: &[cartel_sqlite::Entry<'_>] = &[cartel_sqlite::Entry::of::<Self>()];
}

impl cartel_sqlite::HasQuery<NativeParams> for NativeParams {
    const SLOT: usize = 0;
}

pub fn try_seed<'app, 'local: 'app, 'd: 'local, D>(
    database: &mut Connection<'app, 'local, 'd, CapacityDatabase>,
    names: &[String],
    driver: &mut session::App<'app, '_, 'd, D>,
) -> Result<(), plan::Error>
where
    D: executor::raw::App<'d>,
{
    for (index, name) in names.iter().enumerate() {
        fibers::TEST
            .try_run(
                driver,
                database
                    .query()
                    .run_no_rows(CapacityDatabase::INSERT_ENTRY, (index as i64, (name, ()))),
            )
            .map_err(plan::Error::Run)?
            .map_err(plan::Error::Call)?;
    }
    Ok(())
}
