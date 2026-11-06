use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Catalog as _, OpenOptions, Shape};
use common::{assert_reclaimed, assert_retirement, engine::layout::*};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::{mem, ops};

pub mod common;

#[test]
fn catalog_marker_identity() {
    use common::database::{A, AB};

    assert_eq!(
        [A::STATEMENTS[0].sql(), AB::STATEMENTS[0].sql()],
        ["SELECT 1", "SELECT 2"]
    );
}

#[test]
fn catalog_shape() {
    let shape = Shape::of::<LayoutDatabase>();
    assert_eq!(shape.statements, 4);
    assert_eq!(
        shape.max_sql_bytes,
        "INSERT INTO items (id) VALUES (?1)".len()
    );
    let reserved = (try_memory()
        .expect("SQLite memory")
        .reservation::<LayoutDatabase>())
    .expect("catalog reserved overflowed");
    assert!(reserved.statements.catalog == shape);
    assert_eq!(reserved.statements.items, 10);
    assert_eq!(reserved.statements.sql_bytes, shape.max_sql_bytes);
}

#[test]
fn bounded_fiber_state() -> Result<(), plan::Error> {
    // SAFETY: only this test calls SQLite in this binary. Only Cartel uses the
    // installed runtime, retaining Core handles and allocations on their worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let database = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let result = plan::Plan::<LayoutDatabase>::try_new(database, try_limits()?, try_memory()?)?
        .try_run(
            |driver, mut database| {
                let command = database
                    .query()
                    .run_no_rows(LayoutDatabase::INSERT_ITEM, (2_i64, ()));
                let cmd_size = mem::size_of_val(&command);
                assert!(cmd_size <= COMMAND_WORDS * mem::size_of::<usize>());
                fibers::TEST
                    .try_run(driver, command)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let plain = database
                    .query()
                    .run_one::<i64, _, _>(LayoutDatabase::SELECT_ITEM, ());
                let row_size = mem::size_of_val(&plain);
                assert!(row_size <= PLAIN_WORDS * mem::size_of::<usize>());
                assert_eq!(
                    fibers::TEST
                        .try_run(driver, plain)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?,
                    1
                );

                let first = database
                    .query()
                    .run_first::<i64, _, _>(LayoutDatabase::SELECT_ITEM, ());
                let one_size = mem::size_of_val(&first);
                assert!(one_size <= PLAIN_WORDS * mem::size_of::<usize>());
                assert_eq!(
                    fibers::TEST
                        .try_run(driver, first)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?,
                    Some(1)
                );

                let borrowed = database
                    .query()
                    .run_one::<String, _, _>(LayoutDatabase::SELECT_TEXT, ());
                let ref_size = mem::size_of_val(&borrowed);
                assert!(ref_size <= ARENA_WORDS * mem::size_of::<usize>());
                assert_eq!(
                    fibers::TEST
                        .try_run(driver, borrowed)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?,
                    "value"
                );

                let pair = database
                    .query()
                    .run_one::<(i64, String), _, _>(LayoutDatabase::SELECT_PAIR, ());
                let two_size = mem::size_of_val(&pair);
                assert!(two_size <= ARENA_WORDS * mem::size_of::<usize>());
                assert_eq!(
                    fibers::TEST
                        .try_run(driver, pair)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?,
                    (7, "value")
                );

                let rows = database
                    .query()
                    .run_all::<i64, _, _>(LayoutDatabase::SELECT_ITEM, ());
                let all_size = mem::size_of_val(&rows);
                assert!(all_size <= COLLECT_WORDS * mem::size_of::<usize>());
                assert_eq!(
                    fibers::TEST
                        .try_run(driver, rows)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?
                        .as_slice(),
                    &[1]
                );

                let views = database
                    .query()
                    .run_all::<String, _, _>(LayoutDatabase::SELECT_TEXT, ());
                let view_len = mem::size_of_val(&views);
                assert!(view_len <= COLLECT_WORDS * mem::size_of::<usize>());
                assert_eq!(
                    fibers::TEST
                        .try_run(driver, views)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?
                        .as_slice(),
                    &["value"]
                );

                let scan = database
                    .query()
                    .stream::<i64, _, _>(LayoutDatabase::SELECT_ITEM, ())
                    .visit(|_| ops::ControlFlow::Continue(()));
                let scan_len = mem::size_of_val(&scan);
                assert!(scan_len <= SCAN_WORDS * mem::size_of::<usize>());
                fibers::TEST
                    .try_run(driver, scan)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let tx = database.transaction();
                let tx_size = mem::size_of_val(&tx);
                assert!(tx_size <= BEGIN_WORDS * mem::size_of::<usize>());
                let mut tx = fibers::TEST
                    .try_run(driver, tx)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert!(mem::size_of_val(&tx) <= CAPABILITY_WORDS * mem::size_of::<usize>());

                let save = tx.savepoint();
                assert!(mem::size_of_val(&save) <= TRANSITION_WORDS * mem::size_of::<usize>());
                let mut save = fibers::TEST
                    .try_run(driver, save)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert!(mem::size_of_val(&save) <= SAVEPOINT_WORDS * mem::size_of::<usize>());

                let nested = save.savepoint();
                assert!(mem::size_of_val(&nested) <= TRANSITION_WORDS * mem::size_of::<usize>());
                let nested = fibers::TEST
                    .try_run(driver, nested)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let release = nested.commit();
                assert!(
                    mem::size_of_val(&release) <= TRANSITION_WORDS * mem::size_of::<usize>(),
                    "savepoint commit: {} bytes, limit {}",
                    mem::size_of_val(&release),
                    TRANSITION_WORDS * mem::size_of::<usize>(),
                );
                fibers::TEST
                    .try_run(driver, release)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let rollback = save.rollback();
                assert!(
                    mem::size_of_val(&rollback) <= TRANSITION_WORDS * mem::size_of::<usize>(),
                    "savepoint rollback: {} bytes, limit {}",
                    mem::size_of_val(&rollback),
                    TRANSITION_WORDS * mem::size_of::<usize>(),
                );
                fibers::TEST
                    .try_run(driver, rollback)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let release = save.commit();
                assert!(
                    mem::size_of_val(&release) <= TRANSITION_WORDS * mem::size_of::<usize>(),
                    "savepoint commit: {} bytes, limit {}",
                    mem::size_of_val(&release),
                    TRANSITION_WORDS * mem::size_of::<usize>(),
                );
                fibers::TEST
                    .try_run(driver, release)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let rollback = tx.rollback();
                assert!(mem::size_of_val(&rollback) <= TRANSITION_WORDS * mem::size_of::<usize>());
                fibers::TEST
                    .try_run(driver, rollback)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                let tx = fibers::TEST
                    .try_run(driver, database.transaction())
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                let commit = tx.commit();
                assert!(mem::size_of_val(&commit) <= TRANSITION_WORDS * mem::size_of::<usize>());
                fibers::TEST
                    .try_run(driver, commit)
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;

                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
    result.expect("SQLite layout scope failed")
}
