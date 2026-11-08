use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, OpenOptions, Work};
use common::engine::contend::*;
use dope_core::driver::life::recovery::Policy;
use dope_core::driver::settings::demands::slots::Slots;
use dope_fiber::abi;
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use std::{env, pin::Pin, time::Duration};

pub mod common;

#[test]
fn contention() -> Result<(), plan::Error> {
    // SAFETY: this binary's only test initializes SQLite before any other call.
    // Raw peers stay inside their parent Cartel scope; Core handles and
    // allocations remain exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    timer_matches_delay(runtime)?;
    busy_failure_is_causal(runtime)?;
    zero_delay_yields(runtime)
}

fn timer_matches_delay(runtime: Runtime) -> Result<(), plan::Error> {
    let (mut limits, memory) = try_settings()?;
    let mut budget = limits.work.call;
    budget.busy.interval = Duration::ZERO;
    limits.work = Work::uniform(budget);
    let db = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?;
    let ready = plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Slots::ZERO)?;
    assert!(ready.layout().schedule().timers() == Slots::ZERO);

    let (mut limits, memory) = try_settings()?;
    limits.work.open.busy.interval = Duration::ZERO;
    let db = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?;
    let ready = plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Slots::ZERO)?;
    assert!(ready.layout().schedule().timers() == Slots::fixed::<1>());
    Ok(())
}

fn busy_failure_is_causal(runtime: Runtime) -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let dir = Directory::try_with(&base, "cartel-sqlite-contention-cause", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("database.db");
    let db = OpenOptions::new()
        .open(runtime, &path)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let (limits, memory) = try_settings()?;
    let result =
        plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Slots::fixed::<2>())?
            .try_run(
                |driver, mut db| {
                    let mut blocker = Blocker::try_new(&db, &path).map_err(plan::Error::Client)?;
                    blocker.commit().map_err(plan::Error::Call)?;
                    blocker.read().map_err(plan::Error::Call)?;
                    let mut rows = 0;
                    let query = db.query().stream::<i64, _, _>(
                        ContentionDatabase::INSERT_RETURNING, (2_i64, ()),
                    ).visit(|_| {
                        rows += 1;
                        std::ops::ControlFlow::Continue(())
                    });
                    let result = fibers::TEST.try_run(driver, query);
                    assert_eq!(rows, 1);
                    assert!(matches!(result, Ok(Err(Error::Busy { action: Action::Step, code })) if code.primary() == 5));
                    blocker.commit().map_err(plan::Error::Call)?;
                    fibers::TEST
                        .try_run(driver, db.query().run_no_rows(ContentionDatabase::INSERT_ITEM, (2_i64, ())))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;

                    blocker.lock_again().map_err(plan::Error::Call)?;
                    let mut tx = fibers::TEST
                        .try_run(driver, db.transaction())
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    let result = fibers::TEST.try_run(
                        driver,
                        tx.query()
                            .run_no_rows(ContentionDatabase::INSERT_ITEM, (4_i64, ())),
                    );
                    assert!(matches!(
                        result,
                        Ok(Err(Error::TransactionBusy { action: Action::Step, code }))
                            if code.primary() == 5
                    ));
                    let result = fibers::TEST.try_run(
                        driver,
                        tx.query()
                            .run_one::<i64, _, _>(ContentionDatabase::COUNT_ITEMS, ()),
                    );
                    assert!(matches!(
                        result,
                        Ok(Err(Error::TransactionBusy { action: Action::Step, code }))
                            if code.primary() == 5
                    ));
                    fibers::TEST
                        .try_run(driver, tx.rollback())
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    blocker.commit().map_err(plan::Error::Call)?;
                    drop(blocker);
                    let count = fibers::TEST
                        .try_run(
                            driver,
                            db.query()
                                .run_one::<i64, _, _>(ContentionDatabase::COUNT_ITEMS, ()),
                        )
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert_eq!(count, 3);
                    Ok(())
                },
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            );
    result.expect("contending SQLite scope failed")
}

fn zero_delay_yields(runtime: Runtime) -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let dir = Directory::try_with(&base, "cartel-sqlite-contention-yield", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("database.db");
    let db = OpenOptions::new()
        .open(runtime, &path)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let (mut limits, memory) = try_settings()?;
    let mut budget = limits.work.call;
    budget.busy.interval = Duration::ZERO;
    limits.work = Work::uniform(budget);
    let result =
        plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Slots::fixed::<1>())?
            .try_run(
                |driver, mut db| {
                    fibers::TEST
                        .try_run(
                            driver,
                            db.query()
                                .run_no_rows(ContentionDatabase::INSERT_ITEM, (0_i64, ())),
                        )
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    let mut blocker = Blocker::try_new(&db, &path).map_err(plan::Error::Client)?;
                    let mut task = abi::Task::new(
                        db.query()
                            .run_no_rows(ContentionDatabase::INSERT_ITEM, (99_i64, ())),
                    );
                    assert!(
                        fibers::TEST
                            .try_run(driver, fibers::TEST.poll_once(Pin::new(&mut task)))
                            .map_err(plan::Error::Run)?
                            .is_none()
                    );
                    drop(task);

                    let mut query = abi::Task::new(
                        db.query()
                            .run_no_rows(ContentionDatabase::INSERT_ITEM, (2_i64, ())),
                    );
                    assert!(
                        fibers::TEST
                            .try_run(driver, fibers::TEST.poll_once(Pin::new(&mut query)))
                            .map_err(plan::Error::Run)?
                            .is_none()
                    );
                    blocker.commit().map_err(plan::Error::Call)?;
                    fibers::TEST
                        .try_run(driver, query)
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    drop(blocker);
                    let count = fibers::TEST
                        .try_run(
                            driver,
                            db.query()
                                .run_one::<i64, _, _>(ContentionDatabase::COUNT_ITEMS, ()),
                        )
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert_eq!(count, 3);
                    Ok(())
                },
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            );
    result.expect("contending SQLite scope failed")
}
