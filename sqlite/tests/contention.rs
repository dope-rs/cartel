use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, OpenOptions, Work};
use common::engine::contend::*;
use dope_core::driver::life::recovery::Policy;
use dope_core::driver::settings::demands::slots::Slots;
use dope_fiber::abi;
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use std::{env, sync::OnceLock};
use std::{sync::mpsc, thread, time::Duration};

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn timer_matches_delay() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;

    let (mut limits, memory) = try_settings()?;
    let mut budget = limits.work.call;
    budget.busy.interval = Duration::ZERO;
    limits.work = Work::uniform(budget);
    let db = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?;
    let ready = plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Some(Slots::ZERO))?;
    assert!(ready.layout().schedule().timers() == Slots::ZERO);

    let (mut limits, memory) = try_settings()?;
    limits.work.open.busy.interval = Duration::ZERO;
    limits.work.call.busy.interval = Duration::ZERO;
    let db = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?;
    let ready = plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Some(Slots::ZERO))?;
    assert!(ready.layout().schedule().timers() == Slots::fixed::<1>());
    Ok(())
}

#[test]
fn busy_failure_is_causal() -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let dir = Directory::try_with(&base, "cartel-sqlite-contention-cause", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("database.db");
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let db = OpenOptions::new()
        .open(runtime, &path)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let block_db = OpenOptions::new()
        .open(runtime, &path)
        .map_err(plan::Error::Client)?;
    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<ContentionDatabase>::try_with(
        db,
        limits,
        memory,
        Some(Slots::fixed::<2>()),
    )?
    .try_run(|driver, mut db| {
            let (event_tx, event_rx) = mpsc::channel();
            let (cmd_tx, cmd_rx) = mpsc::channel();
            let blocker = thread::spawn(move || {
                let result = (|| -> Result<(), plan::Error> {
                    let (limits, memory) = try_settings()?;
                    let result = plan::Plan::<ContentionDatabase>::try_new(block_db, limits, memory)?.try_run(
                            |driver, mut db| {
                                let mut tx = fibers::TEST.try_run(driver, db.transaction()).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
                                fibers::TEST
                                    .try_run(
                                        driver,
                                        tx.query().run_no_rows(
                                            ContentionDatabase::INSERT_ITEM,
                                            (1_i64, ()),
                                        ),
                                    ).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
                                event_tx.send(Event::FirstLocked).expect("first lock signal failed");
                                let command = (cmd_rx.recv_timeout(Duration::from_secs(5))).expect("first lock release wait failed");
                                assert!(command == Command::ReleaseFirst);
                                fibers::TEST.try_run(driver, tx.commit()).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
                                event_tx.send(Event::FirstCommitted).expect("first commit signal failed");

                                let command = (cmd_rx.recv_timeout(Duration::from_secs(5))).expect("second stage wait failed");
                                assert!(command == Command::StartSecond);
                                let mut tx = fibers::TEST.try_run(driver, db.transaction()).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
                                fibers::TEST
                                    .try_run(
                                        driver,
                                        tx.query().run_no_rows(
                                            ContentionDatabase::INSERT_ITEM,
                                            (3_i64, ()),
                                        ),
                                    ).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
                                event_tx.send(Event::SecondLocked).expect("second lock signal failed");
                                let command = (cmd_rx.recv_timeout(Duration::from_secs(5))).expect("second lock release wait failed");
                                assert!(command == Command::ReleaseSecond);
                                fibers::TEST.try_run(driver, tx.commit()).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
                                Ok(())
                            },
                            Policy::new(assert_reclaimed, assert_retirement),
                            |result| result.expect("SQLite scope"),
                        );
                    result.expect("blocking SQLite scope failed")
                })();
                assert!(result.is_ok(), "blocking SQLite worker failed");
            });

            let event = (event_rx.recv_timeout(Duration::from_secs(5))).expect("first lock event wait failed");
            assert!(event == Event::FirstLocked);
            let release = {
                let cmd_tx = &cmd_tx;
                dope_fiber::fiber!('_ => async move {
                    cmd_tx.send(Command::ReleaseFirst).expect("first lock release failed");
                    abi::Ready::new(()).await
                })
            };
            let query = db
                .query()
                .run_no_rows(ContentionDatabase::INSERT_ITEM, (2_i64, ()));
            let (result, ()) = fibers::TEST.try_run(driver, abi::Join::new(query, release)).map_err(plan::Error::Run)?;
            result.map_err(plan::Error::Call)?;
            let event = (event_rx.recv_timeout(Duration::from_secs(5))).expect("first commit event wait failed");
            assert!(event == Event::FirstCommitted);

            cmd_tx.send(Command::StartSecond).expect("second stage signal failed");
            let event = (event_rx.recv_timeout(Duration::from_secs(5))).expect("second lock event wait failed");
            assert!(event == Event::SecondLocked);

            let mut tx = fibers::TEST.try_run(driver, db.transaction()).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
            let result = fibers::TEST.try_run(
                    driver,
                    tx.query().run_no_rows(
                        ContentionDatabase::INSERT_ITEM,
                        (4_i64, ()),
                    ),
                );
            assert!(matches!(result,
                Ok(Err(Error::TransactionBusy { action: Action::Step, code })) if code.primary() == 5));

            let result = fibers::TEST.try_run(
                    driver,
                    tx.query()
                        .run_one::<i64, _, _>(ContentionDatabase::COUNT_ITEMS, ()),
                );
            assert!(matches!(result,
                Ok(Err(Error::TransactionBusy { action: Action::Step, code })) if code.primary() == 5));
            fibers::TEST.try_run(driver, tx.rollback()).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;

            cmd_tx.send(Command::ReleaseSecond).expect("second lock release failed");
            blocker.join().expect("blocking SQLite worker join failed");

            let count = fibers::TEST
                .try_run(
                    driver,
                    db.query()
                        .run_one::<i64, _, _>(ContentionDatabase::COUNT_ITEMS, ()),
                ).map_err(plan::Error::Run)?.map_err(plan::Error::Call)?;
            assert_eq!(count, 3);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("contending SQLite scope failed")
}

#[test]
fn zero_delay_yields() -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let dir = Directory::try_with(&base, "cartel-sqlite-contention-yield", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("database.db");
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let db = OpenOptions::new()
        .open(runtime, &path)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let block_db = OpenOptions::new()
        .open(runtime, &path)
        .map_err(plan::Error::Client)?;
    let (mut limits, memory) = try_settings()?;
    let mut budget = limits.work.call;
    budget.busy.interval = Duration::ZERO;
    limits.work = Work::uniform(budget);

    let result =
        plan::Plan::<ContentionDatabase>::try_with(db, limits, memory, Some(Slots::fixed::<1>()))?
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
                    let (lock_tx, lock_rx) = mpsc::channel();
                    let (free_tx, free_rx) = mpsc::channel();
                    let blocker = thread::spawn(move || {
                        let result = (|| -> Result<(), plan::Error> {
                            let (limits, memory) = try_settings()?;
                            let result = plan::Plan::<ContentionDatabase>::try_new(
                                block_db, limits, memory,
                            )?
                            .try_run(
                                |driver, mut db| {
                                    let mut tx = fibers::TEST
                                        .try_run(driver, db.transaction())
                                        .map_err(plan::Error::Run)?
                                        .map_err(plan::Error::Call)?;
                                    fibers::TEST
                                        .try_run(
                                            driver,
                                            tx.query().run_no_rows(
                                                ContentionDatabase::INSERT_ITEM,
                                                (1_i64, ()),
                                            ),
                                        )
                                        .map_err(plan::Error::Run)?
                                        .map_err(plan::Error::Call)?;
                                    assert!(lock_tx.send(()).is_ok(), "write lock signal failed");
                                    assert!(
                                        free_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                                        "lock release wait failed"
                                    );
                                    fibers::TEST
                                        .try_run(driver, tx.commit())
                                        .map_err(plan::Error::Run)?
                                        .map_err(plan::Error::Call)?;
                                    Ok(())
                                },
                                Policy::new(assert_reclaimed, assert_retirement),
                                |result| result.expect("SQLite scope"),
                            );
                            result.expect("blocking SQLite scope failed")
                        })();
                        assert!(result.is_ok(), "blocking SQLite worker failed");
                    });
                    assert!(
                        lock_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                        "write lock wait failed"
                    );

                    let mut task = abi::Task::new(
                        db.query()
                            .run_no_rows(ContentionDatabase::INSERT_ITEM, (99_i64, ())),
                    );
                    assert!(
                        fibers::TEST
                            .try_run(
                                driver,
                                fibers::TEST.poll_once(std::pin::Pin::new(&mut task))
                            )
                            .map_err(plan::Error::Run)?
                            .is_none()
                    );
                    drop(task);

                    let release = dope_fiber::fiber!('_ => async move {
                        assert!(free_tx.send(()).is_ok(), "lock release signal failed");
                        abi::Ready::new(()).await
                    });
                    let query = db
                        .query()
                        .run_no_rows(ContentionDatabase::INSERT_ITEM, (2_i64, ()));
                    let (result, ()) = fibers::TEST
                        .try_run(driver, abi::Join::new(query, release))
                        .map_err(plan::Error::Run)?;
                    result.map_err(plan::Error::Call)?;
                    blocker.join().expect("blocking SQLite worker join failed");

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
