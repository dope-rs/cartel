use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, Progress};
use common::engine::heap::*;
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::num::NonZero;
use std::sync::OnceLock;
use std::{sync::mpsc, thread, time::Duration};

pub mod common;

/// Only Cartel and same-worker allocator checks use installed SQLite; Core handles
/// and allocations stay exclusively with their original worker while hooks can run.
static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn empty_scope_keeps_heap() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings()?;
    let outer = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?;
    let layout = outer.layout();
    let result = outer.try_run(
        |driver, mut database| {
            let (limits, memory) = try_settings()?;
            open(runtime)?
                .plan::<HeapDatabase>(limits, memory)
                .map_err(plan::Error::Plan)?
                .with_config(layout)
                .map_err(plan::Error::Claim)?
                .try_create(
                    |_| (),
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| assert!(result.is_ok(), "unopened SQLite scope"),
                );
            let value = fibers::TEST
                .try_run(
                    driver,
                    database
                        .query()
                        .run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(value, 1);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("outer SQLite scope failed")
}

#[test]
fn realloc_recovers() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            fibers::TEST
                .try_run(
                    driver,
                    dope_fiber::fiber!('_ => async move {
                        for rows in [16_i64, 256, 16] {
                            let result = database.query()
                                .run_one::<i64, _, _>(HeapDatabase::GROW, (rows, ())).await;
                            if rows == 256 {
                                assert!(
                                    matches!(&result, Err(Error::Sqlite {
                                        action: Action::Step, code,
                                    }) if code.primary() == 7),
                                    "growing allocation: {result:?}"
                                );
                            } else {
                                assert_eq!(result?, 16 * 1024 + 15);
                            }
                        }
                        Ok::<(), Error>(())
                    }),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("growing allocation cleanup scope")
}

#[test]
fn column_reset_recovery() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            fibers::TEST
                .try_run(
                    driver,
                    dope_fiber::fiber!('_ => async move {
                        database.query().run_no_rows(HeapDatabase::INSERT_SMALL, ()).await?;
                        let mut txn = database.transaction().await?;
                        let result = txn.query()
                            .run_one::<i64, _, _>(HeapDatabase::MATERIALIZE, ()).await;
                        assert!(
                            matches!(&result, Err(Error::Sqlite {
                                action: Action::ColumnBlob, code,
                            }) if code.primary() == 7),
                            "blob materialization: {result:?}"
                        );
                        let result = txn.query()
                            .run_one::<i64, _, _>(HeapDatabase::CHECK, ()).await;
                        assert!(
                            matches!(&result, Err(Error::TransactionAborted {
                                action: Action::Reset, code,
                            }) if code.primary() == 7),
                            "aborted transaction: {result:?}"
                        );
                        drop(txn);
                        let result = database.query()
                            .run_one::<i64, _, _>(HeapDatabase::CHECK, ()).await;
                        assert!(
                            matches!(&result, Err(Error::Sqlite {
                                action: Action::Reset, code,
                            }) if code.primary() == 7),
                            "deferred reset: {result:?}"
                        );
                        let value = database.query()
                            .run_one::<i64, _, _>(HeapDatabase::CHECK, ()).await?;
                        assert_eq!(value, 1);
                        Ok::<(), Error>(())
                    }),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("column failure cleanup scope")
}

#[test]
fn failed_query_keeps_reset() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings()?;
    plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            fibers::TEST
                .try_run(
                    driver,
                    dope_fiber::fiber!('_ => async move {
                        database.query().run_no_rows(HeapDatabase::INSERT_SMALL, ()).await?;
                        let mut txn = database.transaction().await?;
                        let value = txn.query()
                            .run_one::<i64, _, _>(HeapDatabase::MATERIALIZE, ()).await?;
                        Ok::<_, Error>(value)
                    }),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| {
            assert!(
                matches!(&result, Err(plan::Cause::Cleanup {
                    output: Ok(Ok(Err(plan::Error::Call(Error::Sqlite {
                        action: Action::ColumnBlob, code: primary,
                    })))),
                    first: Error::Sqlite { action: Action::Reset, code: reset },
                    second: None,
                }) if primary.primary() == 7 && reset.primary() == 7),
                "query and reset failures: {result:?}"
            );
        },
    );
    Ok(())
}

#[test]
fn reset_cleanup_receipts() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (mut limits, memory) = try_settings()?;
    limits.work.cleanup.progress = Progress::new(NonZero::<u64>::MIN);
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_app(
        |driver| {
            {
                let mut conn = driver.root_client().map_err(plan::Error::Client)?;
                fibers::TEST
                    .try_run(
                        driver,
                        conn.query().run_no_rows(HeapDatabase::INSERT_SMALL, ()),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
            }
            let mut limited = false;
            for _ in 0..1_000 {
                let mut conn = driver.root_client().map_err(plan::Error::Client)?;
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {
                            let mut txn = conn.transaction().await?;
                            let result = txn.query()
                                .run_one::<i64, _, _>(HeapDatabase::LOCAL_BLOB, ()).await;
                            assert!(
                                matches!(&result, Err(Error::Sqlite {
                                    action: Action::ColumnBlob, code,
                                }) if code.primary() == 7),
                                "local materialization: {result:?}"
                            );
                            drop(txn);
                            drop(conn);
                            Ok::<(), Error>(())
                        }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                fibers::TEST
                    .try_run(driver, fibers::Yield::new())
                    .map_err(plan::Error::Run)?;
                let mut conn = driver.root_client().map_err(plan::Error::Client)?;
                let result = fibers::TEST
                    .try_run(
                        driver,
                        conn.query().run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                    )
                    .map_err(plan::Error::Run)?;
                assert!(
                    matches!(&result, Err(Error::Sqlite {
                        action: Action::Reset, code,
                    }) if code.primary() == 7),
                    "retained reset: {result:?}"
                );
                let result = fibers::TEST
                    .try_run(
                        driver,
                        conn.query().run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                    )
                    .map_err(plan::Error::Run)?;
                if matches!(result, Ok(1)) {
                    continue;
                }
                assert!(
                    matches!(&result, Err(Error::WorkLimit {
                        action: Action::Step, code,
                    }) if code.primary() == 9),
                    "retained cleanup: {result:?}"
                );
                let result = fibers::TEST
                    .try_run(
                        driver,
                        conn.query().run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                    )
                    .map_err(plan::Error::Run)?;
                assert!(matches!(result, Err(Error::ConnectionPoisoned)));
                limited = true;
                break;
            }
            assert!(limited, "cleanup progress limit was not reached");
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("reset and cleanup scope")
}

#[test]
fn heaps_are_isolated() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (full_tx, full_rx) = mpsc::sync_channel(0);
    let (free_tx, free_rx) = mpsc::sync_channel(0);

    let healthy = thread::spawn(move || {
        let result = (|| -> Result<(), plan::Error> {
            let (limits, memory) = try_settings()?;
            let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?
                .try_run(
                    |driver, mut database| {
                        assert!(ready_tx.send(()).is_ok(), "healthy worker signal failed");
                        assert!(
                            full_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                            "peer exhaustion wait failed"
                        );
                        fibers::TEST
                            .try_run(
                                driver,
                                database.query().run_no_rows(HeapDatabase::INSERT_SMALL, ()),
                            )
                            .map_err(plan::Error::Run)?
                            .map_err(plan::Error::Call)?;
                        let value = fibers::TEST
                            .try_run(
                                driver,
                                database
                                    .query()
                                    .run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                            )
                            .map_err(plan::Error::Run)?
                            .map_err(plan::Error::Call)?;
                        assert_eq!(value, 1);
                        Ok(())
                    },
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                );
            result.expect("healthy SQLite scope failed")
        })();
        assert!(result.is_ok(), "healthy SQLite worker failed");
    });

    assert!(
        ready_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
        "healthy worker wait failed"
    );
    let full = thread::spawn(move || {
        let result = (|| -> Result<(), plan::Error> {
            let (limits, memory) = try_settings()?;
            let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?
                .try_run(
                    |driver, mut database| {
                        let mut failure = None;
                        for _ in 0..1_024 {
                            match fibers::TEST.try_run(
                                driver,
                                database
                                    .query()
                                    .run_no_rows(HeapDatabase::INSERT_PAYLOAD, ()),
                            ) {
                                Ok(Err(error)) => {
                                    failure = Some(error);
                                    break;
                                }
                                Ok(Ok(())) => {}
                                Err(error) => return Err(plan::Error::Run(error)),
                            }
                        }
                        let error = (failure).expect("worker heap had no allocation ceiling");
                        assert!(
                            matches!(error, Error::Sqlite { code, .. } if code.primary() == 7),
                            "expected SQLite allocation failure"
                        );
                        assert!(full_tx.send(()).is_ok(), "heap exhaustion signal failed");
                        assert!(
                            free_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                            "full heap release failed"
                        );
                        Ok(())
                    },
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                );
            result.expect("full SQLite scope failed")
        })();
        assert!(result.is_ok(), "full SQLite worker failed");
    });

    assert!(healthy.join().is_ok(), "healthy worker join failed");
    assert!(free_tx.send(()).is_ok(), "full worker release failed");
    assert!(full.join().is_ok(), "full worker join failed");

    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            let value = fibers::TEST
                .try_run(
                    driver,
                    database
                        .query()
                        .run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(value, 1);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("events SQLite scope failed")
}
