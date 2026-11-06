use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, Progress, open};
use common::engine::heap::*;
use dope_core::driver::life::recovery::Policy;
use dope_runtime::{executor::session, process::lifecycle, topology::InstallError};
use dope_test::fibers;
use std::{num::NonZero, ptr::NonNull, time::Duration};

pub mod common;

#[test]
fn heap_contracts() -> Result<(), plan::Error> {
    // SAFETY: this binary has one test; installation precedes every SQLite call
    // and worker start. Only Cartel uses installed SQLite; Core handles and
    // allocations remain exclusively owned by their originating worker until release.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    empty_scope_keeps_heap(runtime)?;
    realloc_recovers(runtime)?;
    column_reset_recovery(runtime)?;
    failed_query_keeps_reset(runtime)?;
    reset_cleanup_receipts(runtime)?;
    heaps_are_isolated(runtime)
}

fn empty_scope_keeps_heap(runtime: Runtime) -> Result<(), plan::Error> {
    let (limits, memory) = try_settings()?;
    let outer = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?;
    let layout = outer.layout();
    let result = outer.try_run(
        |driver, mut database| {
            for attempt in [false, true] {
                let (limits, memory) = try_settings()?;
                open(runtime)?
                    .plan::<HeapDatabase>(limits, memory)
                    .map_err(plan::Error::Plan)?
                    .with_config(layout)
                    .map_err(plan::Error::Claim)?
                    .try_create(
                        (),
                        |worker| {
                            if attempt {
                                let mut cleanup = open::Completion::new();
                                assert!(matches!(
                                    worker.install_with(
                                        |plan, stage| plan.install(stage, &mut cleanup)
                                    ),
                                    Err(InstallError::Install(open::Error::Step(
                                        Error::LocalAllocatorOccupied
                                    )))
                                ));
                                assert!(cleanup.take().is_ok(), "conflicting SQLite cleanup");
                            }
                        },
                        Policy::new(assert_reclaimed, assert_retirement),
                        |result| assert!(result.is_ok(), "nested SQLite scope"),
                    );
            }
            let value = fibers::TEST
                .try_run(
                    driver,
                    database
                        .query()
                        .run_one::<i64, _, _>(HeapDatabase::GROW, (1_i64, ())),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(value, 1024);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("outer SQLite scope failed")
}

fn realloc_recovers(runtime: Runtime) -> Result<(), plan::Error> {
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

fn column_reset_recovery(runtime: Runtime) -> Result<(), plan::Error> {
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

fn failed_query_keeps_reset(runtime: Runtime) -> Result<(), plan::Error> {
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

fn reset_cleanup_receipts(runtime: Runtime) -> Result<(), plan::Error> {
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

fn heaps_are_isolated(runtime: Runtime) -> Result<(), plan::Error> {
    let (limits, memory) = try_settings()?;
    let healthy = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?;
    let layout = healthy.layout();
    let result = healthy.try_run(
        |driver, mut database| {
            use lifecycle::Task as _;

            let (main, worker) = lifecycle::Main::try_pair().expect("worker lifecycle");
            let ((observed, stopped), ()) = lifecycle::Entry(move || {
                let outcome = worker.run(|source| {
                    open(runtime)?
                        .plan::<HeapDatabase>(limits, memory)
                        .map_err(plan::Error::Plan)?
                        .with_config(layout)
                        .map_err(plan::Error::Claim)?
                        .try_create(
                            source,
                            |worker| {
                                let mut cleanup = open::Completion::new();
                                let app = worker
                                    .install_with(|open, stage| open.install(stage, &mut cleanup))
                                    .expect("full SQLite installation");
                                let mut blocks = Vec::new();
                                // Fill before with_app publishes Installed; each native block
                                // retains this worker's arena until its matching free.
                                for shift in (0..=SQLITE_BYTES.ilog2()).rev() {
                                    while let Some(block) = NonNull::new(unsafe {
                                        libsqlite3_sys::sqlite3_malloc64(1_u64 << shift)
                                    }) {
                                        blocks.push(block);
                                    }
                                }
                                let mut complete = session::Completion::new();
                                let output = app.with_app(&mut complete, |mut app| {
                                    let mut database =
                                        app.root_client().expect("full SQLite client");
                                    let exhausted = fibers::TEST.try_run(
                                        &mut app,
                                        database
                                            .query()
                                            .run_no_rows(HeapDatabase::INSERT_PAYLOAD, ()),
                                    );
                                    let stopped = app.run_for(Duration::from_secs(5)).map(|_| ());
                                    drop(database);
                                    (exhausted, stopped)
                                });
                                for block in blocks {
                                    // SAFETY: each receipt is freed once on its original worker,
                                    // while the owning State remains inside this callback.
                                    unsafe { libsqlite3_sys::sqlite3_free(block.as_ptr()) };
                                }
                                let (exhausted, stopped) = output.expect("full SQLite session");
                                assert!(
                                    matches!(exhausted, Ok(Err(Error::Sqlite { code, .. }))
                                if code.primary() == 7),
                                    "expected SQLite allocation failure"
                                );
                                stopped.expect("full worker shutdown");
                                cleanup.take().expect("full SQLite cleanup");
                            },
                            Policy::new(assert_reclaimed, assert_retirement),
                            |result| {
                                if let Err(error) = result {
                                    panic!("full SQLite scope: {:?}", error.kind());
                                }
                            },
                        );
                    Ok::<(), plan::Error>(())
                });
                assert!(
                    matches!(&outcome, lifecycle::Outcome::Done(())),
                    "SQLite worker: {outcome:?}"
                );
            })
            .with(|| {
                main.run(Duration::from_secs(5), || {
                    fibers::TEST
                        .try_run(
                            driver,
                            database
                                .query()
                                .run_one::<i64, _, _>(HeapDatabase::GROW, (1_i64, ())),
                        )
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)
                })
            })
            .expect("SQLite worker start and join");
            stopped.expect("SQLite worker stop");
            assert_eq!(observed.expect("SQLite worker readiness")?, 1024);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("healthy SQLite scope failed")?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_count(
        HeapDatabase::CHECK,
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    assert_eq!(result.expect("events SQLite scope failed")?, 1);
    Ok(())
}
