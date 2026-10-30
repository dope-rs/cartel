use crate::common::plan::{self, late};
use crate::{
    common::database::IntegrationDatabase, common::query::dispatch::user_by_id,
    common::query::mutation::rename_user,
};
use cartel_sqlite::{
    open,
    runtime::{self, Runtime},
};
use common::{assert_reclaimed, assert_retirement, schema};
use dope_core::driver::{life::recovery::Policy, settings};
use dope_manifold::timing::Throughput;
use dope_runtime::executor::session;
use dope_test::fibers;
use std::{cell, mem, ops, pin, sync::OnceLock};

pub mod common;

/// Only Cartel uses installed SQLite; Core handles and allocations remain
/// exclusively owned by their originating worker while the installed hooks can run.
static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn transaction_queries() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        let mut tx = conn.transaction().await?;
                        let u = user_by_id(&mut tx, 1).await?;
                        ::core::assert_eq!(u.name, "alice");
                        rename_user(&mut tx, 1, "TX-renamed").await?;
                        let u2 = user_by_id(&mut tx, 1).await?;
                        ::core::assert_eq!(u2.name, "TX-renamed");
                        tx.rollback().await?;
                        let u3 = user_by_id(&mut conn, 1).await?;
                        ::core::assert_eq!(u3.name, "alice");

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn released_work_applies() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        let mut tx = conn.transaction().await?;
                        rename_user(&mut tx, 1, "outer").await?;

                        drop(tx.savepoint().await?);
                        let mut save = tx.savepoint().await?;
                        rename_user(&mut save, 2, "released").await?;

                        let mut nested = save.savepoint().await?;
                        let error = (nested.savepoint().await)
                            .err()
                            .expect("save exceeded its nesting capacity");
                        assert!(matches!(
                            error,
                            cartel_sqlite::Error::Savepoints {
                                required: 3,
                                available: 2
                            }
                        ));
                        rename_user(&mut nested, 3, "rolled-back").await?;
                        nested.rollback().await?;
                        ::core::assert_eq!(user_by_id(&mut nested, 3).await?.name, "carol");
                        nested.commit().await?;

                        ::core::assert_eq!(user_by_id(&mut save, 2).await?.name, "released");
                        save.commit().await?;
                        tx.commit().await?;

                        ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "outer");
                        ::core::assert_eq!(user_by_id(&mut conn, 2).await?.name, "released");
                        ::core::assert_eq!(user_by_id(&mut conn, 3).await?.name, "carol");

                        for forgotten in [false, true] {
                            for (rollback, commit) in [(false, true), (true, true), (false, false)] {
                                let mut tx = conn.transaction().await?;
                                rename_user(&mut tx, 1, "parent").await?;
                                rename_user(&mut tx, 2, "parent").await?;
                                let mut outer = tx.savepoint().await?;
                                rename_user(&mut outer, 1, "outer").await?;
                                let mut inner = outer.savepoint().await?;
                                rename_user(&mut inner, 2, "inner").await?;
                                if forgotten {
                                    mem::forget(inner);
                                } else {
                                    drop(inner);
                                }
                                if rollback {
                                    outer.rollback().await?;
                                }
                                if commit {
                                    outer.commit().await?;
                                } else {
                                    drop(outer);
                                }
                                let mut last = tx.savepoint().await?;
                                last.savepoint().await?.commit().await?;
                                last.commit().await?;
                                tx.commit().await?;

                                let kept = commit && !rollback;
                                assert_eq!(user_by_id(&mut conn, 1).await?.name, if kept { "outer" } else { "parent" });
                                assert_eq!(user_by_id(&mut conn, 2).await?.name, if kept && forgotten { "inner" } else { "parent" });
                            }
                        }

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn scopes_restore_owner() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (mut limits, memory) = schema::try_settings()?;
        limits.savepoints = 1;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        {
                            let mut tx = conn.transaction().await?;
                            {
                                let mut save = tx.savepoint().await?;
                                rename_user(&mut save, 1, "save").await?;
                            }

                            let mut replacement = tx.savepoint().await?;
                            ::core::assert_eq!(user_by_id(&mut replacement, 1).await?.name, "alice");
                            replacement.commit().await?;
                            rename_user(&mut tx, 2, "tx").await?;
                        }

                        ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "alice");
                        ::core::assert_eq!(user_by_id(&mut conn, 2).await?.name, "bob");

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn unpolled_restore() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        for commit in [false, true] {
                            let mut tx = conn.transaction().await?;
                            rename_user(&mut tx, 1, "cancelled").await?;
                            if commit {
                                drop(tx.commit());
                            } else {
                                drop(tx.rollback());
                            }
                            ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "alice");
                        }

                        let mut tx = conn.transaction().await?;
                        drop(tx.savepoint());
                        ::core::assert_eq!(user_by_id(&mut tx, 2).await?.name, "bob");

                        let mut save = tx.savepoint().await?;
                        rename_user(&mut save, 2, "release").await?;
                        drop(save.commit());
                        ::core::assert_eq!(user_by_id(&mut tx, 2).await?.name, "bob");
                        tx.rollback().await?;

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn savepoints_retire_order() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        {
                            let mut tx = conn.transaction().await?;
                            {
                                let mut outer = tx.savepoint().await?;
                                rename_user(&mut outer, 1, "outer").await?;
                                {
                                    let mut inner = outer.savepoint().await?;
                                    rename_user(&mut inner, 2, "inner").await?;
                                }
                            }

                            ::core::assert_eq!(user_by_id(&mut tx, 1).await?.name, "alice");
                            ::core::assert_eq!(user_by_id(&mut tx, 2).await?.name, "bob");
                        }

                        ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "alice");
                        ::core::assert_eq!(user_by_id(&mut conn, 2).await?.name, "bob");

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn shutdown_drains_scope() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        {
                            let mut tx = conn.transaction().await?;
                            rename_user(&mut tx, 1, "uncommitted").await?;
                        }

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn cleanup_stays_failed() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = schema::try_settings()?;
    let plan = plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
    let result = plan.try_run(
        |driver, mut conn| {
            fibers::TEST
                .try_run(
                    driver,
                    dope_fiber::fiber!('_ => async move {
                        let mut tx = conn.transaction().await?;
                        let mut outer = tx.savepoint().await?;
                        let mut inner = outer.savepoint().await?;
                        fibers::Yield::new().await;

                        let seen = cell::Cell::new(false);
                        let scan = inner.query()
                            .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, ())
                            .visit(|_| {
                                seen.set(true);
                                ops::ControlFlow::Continue(())
                            });
                        let mut task = dope_fiber::abi::Task::new(scan);
                        assert!(fibers::TEST.poll_once(pin::Pin::new(&mut task)).await.is_none());
                        assert!(seen.get());
                        mem::forget(task);
                        drop(inner);

                        assert!(matches!(
                            user_by_id(&mut outer, 1).await,
                            Err(cartel_sqlite::Error::ReentrantQuery)
                        ));
                        drop(outer);
                        assert!(matches!(
                            user_by_id(&mut tx, 1).await,
                            Err(cartel_sqlite::Error::ConnectionPoisoned)
                        ));
                        drop(tx);
                        assert!(matches!(
                            user_by_id(&mut conn, 1).await,
                            Err(cartel_sqlite::Error::ConnectionPoisoned)
                        ));
                        Ok::<(), cartel_sqlite::Error>(())
                    }),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite terminal cleanup scope"),
    );
    result.expect("SQLite terminal cleanup installation")
}

#[test]
fn forgotten_client_drains() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = schema::try_settings()?;
    let plan = plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
    plan.try_run(
        |driver, mut conn| {
            fibers::TEST
                .try_run(
                    driver,
                    dope_fiber::fiber!('_ => async move {
                        let mut tx = conn.transaction().await?;
                        fibers::Yield::new().await;
                        let seen = cell::Cell::new(false);
                        let scan = tx.query()
                            .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, ())
                            .visit(|_| {
                                seen.set(true);
                                ops::ControlFlow::Continue(())
                            });
                        let mut task = dope_fiber::abi::Task::new(scan);
                        assert!(fibers::TEST.poll_once(pin::Pin::new(&mut task)).await.is_none());
                        assert!(seen.get());
                        mem::forget(task);
                        drop(tx);
                        mem::forget(conn);
                        Ok::<(), cartel_sqlite::Error>(())
                    }),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| {
            assert!(
                matches!(
                    result,
                    Err(plan::Cause::Cleanup {
                        output: Ok(Ok(Ok(()))),
                        first: cartel_sqlite::Error::ReentrantQuery,
                        second: None,
                    })
                ),
                "forgotten connection cleanup: {result:?}"
            );
        },
    );
    Ok(())
}

#[test]
fn tail_transaction_drains() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = schema::try_settings()?;
    let plan = schema::open(runtime)?
        .plan::<IntegrationDatabase>(limits, memory)
        .map_err(plan::Error::Plan)?
        .with_config(settings::Config::for_profile::<Throughput>())
        .map_err(plan::Error::Claim)?;
    let entered = cell::Cell::new(false);
    plan.try_create(
        |worker| {
            let mut cleanup = open::Completion::new();
            let mut complete = session::Completion::new();
            worker
                .install_with(|open, stage| open.install(stage, &mut cleanup))
                .expect("tail database installation")
                .with_lent(&mut complete, late::Create(&entered))
                .expect("tail application scope")
                .expect("tail reaches its active transaction");
            assert!(entered.get());
            assert!(matches!(
                cleanup.take(),
                Err((cartel_sqlite::Error::ReentrantQuery, None))
            ));
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| assert!(result.is_ok(), "tail driver scope"),
    );
    Ok(())
}
