use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, OpenOptions, Progress, RequestPhase, open};
use common::catalog::*;
use dope_core::driver::life::recovery::Policy;
use dope_core::driver::settings::demands::slots::Slots;
use dope_manifold::timing::Throughput;
use dope_runtime::topology;
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use std::num::NonZero;
use std::{path::Path, sync::OnceLock};
use std::{thread, time};

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn work_limit_releases() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(base, "cartel-sqlite-bounded-command", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    let target = open(runtime, &path)?
        .commands(EXPENSIVE_COMMANDS)
        .map_err(plan::Error::Client)?;
    let result = plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        });
    assert!(matches!(result,
        Err(plan::Failure::Install(
            topology::InstallError::Install(open::Error::Step(Error::WorkLimit {
                action: Action::Step, code
            }))
        )) if code.primary() == libsqlite3_sys::SQLITE_INTERRUPT
    ));
    assert_eq!(
        plan::Plan::try_new(open(runtime, &path)?, try_limits()?, try_memory()?)?
            .try_count(
                Bounded::COUNT,
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            )
            .expect("SQLite count scope")?,
        0
    );
    Ok(())
}

#[test]
fn open_work_is_aggregate() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let mut bounded = try_limits()?;
    bounded.work.open.progress = Progress::new(NonZero::new(70).ok_or(plan::Error::Progress)?);

    let one = Directory::try_with(base, "cartel-sqlite-single-open-work", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let target = open(runtime, &one.path().join("catalog.db"))?
        .commands(SINGLE_WORK_COMMANDS)
        .map_err(plan::Error::Client)?;
    plan::Plan::<Commands>::try_new(target, bounded, try_memory()?)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        })
        .expect("one command fits the open work budget");

    let sum = Directory::try_with(base, "cartel-sqlite-aggregate-open-work", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = sum.path().join("catalog.db");
    let target = open(runtime, &path)?
        .commands(AGGREGATE_WORK_COMMANDS)
        .map_err(plan::Error::Client)?;
    let result = plan::Plan::<Commands>::try_new(target, bounded, try_memory()?)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        });
    assert!(matches!(result,
        Err(plan::Failure::Install(
            topology::InstallError::Install(open::Error::Step(Error::WorkLimit {
                action: Action::Step, code
            }))
        )) if code.primary() == libsqlite3_sys::SQLITE_INTERRUPT
    ));
    assert_eq!(
        plan::Plan::try_new(open(runtime, &path)?, try_limits()?, try_memory()?)?
            .try_count(
                Aggregate::COUNT,
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            )
            .expect("SQLite count scope")?,
        1001
    );
    Ok(())
}

#[test]
fn short_commands_exhaust_open_work() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    for (commands, fits) in [
        (&SHORT_WORK_COMMANDS[..2], true),
        (SHORT_WORK_COMMANDS, false),
    ] {
        let target = OpenOptions::new()
            .open_memory(runtime)
            .map_err(plan::Error::Client)?
            .commands(commands)
            .map_err(plan::Error::Client)?;
        let result = plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
            .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                result.expect("SQLite scope")
            });
        if fits {
            result.expect("short command prefix fits the open work budget");
        } else {
            assert!(matches!(
                result,
                Err(plan::Failure::Install(topology::InstallError::Install(
                    open::Error::Step(Error::WorkExhausted)
                )))
            ));
        }
    }
    Ok(())
}

#[test]
fn use_connection_limits() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(base, "cartel-sqlite-local-command", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    let target = open(runtime, &path)?
        .commands(OVERSIZED_COMMANDS)
        .map_err(plan::Error::Client)?;
    let result = plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        });
    assert!(matches!(result,
        Err(plan::Failure::Install(
            topology::InstallError::Install(open::Error::Step(Error::Sqlite {
                action: Action::Step, code
            }))
        )) if code.primary() == libsqlite3_sys::SQLITE_TOOBIG
    ));
    assert_eq!(
        plan::Plan::try_new(open(runtime, &path)?, try_limits()?, try_memory()?)?
            .try_count(
                Compact::COUNT,
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            )
            .expect("SQLite count scope")?,
        0
    );
    Ok(())
}

#[test]
fn startup_timeout_releases() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(base, "cartel-sqlite-startup-contention", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    assert_eq!(
        plan::Plan::try_new(
            open(runtime, &path)?
                .commands(STABLE_COMMANDS)
                .map_err(plan::Error::Client)?,
            try_limits()?,
            try_memory()?
        )
        .and_then(|plan| plan
            .try_count(
                Stable::COUNT,
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            )
            .expect("SQLite count scope"))?,
        1
    );
    let held = open(runtime, &path)?;
    let rival = open(runtime, &path)?
        .commands(CONTENDED_COMMANDS)
        .map_err(plan::Error::Client)?;
    let yielding = open(runtime, &path)?
        .commands(CONTENDED_COMMANDS)
        .map_err(plan::Error::Client)?;

    let result = plan::Plan::<Stable>::try_new(held, try_limits()?, try_memory()?)?.try_run(
        |driver, mut conn| {
            let mut tx = fibers::TEST
                .try_run(driver, conn.transaction())
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            fibers::TEST
                .try_run(driver, tx.query().run_no_rows(Stable::INSERT, (2_i64, ())))
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;

            let attempt = thread::spawn(move || {
                let mut limits = try_limits().expect("SQLite limits");
                limits.work.open.busy.window = time::Duration::MAX;
                limits.work.open.busy.interval = time::Duration::ZERO;
                limits.work.open.busy.retries = NonZero::<u32>::MIN;
                yielding
                    .plan::<Commands>(limits, try_memory().expect("SQLite memory"))
                    .expect("yielding contention plan")
                    .for_profile::<Throughput>()
                    .expect("yielding contention resources")
                    .try_create(
                        |worker| {
                            let mut cleanup = open::Completion::new();
                            let mut polls = 0;
                            assert!(matches!(
                                worker.install_with(|open, stage| plan::Polls {
                                    root: open.install(stage, &mut cleanup),
                                    count: &mut polls,
                                }),
                                Err(topology::InstallError::Install(open::Error::Step(
                                    Error::Busy {
                                        action: Action::Step,
                                        code,
                                    }
                                ))) if code.primary() == libsqlite3_sys::SQLITE_BUSY
                            ));
                            assert_eq!(polls, 2, "one retry must yield between startup polls");
                            assert!(cleanup.take().is_ok(), "yielded startup cleanup");
                        },
                        Policy::new(assert_reclaimed, assert_retirement),
                        |outcome| {
                            outcome
                                .as_ref()
                                .map_err(|error| error.cause())
                                .expect("yielding SQLite scope");
                        },
                    );
                let mut limits = try_limits().expect("SQLite limits");
                limits.work.open.busy.window = time::Duration::from_millis(20);
                let plan = plan::Plan::<Commands>::try_with(
                    rival,
                    limits,
                    try_memory().expect("SQLite memory"),
                    Some(Slots::ZERO),
                );
                assert!(plan.is_ok(), "contention plan construction failed");
                let plan = plan.expect("contention plan construction failed");
                let result = plan
                    .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                        result.expect("SQLite scope")
                    });
                assert!(
                    matches!(result, Err(plan::Failure::Install(
                    topology::InstallError::Install(open::Error::Step(Error::RequestTimeout {
                        elapsed,
                        action: Action::Step,
                        code,
                    }))
                )) if elapsed.phase() == RequestPhase::Contention && code.primary() == 5),
                    "contention must exhaust the startup deadline"
                );
            });
            (attempt.join()).expect("contention worker failed");
            fibers::TEST
                .try_run(driver, tx.rollback())
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("command contention scope failed")?;

    assert!(
        plan::Plan::<Commands>::try_new(
            open(runtime, &path)?
                .commands(CONTENDED_COMMANDS)
                .map_err(plan::Error::Client)?,
            try_limits()?,
            try_memory()?
        )
        .expect("SQLite runtime succeeds")
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        },)
        .is_ok(),
        "SQLite installation succeeds"
    );
    Ok(())
}

#[test]
fn readonly_reuses_schema() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(base, "cartel-sqlite-read-only", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    let target = open(runtime, &path)?
        .commands(STABLE_COMMANDS)
        .map_err(plan::Error::Client)?;
    plan::Plan::<Stable>::try_new(target, try_limits()?, try_memory()?)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        })
        .expect("fresh schema installation succeeds");
    assert_eq!(
        plan::Plan::try_new(readonly(runtime, &path)?, try_limits()?, try_memory()?)?
            .try_count(
                Stable::COUNT,
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            )
            .expect("SQLite count scope")?,
        1
    );
    Ok(())
}
