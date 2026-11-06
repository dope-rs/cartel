use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, open};
use common::catalog::*;
use dope_core::driver::life::recovery::Policy;
use dope_runtime::topology;
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use std::path::Path;

pub mod common;

#[test]
fn commands() -> Result<(), plan::Error> {
    // SAFETY: this is the binary's only SQLite test and installs before any native call.
    // Core handles and allocations remain exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    plan_keeps_file_absent(runtime)?;
    commands_before_queries(runtime)?;
    failed_command_releases(runtime)?;
    sql_errors_keep_offsets(runtime)?;
    nul_rejected_before_open(runtime)?;
    require_statement(runtime)?;
    reject_parameters(runtime)?;
    reject_result_columns(runtime)?;
    preserve_authorization(runtime)?;
    catalog_preserves_authorization(runtime)?;
    Ok(())
}

fn plan_keeps_file_absent(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-pure-plan", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    let target = open(runtime, &path)?
        .commands(SEED_COMMANDS)
        .map_err(plan::Error::Client)?;
    let plan = plan::Plan::<Seed>::try_new(target, try_limits()?, try_memory()?)?;
    assert!(!path.exists());
    plan.try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
        result.expect("SQLite scope")
    })
    .expect("SQLite installation succeeds");
    assert!(path.exists());
    Ok(())
}

fn commands_before_queries(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-commands-open", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    let insert = STAGED_COMMANDS[1].to_owned();
    let commands = [STAGED_COMMANDS[0], insert.as_str(), STAGED_COMMANDS[2]];
    let target = open(runtime, &path)?
        .commands(&commands)
        .map_err(plan::Error::Client)?;
    plan::Plan::<Staged>::try_new(target, try_limits()?, try_memory()?)?
        .try_run(
            |driver, mut conn| {
                let count = fibers::TEST
                    .try_run(driver, conn.query().run_one::<i64, _, _>(Staged::COUNT, ()))
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                let sum = fibers::TEST
                    .try_run(driver, conn.query().run_one::<i64, _, _>(Staged::SUM, ()))
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                let max = fibers::TEST
                    .try_run(driver, conn.query().run_one::<i64, _, _>(Staged::MAX, ()))
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert_eq!((count, sum, max), (2, 5, 3));
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
        .expect("SQLite installation succeeds")?;
    Ok(())
}

fn failed_command_releases(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-command-failure", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    for (name, commands, expected, native, count) in [
        (
            "prepare.db",
            BROKEN_COMMANDS,
            Action::Prepare,
            libsqlite3_sys::SQLITE_ERROR,
            0,
        ),
        (
            "step.db",
            DUPLICATE_COMMANDS,
            Action::Step,
            libsqlite3_sys::SQLITE_CONSTRAINT_PRIMARYKEY,
            1,
        ),
    ] {
        let path = dir.path().join(name);
        let target = open(runtime, &path)?
            .commands(commands)
            .map_err(plan::Error::Client)?;
        assert!(matches!(
            plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
                .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                    result.expect("SQLite scope")
                },),
            Err(plan::Failure::Install(topology::InstallError::Install(
                open::Error::Step(Error::Sqlite { action, code })
            ))) if action == expected && code.get() == native
        ));
        assert_eq!(
            plan::Plan::try_new(open(runtime, &path)?, try_limits()?, try_memory()?)?
                .try_count(
                    Stable::COUNT,
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                )
                .expect("SQLite count scope")?,
            count
        );
    }
    Ok(())
}

fn sql_errors_keep_offsets(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-sql-errors", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    for (name, commands, expected) in [
        ("tail.db", TAIL_COMMANDS, 10),
        ("unicode-tail.db", UNICODE_TAIL_COMMANDS, 14),
    ] {
        let target = open(runtime, &dir.path().join(name))?
            .commands(commands)
            .map_err(plan::Error::Client)?;
        assert!(matches!(
            plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
                .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                    result.expect("SQLite scope")
                },),
            Err(plan::Failure::Install(topology::InstallError::Install(
                open::Error::Step(Error::StatementTail { offset })
            ))) if offset == expected
        ));
    }
    for expected in [Some(10), Some(14), None] {
        let target = cartel_sqlite::OpenOptions::new()
            .open_memory(runtime)
            .map_err(plan::Error::Client)?;
        let mut reports = 0;
        let report = Policy::new(
            |event| {
                assert_reclaimed(event);
                reports += 1;
            },
            assert_retirement,
        );
        let result = match expected {
            Some(14) => {
                plan::Plan::<Malformed<true, true>>::try_new(target, try_limits()?, try_memory()?)?
                    .try_install(report, |result| result.expect("SQLite scope"))
            }
            Some(_) => {
                plan::Plan::<Malformed<true>>::try_new(target, try_limits()?, try_memory()?)?
                    .try_install(report, |result| result.expect("SQLite scope"))
            }
            None => plan::Plan::<Malformed<false>>::try_new(target, try_limits()?, try_memory()?)?
                .try_install(report, |result| result.expect("SQLite scope")),
        };
        assert!(match (expected, result) {
            (
                Some(expected),
                Err(plan::Failure::Install(topology::InstallError::Install(open::Error::Step(
                    Error::StatementTail { offset },
                )))),
            ) => offset == expected,
            (
                None,
                Err(plan::Failure::Install(topology::InstallError::Install(open::Error::Step(
                    Error::ParameterCount {
                        expected: 0,
                        actual: 1,
                    },
                )))),
            ) => true,
            _ => false,
        });
        assert_eq!(
            reports, 0,
            "Catalog failure closes SQLite before application installation can report Shutdown."
        );
    }
    Ok(())
}

fn nul_rejected_before_open(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-command-nul", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("catalog.db");
    assert!(matches!(
        open(runtime, &path)?.commands(NUL_COMMANDS),
        Err(Error::StatementNul { offset: 6 })
    ));
    assert!(!path.exists());
    Ok(())
}

fn require_statement(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-command-empty", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let target = open(runtime, &dir.path().join("catalog.db"))?
        .commands(EMPTY_COMMANDS)
        .map_err(plan::Error::Client)?;
    assert!(matches!(
        plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
            .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                result.expect("SQLite scope")
            },),
        Err(plan::Failure::Install(topology::InstallError::Install(
            open::Error::Step(Error::StatementMissing { bytes: 3 })
        )))
    ));
    Ok(())
}

fn reject_parameters(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-command-parameters", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let target = open(runtime, &dir.path().join("catalog.db"))?
        .commands(PARAMETER_COMMANDS)
        .map_err(plan::Error::Client)?;
    assert!(matches!(
        plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
            .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                result.expect("SQLite scope")
            },),
        Err(plan::Failure::Install(topology::InstallError::Install(
            open::Error::Step(Error::ParameterCount {
                expected: 0,
                actual: 1
            })
        )))
    ));
    Ok(())
}

fn reject_result_columns(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-command-columns", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let target = open(runtime, &dir.path().join("catalog.db"))?
        .commands(ROW_COMMANDS)
        .map_err(plan::Error::Client)?;
    assert!(matches!(
        plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?
            .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                result.expect("SQLite scope")
            },),
        Err(plan::Failure::Install(topology::InstallError::Install(
            open::Error::Step(Error::ColumnCount {
                expected: 0,
                actual: 1
            })
        )))
    ));
    Ok(())
}

fn preserve_authorization(runtime: Runtime) -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-command-authorization", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    for (index, commands) in FORBIDDEN_COMMANDS.iter().enumerate() {
        let target = open(runtime, &dir.path().join(format!("{index}.db")))?
            .commands(commands)
            .map_err(plan::Error::Client)?;
        assert!(matches!(
            plan::Plan::<Commands>::try_new(target, try_limits()?, try_memory()?)?.try_install(
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            ),
            Err(plan::Failure::Install(
                topology::InstallError::Install(open::Error::Step(Error::Sqlite { action: Action::Prepare, code }))
            )) if code.primary() == libsqlite3_sys::SQLITE_AUTH
        ));
    }
    Ok(())
}

fn catalog_preserves_authorization(runtime: Runtime) -> Result<(), plan::Error> {
    for result in [
        plan::Plan::<Unauthorized<false>>::try_new(
            cartel_sqlite::OpenOptions::new()
                .open_memory(runtime)
                .map_err(plan::Error::Client)?,
            try_limits()?,
            try_memory()?,
        )?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        }),
        plan::Plan::<Unauthorized<true>>::try_new(
            cartel_sqlite::OpenOptions::new()
                .open_memory(runtime)
                .map_err(plan::Error::Client)?,
            try_limits()?,
            try_memory()?,
        )?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        }),
    ] {
        assert!(matches!(
            result,
            Err(plan::Failure::Install(
                topology::InstallError::Install(open::Error::Step(Error::Sqlite {
                    action: Action::Prepare,
                    code,
                }))
            )) if code.get() == libsqlite3_sys::SQLITE_AUTH
        ));
    }
    Ok(())
}
