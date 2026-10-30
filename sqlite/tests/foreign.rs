use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, OpenOptions, database, open};
use common::engine::foreign::*;
use dope_core::driver::life::recovery::Policy;
use dope_runtime::topology;
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use libsqlite3_sys::SQLITE_CONSTRAINT_FOREIGNKEY;
use std::{env, sync::OnceLock};

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

fn assert_relations(db: database::Target<'_>) -> Result<(), plan::Error> {
    let result = plan::Plan::<Relations>::try_new(db, try_limits()?, try_memory()?)?.try_run(
        |driver, mut conn| {
            fibers::TEST
                .try_run(
                    driver,
                    conn.query()
                        .run_no_rows(Relations::INSERT_PARENT, (7_i64, ())),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            fibers::TEST
                .try_run(
                    driver,
                    conn.query()
                        .run_no_rows(Relations::INSERT_CHILD, (1_i64, (7_i64, ()))),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            let result = fibers::TEST.try_run(
                driver,
                conn.query()
                    .run_no_rows(Relations::INSERT_CHILD, (2_i64, (8_i64, ()))),
            );
            assert!(matches!(result,
                Ok(Err(Error::Sqlite { action: Action::Step, code }))
                    if code.get() == SQLITE_CONSTRAINT_FOREIGNKEY));
            let children = fibers::TEST
                .try_run(
                    driver,
                    conn.query()
                        .run_one::<i64, _, _>(Relations::COUNT_CHILDREN, ()),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(children, 1);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("foreign key scope failed")
}

#[test]
fn orphans_are_rejected() -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let target = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?
        .commands(RELATIONS_COMMANDS)
        .map_err(plan::Error::Client)?;
    assert_relations(target)?;

    let dir = Directory::try_with(&base, "cartel-sqlite-foreign-connections", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("relations.db");
    assert_relations(
        file_database(runtime, &path)?
            .commands(RELATIONS_COMMANDS)
            .map_err(plan::Error::Client)?,
    )?;
    let result = plan::Plan::<Relations>::try_new(
        file_database(runtime, &path)?,
        try_limits()?,
        try_memory()?,
    )?
    .try_run(
        |driver, mut conn| {
            let result = fibers::TEST.try_run(
                driver,
                conn.query()
                    .run_no_rows(Relations::INSERT_CHILD, (3_i64, (8_i64, ()))),
            );
            assert!(matches!(result,
                Ok(Err(Error::Sqlite { action: Action::Step, code }))
                    if code.get() == SQLITE_CONSTRAINT_FOREIGNKEY));
            let children = fibers::TEST
                .try_run(
                    driver,
                    conn.query()
                        .run_one::<i64, _, _>(Relations::COUNT_CHILDREN, ()),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(children, 1);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("reopened foreign key scope failed")
}

#[test]
fn enforce_foreign_keys() -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(&base, "cartel-sqlite-foreign-commands", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("relations.db");
    let target = file_database(runtime, &path)?
        .commands(BROKEN_COMMANDS)
        .map_err(plan::Error::Client)?;
    assert!(matches!(
        plan::Plan::<Relations>::try_new(target, try_limits()?, try_memory()?)?.try_install(
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        ),
        Err(plan::Failure::Install(
            topology::InstallError::Install(open::Error::Step(
                Error::Sqlite { action: Action::Step, code }
            ))
        )) if code.get() == SQLITE_CONSTRAINT_FOREIGNKEY
    ));
    assert_eq!(
        plan::Plan::<Relations>::try_new(
            file_database(runtime, &path)?,
            try_limits()?,
            try_memory()?
        )?
        .try_count(
            Relations::COUNT_CHILDREN,
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
        .expect("SQLite count scope")?,
        0
    );
    Ok(())
}
