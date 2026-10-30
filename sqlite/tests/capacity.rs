use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Error, OpenOptions, Statement};
use common::engine::capacity::*;
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::sync::OnceLock;

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn collect_row_limit() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings(4 << 10, 3, 1 << 10, 2, 4 << 10, 100)?;
    assert!(
        matches!(
            {
                plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
                    |driver, mut database| {
                        try_seed(&mut database, &["a".into(), "b".into(), "c".into()], driver)?;
                        assert!(matches!(
                            fibers::TEST
                                .try_run(driver, all_entries(&mut database))
                                .map_err(plan::Error::Run)?,
                            Err(Error::CollectRows {
                                required: 3,
                                available: 2
                            })
                        ));
                        Ok(())
                    },
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                )
            },
            Ok(Ok(()))
        ),
        "SQLite capacity scope failed"
    );
    Ok(())
}

#[test]
fn collect_text_storage() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings(4 << 10, 3, 1_023, 3, 1_023, 100)?;
    assert!(
        matches!(
            {
                plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
                    |driver, mut database| {
                        try_seed(&mut database, &["a".repeat(600), "b".repeat(600)], driver)?;
                        assert!(matches!(
                            fibers::TEST
                                .try_run(driver, all_entries(&mut database))
                                .map_err(plan::Error::Run)?,
                            Err(Error::CollectValueBytes {
                                required: 600,
                                available: 399
                            })
                        ));
                        Ok(())
                    },
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                )
            },
            Ok(Ok(()))
        ),
        "SQLite capacity scope failed"
    );
    Ok(())
}

#[test]
fn scan_row_limit() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings(4 << 10, 2, 1 << 10, 2, 4 << 10, 100)?;
    assert!(
        matches!(
            {
                plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
                    |driver, mut database| {
                        try_seed(&mut database, &["a".into(), "b".into(), "c".into()], driver)?;
                        let scan = scan_entries(&mut database);
                        assert!(matches!(
                            fibers::TEST
                                .try_run(
                                    driver,
                                    scan.visit(|_| ::core::ops::ControlFlow::Continue(()))
                                )
                                .map_err(plan::Error::Run)?,
                            Err(Error::ScanRows { capacity: 2 })
                        ));
                        Ok(())
                    },
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                )
            },
            Ok(Ok(()))
        ),
        "SQLite capacity scope failed"
    );
    Ok(())
}

#[test]
fn parameter_limits() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (mut limits, memory) = try_settings(64, 2, 1 << 10, 2, 4 << 10, 100)?;
    limits.parameters.items = 2;
    assert!(
        matches!(
            {
                plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
                    |driver, mut database| {
                        try_seed(&mut database, &["small".into()], driver)?;
                        assert!(matches!(
                            fibers::TEST
                                .try_run(
                                    driver,
                                    database
                                        .query()
                                        .run_no_rows(CapacityDatabase::INSERT_ENTRY, (0_i64, ()),),
                                )
                                .map_err(plan::Error::Run)?,
                            Err(Error::ParameterCount {
                                expected: 2,
                                actual: 1
                            })
                        ));
                        assert!(matches!(
                            fibers::TEST
                                .try_run(
                                    driver,
                                    database.query().run_no_rows(
                                        CapacityDatabase::INSERT_ENTRY,
                                        (0_i64, ("unused", (0_i64, ()))),
                                    ),
                                )
                                .map_err(plan::Error::Run)?,
                            Err(Error::ParameterItems {
                                required: 3,
                                available: 2
                            })
                        ));
                        let name = "x".repeat(256);
                        assert!(matches!(
                            fibers::TEST
                                .try_run(driver, entries_named(&mut database, &name))
                                .map_err(plan::Error::Run)?,
                            Err(Error::ParameterBytes {
                                required: 256,
                                available: 64
                            })
                        ));
                        Ok(())
                    },
                    Policy::new(assert_reclaimed, assert_retirement),
                    |result| result.expect("SQLite scope"),
                )
            },
            Ok(Ok(()))
        ),
        "SQLite capacity scope failed"
    );
    let target = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?;
    plan::Plan::<NativeParams>::try_new(target, limits, memory)?
        .try_run(
            |driver, mut database| {
                let stmt = Statement::<NativeParams, NativeParams>::of();
                assert!(matches!(
                    fibers::TEST
                        .try_run(driver, database.query().run_one::<i64, _, _>(stmt, ()))
                        .map_err(plan::Error::Run)?,
                    Err(Error::ParameterCount {
                        expected: 1,
                        actual: 0
                    })
                ));
                assert!(matches!(
                    fibers::TEST
                        .try_run(
                            driver,
                            database
                                .query()
                                .run_one::<i64, _, _>(stmt, (1_i64, (2_i64, (3_i64, ())))),
                        )
                        .map_err(plan::Error::Run)?,
                    Err(Error::ParameterItems {
                        required: 3,
                        available: 2
                    })
                ));
                let value = fibers::TEST
                    .try_run(
                        driver,
                        database.query().run_one::<i64, _, _>(stmt, (7_i64, ())),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert_eq!(value, 7);
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
        .expect("SQLite capacity scope failed")?;
    Ok(())
}
