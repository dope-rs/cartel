use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Action, Error, open};
use common::engine::capacity::*;
use dope_core::driver::life::recovery::Policy;
use dope_runtime::topology;
use dope_test::fibers;
use std::mem;
use std::sync::OnceLock;

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn integer_error_is_exact() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings(64, 2, 1 << 10, 2, 4 << 10, 100)?;
    {
        plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
            |driver, mut database| {
                let error = (fibers::TEST
                    .try_run(
                        driver,
                        database
                            .query()
                            .run_no_rows(CapacityDatabase::INSERT_ENTRY, (u64::MAX, ("u64", ()))),
                    )
                    .map_err(plan::Error::Run)?)
                .expect_err("maximum u64 fit SQLite INTEGER");
                assert!(matches!(error, Error::ParameterU64 { value: u64::MAX }));

                let error = (fibers::TEST
                    .try_run(
                        driver,
                        database.query().run_no_rows(
                            CapacityDatabase::INSERT_ENTRY,
                            (usize::MAX, ("usize", ())),
                        ),
                    )
                    .map_err(plan::Error::Run)?)
                .expect_err("maximum usize fit SQLite INTEGER");
                assert!(matches!(error, Error::ParameterUsize { value: usize::MAX }));

                let error = (fibers::TEST
                    .try_run(
                        driver,
                        database
                            .query()
                            .run_one::<i32, _, _>(CapacityDatabase::WIDE_INTEGER, ()),
                    )
                    .map_err(plan::Error::Run)?)
                .expect_err("wide SQLite integer fit i32");
                assert!(matches!(
                    error,
                    Error::ColumnI32 {
                        column: 0,
                        value: 2_147_483_648,
                    }
                ));
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
    }
    .expect("SQLite capacity scope failed")?;
    Ok(())
}

#[test]
fn vm_work_is_interrupted() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings(4 << 10, 2, 1 << 10, 2, 4 << 10, 1)?;
    {
        plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
            |driver, mut database| {
                let result = fibers::TEST.try_run(
                    driver,
                    database
                        .query()
                        .run_one::<i64, _, _>(CapacityDatabase::RECURSIVE_SUM, ()),
                );
                assert!(matches!(
                    result,
                    Ok(Err(Error::WorkLimit { action: Action::Step, code }))
                        if code.primary() == libsqlite3_sys::SQLITE_INTERRUPT
                ));
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
    }
    .expect("SQLite capacity scope failed")?;
    Ok(())
}

#[test]
fn row_layout_and_payload() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let name = "abc";
    let exact = mem::size_of::<EntryRef<'_, String>>() + mem::size_of::<i64>() + name.len();
    let (limits, memory) = try_settings(4 << 10, 1, exact, 1, 4 << 10, 100)?;
    {
        plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
            |driver, mut database| {
                let found = fibers::TEST
                    .try_run(
                        driver,
                        database
                            .query()
                            .run_one::<Entry, _, _>(CapacityDatabase::EXACT_ROW, ()),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert_eq!(found.id, 7);
                assert_eq!(found.name, name);
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
    }
    .expect("SQLite capacity scope failed")?;
    let (limits, memory) = try_settings(4 << 10, 1, exact - 1, 1, 4 << 10, 100)?;
    {
        plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
            |driver, mut database| {
                assert!(matches!(
                    fibers::TEST
                        .try_run(
                            driver,
                            database
                                .query()
                                .run_one::<Entry, _, _>(CapacityDatabase::EXACT_ROW, ()),
                        )
                        .map_err(plan::Error::Run)?,
                    Err(Error::RowBytes {
                        required: 3,
                        available: 2
                    })
                ));
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
    }
    .expect("SQLite capacity scope failed")?;
    Ok(())
}

#[test]
fn validate_shape_once() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings(4 << 10, 1, 1 << 10, 1, 4 << 10, 1)?;
    {
        plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
            |driver, mut database| {
                let error = (fibers::TEST
                    .try_run(
                        driver,
                        database
                            .query()
                            .run_one::<(i64, i64), _, _>(CapacityDatabase::RECURSIVE_SUM, ()),
                    )
                    .map_err(plan::Error::Run)?)
                .expect_err("invalid result shape was accepted");
                assert!(matches!(
                    error,
                    Error::ColumnCount {
                        expected: 2,
                        actual: 1
                    }
                ));
                fibers::TEST
                    .try_run(
                        driver,
                        database
                            .query()
                            .run_no_rows(CapacityDatabase::ADD_COLUMN, ()),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert!(matches!(
                    fibers::TEST
                        .try_run(
                            driver,
                            database
                                .query()
                                .run_first::<Entry, _, _>(CapacityDatabase::ALL_COLUMNS, ()),
                        )
                        .map_err(plan::Error::Run)?,
                    Err(Error::ColumnCount {
                        expected: 2,
                        actual: 3
                    })
                ));
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
    }
    .expect("SQLite capacity scope failed")?;
    Ok(())
}

#[test]
fn catalog_reservation() {
    let (_, memory) = try_settings(128, 4, 256, 2, 512, 10).expect("SQLite capacity settings");
    let reserved = (memory.reservation::<CapacityDatabase>()).expect("catalog reserved overflowed");
    assert_eq!(reserved.sqlite_bytes, 4 << 20);
    assert_eq!(reserved.result_bytes, 512);
    assert_eq!(reserved.lookaside_bytes, 128 * 64);
    assert!(reserved.statements.slots_bytes > 0);
    assert_eq!(
        reserved.backing_bytes,
        reserved.control_bytes
            + reserved.sqlite_bytes
            + reserved.result_bytes
            + reserved.statements.slots_bytes
            + reserved.lookaside_bytes
    );
    assert_eq!(
        reserved.targets.retained,
        reserved.targets.statements + reserved.targets.pager
    );
    assert!(reserved.targets.retained <= reserved.sqlite_capacity);
}

#[test]
fn prepare_failure_retires() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, mut memory) = try_settings(128, 4, 256, 2, 512, 10)?;
    memory.statement_bytes = 4 << 10;
    let result = plan::Plan::<CapacityDatabase>::try_new(open(runtime)?, limits, memory)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        });
    assert!(matches!(
        result,
        Err(plan::Failure::Install(
            topology::InstallError::Install(open::Error::Step(
                Error::StatementBytes {
                    required,
                    available,
                }
            ))
        )) if required > available && available == 4 << 10
    ));
    Ok(())
}
