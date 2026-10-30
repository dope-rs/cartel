use crate::common::plan;
use crate::{common::database::IntegrationDatabase, common::query::dispatch::user_by_id};
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, query::stream::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::{cell::Cell, pin::Pin, sync::OnceLock};

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn stream_visits_rows() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
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
                        let mut ids = Vec::new();
                        users(conn.query())
                            .visit(|user| {
                                ids.push(user.id);
                                ::core::ops::ControlFlow::Continue(())
                            })
                            .await?;
                        ::core::assert_eq!(ids, vec![1, 2, 3, 4, 5]);

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
fn stream_stop_releases() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
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
                        let mut ids = Vec::new();
                        users(conn.query())
                            .visit(|user| {
                                ids.push(user.id);
                                if ids.len() == 2 {
                                    ::core::ops::ControlFlow::Break(())
                                } else {
                                    ::core::ops::ControlFlow::Continue(())
                                }
                            })
                            .await?;
                        ::core::assert_eq!(ids, vec![1, 2]);
                        ::core::assert_eq!(user_by_id(&mut conn, 3).await?.name, "carol");

                        fibers::Yield::new().await;
                        let seen = Cell::new(0_usize);
                        let scan = conn.query()
                            .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, ())
                            .visit(|_| {
                                seen.set(seen.get() + 1);
                                ::core::ops::ControlFlow::Continue(())
                            });
                        let mut task = dope_fiber::abi::Task::new(scan);
                        assert!(fibers::TEST.poll_once(Pin::new(&mut task)).await.is_none());
                        assert!(seen.get() > 1);
                        drop(task);

                        let mut first = None;
                        conn.query()
                            .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, ())
                            .visit(|row| {
                                first = Some(row);
                                ::core::ops::ControlFlow::Break(())
                            })
                            .await?;
                        assert_eq!(first, Some(1));

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
