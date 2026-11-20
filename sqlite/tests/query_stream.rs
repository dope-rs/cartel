use crate::common::plan;
use crate::{common::database::IntegrationDatabase, common::query::dispatch::user_by_id};
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::stream::*, schema};
use dope_test::fibers;
use std::{cell::Cell, pin::Pin};

pub mod common;

#[test]
fn query_stream() -> Result<(), plan::Error> {
    // SAFETY: only this test calls SQLite in this binary. Only Cartel uses the
    // installed runtime, retaining Core handles and allocations on their worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    stream_visits_rows(runtime)?;
    stream_stop_releases(runtime)
}

fn stream_visits_rows(runtime: Runtime) -> Result<(), plan::Error> {
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
                let mut ids = Vec::new();
                users(conn.query(), |user| {
                    ids.push(user.0);
                    if user.0 == 1 {
                        assert_eq!(user.1, "alice");
                    }
                    ::core::ops::ControlFlow::Continue(())
                })
                .await?;
                ::core::assert_eq!(ids, vec![1, 2, 3, 4, 5]);
                Ok(())
            }),
        )
    })
}

fn stream_stop_releases(runtime: Runtime) -> Result<(), plan::Error> {
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
                let mut ids = Vec::new();
                users(conn.query(), |user| {
                    ids.push(user.0);
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
                    .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, (), |_| {
                        seen.set(seen.get() + 1);
                        ::core::ops::ControlFlow::Continue(())
                    });
                let mut task = dope_fiber::abi::Task::new(scan);
                assert!(fibers::TEST.poll_once(Pin::new(&mut task)).await.is_none());
                assert!(seen.get() > 1);
                drop(task);

                let mut first = None;
                conn.query()
                    .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, (), |row| {
                        first = Some(row);
                        ::core::ops::ControlFlow::Break(())
                    })
                    .await?;
                assert_eq!(first, Some(1));
                Ok(())
            }),
        )
    })
}
