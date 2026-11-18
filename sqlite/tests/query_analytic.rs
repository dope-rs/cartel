use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::analytic::*, schema};
use dope_test::fibers;

pub mod common;

#[test]
fn arithmetic_in_aggregate() -> Result<(), plan::Error> {
    // SAFETY: no SQLite call can pass this binary's initializer before it returns.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
                let s = count_widgets_arith(&mut conn, 0).await?;
                ::core::assert_eq!(s, Some((10 + 20 + 30 + 40 + 50) * 2));
                ::core::assert_eq!(count_widgets_arith(&mut conn, 51).await?, None);
                Ok(())
            }),
        )
    })?;
    window_functions(runtime)
}

fn window_functions(runtime: Runtime) -> Result<(), plan::Error> {
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
                let rows = user_row_numbers(&mut conn).await?;
                ::core::assert_eq!(rows, &[(1, 4), (2, 2), (3, 5), (4, 1), (5, 3)]);
                let rows = widget_positions(&mut conn, -1, 1).await?;
                ::core::assert_eq!(rows, &[2, 1, 2, 1, 1]);
                Ok(())
            }),
        )
    })
}
