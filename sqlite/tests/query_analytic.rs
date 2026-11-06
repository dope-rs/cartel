use crate::common::database::IntegrationDatabase;
use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, query::analytic::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;

pub mod common;

#[test]
fn arithmetic_in_aggregate() -> Result<(), plan::Error> {
    // SAFETY: no SQLite call can pass this binary's initializer before it returns.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
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
                        let s = count_widgets_arith(&mut conn, 0).await?;
                        ::core::assert_eq!(s, Some((10 + 20 + 30 + 40 + 50) * 2));
                        ::core::assert_eq!(count_widgets_arith(&mut conn, 51).await?, None);

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
    }?;
    window_functions(runtime)
}

fn window_functions(runtime: Runtime) -> Result<(), plan::Error> {
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
                        let rows = user_row_numbers(&mut conn).await?;
                        ::core::assert_eq!(rows.as_slice(), &[(1, 4), (2, 2), (3, 5), (4, 1), (5, 3)]);

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
