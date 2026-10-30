use crate::common::plan;
use crate::{common::database::IntegrationDatabase, common::schema::Item, common::schema::Widget};
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, query::item::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::sync::OnceLock;

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn composite_key_and_names() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    ::core::assert_eq!(Item::__CARTEL_TABLE, "\"items\"");
    ::core::assert_eq!(Widget::__CARTEL_TABLE, "\"widgets\"");
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST.try_run(driver, dope_fiber::fiber!('_ => async move {

        let mut conn = local;
        insert(&mut conn, 1, 2, "hi").await?;
        insert(&mut conn, 1, 3, "ho").await?;
        let it =
            (by_key(&mut conn, 1, 2).await?).expect("inserted pair key was missing");
        ::core::assert_eq!(it.payload, "hi");
        ::core::assert!(by_key(&mut conn, 2, 2).await?.is_none());

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}
