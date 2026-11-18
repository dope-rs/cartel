use crate::common::plan;
use crate::{common::schema::Item, common::schema::Widget};
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::item::*, schema};
use dope_test::fibers;

pub mod common;

#[test]
fn composite_key_and_names() -> Result<(), plan::Error> {
    // SAFETY: only this test calls SQLite in this binary. Only Cartel uses the
    // installed runtime, retaining Core handles and allocations on their worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    ::core::assert_eq!(Item::__CARTEL_TABLE, "\"items\"");
    ::core::assert_eq!(Widget::__CARTEL_TABLE, "\"widgets\"");
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
                insert(&mut conn, 1, 2, "hi").await?;
                insert(&mut conn, 1, 3, "ho").await?;
                let it =
                    (by_key(&mut conn, 1, 2).await?).expect("inserted pair key was missing");
                ::core::assert_eq!(it.payload, "hi");
                ::core::assert!(by_key(&mut conn, 2, 2).await?.is_none());
                Ok(())
            }),
        )
    })
}
