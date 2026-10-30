use crate::common::external::runtime::Plan;
use crate::{
    common::external::own, common::external::own::Convert as _, common::external::row::Item,
    common::external::row::Widget,
};
use cartel_pg::api::traits::Client as _;
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn composite_and_names() {
    ::core::assert_eq!(Item::__CARTEL_TABLE, "\"cartel_pg_items\"");
    ::core::assert_eq!(Widget::__CARTEL_TABLE, "\"cartel_pg_widgets\"");
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    Item::add(client.query(), 1, 2, "hi".into())
                        .await
                        .try_own()?;
                    Item::add(client.query(), 1, 3, "ho".into())
                        .await
                        .try_own()?;
                    let item = Item::by_key(client.query(), 1, 2)
                        .await
                        .try_own()?
                        .expect("inserted pair key exists");
                    ::core::assert_eq!(item.payload, "hi");
                    ::core::assert!(
                        Item::by_key(client.query(), 2, 2)
                            .await
                            .try_own()?
                            .is_none()
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "pair keys and names must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
