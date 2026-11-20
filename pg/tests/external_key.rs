use crate::common::external::runtime::fixtures;
use crate::{
    common::external::own, common::external::own::Convert as _, common::external::row::Item,
    common::external::row::Widget,
};
use common::env;
use dope_fiber::BlockOn as _;
use std::env::var_os;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn composite_and_names() {
    ::core::assert_eq!(Item::__CARTEL_TABLE, "\"cartel_pg_items\"");
    ::core::assert_eq!(Widget::__CARTEL_TABLE, "\"cartel_pg_widgets\"");
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            Item::add(client, 1, 2, "hi".into())
                .await
                .try_own()?;
            Item::add(client, 1, 3, "ho".into())
                .await
                .try_own()?;
            let item = Item::by_key(client, 1, 2)
                .await
                .try_own()?
                .expect("inserted pair key exists");
            ::core::assert_eq!(item.payload, "hi");
            ::core::assert!(
                Item::by_key(client, 2, 2)
                    .await
                    .try_own()?
                    .is_none()
            );
            Ok::<(), own::Error<'_>>(())
        }))?;
        assert!(result.is_ok(), "pair keys and names must match");
        Ok(())
    });
}
