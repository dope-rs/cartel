use crate::common::external::runtime::Plan;
use crate::common::external::runtime::fixtures::try_suffix;
use cartel_pg::Identifier;
use cartel_pg::api::traits::Connection as _;
use cartel_pg::api::traits::{Listener as _, Notify as _, Unchecked as _};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;
use std::num::NonZero;

use dope_test::fibers;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn listen_unlisten_api() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_conns(
            NonZero::<usize>::MIN,
            |rt| {
                let client = rt.client();

                let suffix = try_suffix().expect("fixture suffix");
                let channel = ::std::format!("cartel_test_\"{suffix}");
                let guard = rt.runtime.block_on(client.listen(
                    Identifier::try_from(&mut channel.clone()).expect("channel identifier"),
                ))?;
                let guard = guard.ok().expect("LISTEN succeeds");
                ::core::assert_eq!(guard.channel(), channel);
                assert!(
                    rt.runtime.block_on(guard.unlisten())?.is_ok(),
                    "UNLISTEN succeeds"
                );
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn listen_cancel() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_conns(NonZero::<usize>::MIN, |rt| {
            let client = rt.client();
            let channel = ::std::format!(
                "cartel_cancel_open_{}",
                try_suffix().expect("fixture suffix")
            );
            rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                ::core::assert!(
                    fibers::TEST
                        .poll_once(client.listen(Identifier::try_from(&mut channel.clone()).expect("channel identifier")))
                        .await
                        .is_none()
                );
                let sql = ::std::format!("NOTIFY \"{}\"", channel.replace('"', "\"\""));
                ::core::assert!(client
                    .command(&sql)
                    .await
                    .is_ok(), "NOTIFY succeeds");
                ::core::assert!(
                    fibers::TEST
                        .poll_once(client.next())
                        .await
                        .is_none()
                );
            }))?;
            Ok(())
        }, recover, check).expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn unlisten_cancel() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_conns(
            NonZero::<usize>::MIN,
            |rt| {
                let client = rt.client();
                let channel = ::std::format!(
                    "cartel_cancel_close_{}",
                    try_suffix().expect("fixture suffix")
                );
                rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let guard = client
                    .listen(Identifier::try_from(&mut channel.clone()).expect("channel identifier"))
                    .await;
                let guard = guard.ok().expect("LISTEN succeeds");
                ::core::assert!(fibers::TEST.poll_once(guard.unlisten()).await.is_none());

                let sql = ::std::format!("NOTIFY \"{}\"", channel.replace('"', "\"\""));
                ::core::assert!(client
                    .command(&sql)
                    .await
                    .is_ok(), "NOTIFY succeeds");
                ::core::assert!(
                    fibers::TEST
                        .poll_once(client.next())
                        .await
                        .is_none()
                );
            }))?;
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn listen_notify_delivery() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_conns(
            NonZero::<usize>::MIN.saturating_add(1),
            |rt| {
                let client = rt.client();

                let suffix = try_suffix().expect("fixture suffix");
                let channel = ::std::format!("cartel_notify_{suffix}");
                let payload = "hello world";
                rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            let guard = client
                .listen(Identifier::try_from(&mut channel.clone()).expect("channel identifier"))
                .await;
            let guard = guard.ok().expect("LISTEN succeeds");
            let notifier = client;
            let escaped = payload.replace('\'', "''");
            let sql = ::std::format!("NOTIFY \"{}\", '{}'", channel.replace('"', "\"\""), escaped);
            ::core::assert!(notifier
                .command(&sql)
                .await
                .is_ok(), "NOTIFY succeeds");
            let n = guard.query().next().await;
            let n = n.ok().expect("notification arrives");
            ::core::assert_eq!(n.channel(), channel);
            ::core::assert_eq!(n.payload(), payload);
            ::core::assert!(n.pid() > 0);
            ::core::assert!(guard.unlisten().await.is_ok(), "UNLISTEN succeeds");
        }))?;
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
