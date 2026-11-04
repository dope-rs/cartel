use cartel_pg::api::traits::{Client as _, Notify as _, Status as _, Unchecked as _};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use dope_test::harness::peer;
use std::env::var_os;
use std::{
    net::{Ipv4Addr, TcpListener},
    num::NonZero,
    thread,
    time::Duration,
};

use crate::common::external::runtime::{Plan, Session};
use crate::common::{port::Queries, postgres::accept::Listener as _};
use cartel_pg::{
    api::traits::{
        self,
        copy::{Sink as _, Stream as _, Transfer as _},
    },
    reply,
};
use o3::base::time::Positive;

use crate::common::external::{own, own::Convert as _, row::Copy, user::Lookup};

pub mod common;

#[test]
fn rejected_copy_recovers() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let peer = thread::spawn(move || {
        peer::TcpAcceptor::try_adopt(listener.into())
            .expect("listener admission")
            .copy_recover()
    });
    Session::<Queries>::new(
        address,
        Queries::try_config().expect("database config"),
        NonZero::<usize>::MIN,
        "public".to_owned(),
        false,
    )
    .try_run(
        |rt| {
            let client = rt.client();
            assert!(rt.runtime.block_on(client.status().wait_ready())?.is_ok());
            for finish in [false, true] {
                let mut copy = traits::Client::copy(client).copy_stream("COPY missing FROM STDIN");
                assert!(
                    rt.runtime.block_on(copy.write(&[]))?.is_ok(),
                    "initial COPY admission"
                );
                let marker = rt
                    .runtime
                    .block_on(client.notify().next())?
                    .ok()
                    .expect("server rejection marker");
                assert_eq!(marker.channel(), "copy");
                let rejected = if finish {
                    rt.runtime.block_on(copy.finish())?
                } else {
                    rt.runtime.block_on(copy.write(&[]))?
                };
                assert_eq!(
                    rejected
                        .as_ref()
                        .err()
                        .and_then(|error| error.as_server())
                        .map(|error| error.fields().code().as_str()),
                    Some("42P01"),
                    "COPY retains the exact server rejection",
                );
                drop(rejected);
                assert!(
                    rt.runtime
                        .block_on(client.unchecked().command("SELECT 1"))?
                        .is_ok(),
                    "a rejected COPY recovers through a replacement connection",
                );
            }
            Ok(())
        },
        recover,
        check,
    )
    .expect("PostgreSQL runtime");
    peer.join().expect("peer").expect("COPY recovery peer");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn copy_in_binary() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    client
                        .unchecked()
                        .command("DELETE FROM cartel_pg_copy")
                        .await
                        .try_own()?;

                    let mut buf = Vec::new();
                    buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
                    buf.extend_from_slice(&0i32.to_be_bytes());
                    buf.extend_from_slice(&0i32.to_be_bytes());
                    for (id, label) in [(1i64, "alpha"), (2, "beta"), (3, "gamma")] {
                        buf.extend_from_slice(&2i16.to_be_bytes());
                        buf.extend_from_slice(&8i32.to_be_bytes());
                        buf.extend_from_slice(&id.to_be_bytes());
                        buf.extend_from_slice(&(label.len() as i32).to_be_bytes());
                        buf.extend_from_slice(label.as_bytes());
                    }
                    buf.extend_from_slice(&(-1i16).to_be_bytes());

                    client
                        .copy()
                        .copy_in("COPY cartel_pg_copy (id, label) FROM STDIN BINARY", &buf)
                        .await
                        .map_err(own::Error::Query)?;

                    let n = Copy::count_all(client.query())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(n, 3);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "binary COPY IN must preserve rows");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn copy_out_deadline() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_window(
            Positive::from_millis::<100>(),
            |rt| {
                let client = rt.client();
                let mut stream = client
                    .copy()
                    .copy_out("COPY cartel_pg_users TO STDOUT BINARY");
                let first = rt.runtime.block_on(stream.next_chunk())?;
                ::core::assert!(matches!(first, Ok(Some(_))), "first chunk must arrive");
                thread::sleep(Duration::from_millis(150));
                let failure = rt.runtime.block_on(stream.next_chunk())?;
                ::core::assert!(
                    failure.as_ref().is_err_and(|error| ::core::matches!(
                        error,
                        reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))
                            if timeout.phase() == cartel_pg::RequestPhase::Response
                    )),
                    "COPY OUT deadline must not rearm per chunk"
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
fn copy_in_deadline() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_window(
            Positive::from_millis::<100>(),
            |rt| {
                let client = rt.client();
                let mut copy = client
                    .copy()
                    .copy_stream("COPY cartel_pg_copy (id, label) FROM STDIN BINARY");
                let mut header = Vec::new();
                header.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
                header.extend_from_slice(&0i32.to_be_bytes());
                header.extend_from_slice(&0i32.to_be_bytes());
                assert!(
                    rt.runtime.block_on(copy.write(&header))?.is_ok(),
                    "COPY IN header"
                );
                thread::sleep(Duration::from_millis(150));
                let failure = rt.runtime.block_on(copy.finish())?;
                ::core::assert!(failure.as_ref().is_err_and(|error| ::core::matches!(
                    error,
                    reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))
                        if timeout.phase() == cartel_pg::RequestPhase::Response
                )));
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn stream_chunks_at_limit() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_inflight(
            NonZero::<usize>::MIN,
            NonZero::<usize>::MIN,
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    client
                        .unchecked()
                        .command("DELETE FROM cartel_pg_copy")
                        .await
                        .map_err(own::Error::Query)?;
                    let mut guard = client
                        .copy()
                        .copy_stream("COPY cartel_pg_copy (id, label) FROM STDIN BINARY");
                    let mut header = Vec::new();
                    header.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
                    header.extend_from_slice(&0i32.to_be_bytes());
                    header.extend_from_slice(&0i32.to_be_bytes());
                    guard.write(&header).await.map_err(own::Error::Query)?;
                    for (id, label) in [(50i64, "p"), (51, "q")] {
                        let mut row = Vec::new();
                        row.extend_from_slice(&2i16.to_be_bytes());
                        row.extend_from_slice(&8i32.to_be_bytes());
                        row.extend_from_slice(&id.to_be_bytes());
                        row.extend_from_slice(&(label.len() as i32).to_be_bytes());
                        row.extend_from_slice(label.as_bytes());
                        guard.write(&row).await.map_err(own::Error::Query)?;
                    }
                    let mut trailer = Vec::new();
                    trailer.extend_from_slice(&(-1i16).to_be_bytes());
                    guard.write(&trailer).await.map_err(own::Error::Query)?;
                    guard.finish().await.map_err(own::Error::Query)?;
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "streamed COPY IN must complete");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn cancel_reconnects() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_inflight(
            NonZero::<usize>::MIN,
            NonZero::<usize>::MIN,
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    client
                        .copy()
                        .copy_stream("COPY cartel_pg_copy (id, label) FROM STDIN BINARY")
                        .cancel();
                    ::core::assert_eq!(
                        Lookup::by_id(client.query(), 1)
                            .await
                            .try_own()?
                            .name,
                        "alice"
                    );

                    let mut guard = client
                        .copy()
                        .copy_stream("COPY cartel_pg_copy (id, label) FROM STDIN BINARY");
                    let mut header = Vec::new();
                    header.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
                    header.extend_from_slice(&0i32.to_be_bytes());
                    header.extend_from_slice(&0i32.to_be_bytes());
                    guard.write(&header).await.map_err(own::Error::Query)?;
                    guard.cancel();

                    ::core::assert_eq!(
                        Lookup::by_id(client.query(), 1)
                            .await
                            .try_own()?
                            .name,
                        "alice"
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "cancelled COPY must release the connection");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
