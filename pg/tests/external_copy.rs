use cartel_pg::api::traits::{Client as _, Notify as _, Query as _, Status as _, Unchecked as _};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::{BlockOn as _, abi::poll_fn, context::raw::Live};
use dope_test::harness::peer;
use std::env::var_os;
use std::{num::NonZero, pin::pin, task::Poll, thread, time::Duration};

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
fn copy_out_done_ends_data() {
    for extra in [None, Some(b'c'), Some(b'd')] {
        peer::AcceptScript::with(
            move |listener| common::postgres::accept::copy_out_done(listener, extra),
            |server| {
                Session::<Queries>::new(
                    server.addr(),
                    Queries::try_config().expect("database config"),
                    NonZero::<usize>::MIN,
                    "public".to_owned(),
                    false,
                )
                .try_run(
                    |rt| {
                        let client = rt.client();
                        let mut copy = client.copy().copy_out("COPY items TO STDOUT");
                        let next = rt.runtime.block_on(copy.next_chunk())?;
                        match extra {
                            None => assert!(matches!(next, Ok(None))),
                            Some(tag) => assert!(matches!(
                                next,
                                Err(reply::Error::Driver(
                                    cartel_pg::Error::UnexpectedReadyMessage(actual)
                                )) if actual == tag
                            )),
                        }
                        Ok(())
                    },
                    recover,
                    check,
                )
                .expect("PostgreSQL runtime");
                server.finish().expect("COPY peer").expect("COPY protocol");
            },
        )
        .expect("COPY listener");
    }
}

#[test]
fn finish_with_full_request_pool() {
    peer::AcceptScript::with(common::postgres::accept::copy_finish, |server| {
        Session::<Queries>::new(
            server.addr(),
            Queries::try_config().expect("database config"),
            NonZero::<usize>::MIN,
            "public".to_owned(),
            false,
        )
        .try_run(
            |rt| {
                let client = rt.client();
                assert!(rt.runtime.block_on(client.status().wait_ready())?.is_ok());
                let timer = rt.runtime.block_on(poll_fn(|mut context| {
                    Poll::Ready(context.as_mut().driver().driver_ref().timer())
                }))?;
                let mut alarms = Vec::new();
                while let Ok(alarm) = timer.try_alarm() {
                    alarms.push(alarm);
                }
                drop(alarms.pop().expect("last available timer"));
                let acquired = rt
                    .runtime
                    .block_on(client.query().acquire())?
                    .ok()
                    .expect("request frame");
                let mut copy = acquired.copy_stream("COPY items FROM STDIN");
                {
                    let mut write = pin!(copy.write(&[]));
                    assert!(
                        rt.runtime
                            .block_on(poll_fn(|context| {
                                // SAFETY: the pinned operation remains owned here, and Ready
                                // ends this closure's polling authority.
                                unsafe { Live::new(write.as_mut()) }.poll(context)
                            }))?
                            .is_ok()
                    );
                    let _alarm = timer.try_alarm().expect("completed COPY write timer");
                }
                drop(alarms);
                let notice = rt
                    .runtime
                    .block_on(client.notify().next())?
                    .ok()
                    .expect("COPY admission marker");
                assert_eq!((notice.channel(), notice.payload()), ("copy", "ready"));
                drop(notice);
                // Session admits sixteen request frames per connection.
                let held =
                    std::array::from_fn::<_, 16, _>(|_| client.unchecked().command("SELECT 1"));
                assert!(matches!(
                    rt.runtime
                        .block_on(client.unchecked().command("SELECT 1"))?,
                    Err(reply::Error::Driver(cartel_pg::Error::RequestCapacity))
                ));
                {
                    let mut finish = pin!(copy.finish());
                    let mut alarms = Vec::new();
                    while let Ok(alarm) = timer.try_alarm() {
                        alarms.push(alarm);
                    }
                    assert!(
                        rt.runtime
                            .block_on(poll_fn(|context| {
                                // SAFETY: the pinned operation remains owned here, and Ready
                                // ends this closure's polling authority.
                                unsafe { Live::new(finish.as_mut()) }.poll(context)
                            }))?
                            .is_ok()
                    );
                    let _alarm = timer.try_alarm().expect("completed COPY finish timer");
                }
                let [reuse, held @ ..] = held;
                assert!(rt.runtime.block_on(reuse)?.is_ok());
                drop(held);
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
        server.finish().expect("COPY peer").expect("COPY protocol");
    })
    .expect("COPY listener");
}

#[test]
fn rejected_copy_recovers() {
    peer::AcceptScript::with(
        |listener| listener.copy_recover(),
        |server| {
            Session::<Queries>::new(
                server.addr(),
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
                        let mut copy =
                            traits::Client::copy(client).copy_stream("COPY missing FROM STDIN");
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
            server
                .finish()
                .expect("COPY peer")
                .expect("COPY recovery peer");
        },
    )
    .expect("COPY listener");
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
