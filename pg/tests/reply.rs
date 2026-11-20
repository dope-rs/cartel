use crate::common::topology;
use cartel_pg::api::traits::{Client as _, Query as _, Status as _, Stream as _};
use cartel_pg::{port, reply::owned};
use common::postgres::{accept, startup::Stream as _};
use dope::{
    core::{
        Backend, Errno,
        dope_sys::io::poll::{Entry, Poll as _},
    },
    manifold::timing::Throughput,
};
use dope_fiber::{BlockOn as _, abi::poll_fn};
use dope_test::{fibers, harness::peer::TcpScript};
use std::{os::fd::AsFd as _, task::Poll};
use cartel_pg::{reply, reply::server::Fields};
use common::port::Queries;
use dope_test::alloc::Tracker;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn startup_closes_live() {
    TcpScript::with(
        |socket| socket.reject(),
        |server| {
            let plan = port::Port::<Queries>::plan(
                cartel_pg::Config::new("user".into(), "password".into(), "database".into())
                    .expect("database configuration"),
                Queries::try_reply().expect("port configuration"),
            );
            topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
                let client = runtime.root_client();
                let first = runtime
                    .block_on(client.status().wait_ready())
                    .expect("startup driver")
                    .expect_err("startup rejection");
                assert!(matches!(
                    &first,
                    reply::Error::Server(error)
                        if error.fields().code().as_str() == "23505"
                            && error.fields().message() == "duplicate key"
                ));

                runtime
                    .block_on(poll_fn(|context| {
                        let mut entry =
                            [Entry::readable(server.completion().as_fd())];
                        match Backend::wait::<_, Errno>(&mut entry, 0) {
                            Ok(events) if events.count() != 0 => {
                                Poll::Ready(Ok(()))
                            }
                            Ok(_) => {
                                context.wake();
                                Poll::Pending
                            }
                            Err(error) => Poll::Ready(Err(error)),
                        }
                    }))
                    .expect("close driver")
                    .expect("peer observes EOF before application teardown");

                assert!(matches!(
                    runtime.block_on(client.status().wait_ready()),
                    Ok(Err(reply::Error::Server(error)))
                        if error.fields().code().as_str() == "23505"
                            && error.fields().message() == "duplicate key"
                ));
            });
            server
                .finish()
                .expect("startup peer execution")
                .expect("startup peer protocol");
        },
    )
    .expect("startup peer");
}

#[test]
fn catalog_mismatch() {
    use common::wire::checked::Mismatch;

    let ready = TcpScript::with(Mismatch::serve, |server| {
        let plan = port::Port::<Mismatch>::plan(
            Queries::try_config().expect("database configuration"),
            Queries::try_reply().expect("port configuration"),
        );
        let ready = topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
            let client = runtime.root_client();
            runtime
                .block_on(client.status().wait_ready())
                .expect("startup driver")
                .map_err(|error| error.try_own().ok().expect("owned rejection"))
        });
        server
            .finish()
            .expect("startup peer execution")
            .expect("startup peer protocol");
        ready
    })
    .expect("startup peer");
    assert!(matches!(
        ready,
        Err(owned::Error::Driver(cartel_pg::Error::QueryMetadata))
    ));
}

#[test]
fn request_errors() {
    for (frame, expected) in accept::REPLIES {
        let (reply, alloc) = TcpScript::with(
            move |socket| accept::reply(socket, frame),
            |server| {
                let plan = port::Port::<Queries>::plan(
                    Queries::try_config().expect("database configuration"),
                    Queries::try_reply().expect("test configuration"),
                );
                let result = topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
                    let client = runtime.root_client();
                    assert!(
                        matches!(
                            runtime.block_on(client.status().wait_ready()),
                            Ok(Ok(()))
                        ),
                        "PostgreSQL connection ready for {expected:?}"
                    );
                    let error = runtime
                        .block_on(client.command("SELECT 1"))
                        .expect("runtime")
                        .expect_err("query must be rejected");
                    Tracker::count(|| {
                        error.try_own().ok().expect("escape server reply")
                    })
                });
                server
                    .finish()
                    .expect("reply server")
                    .expect("reply protocol");
                result
            },
        )
        .expect("reply peer");
        match expected {
            Some(expected) => {
                let actual = match &reply {
                    owned::Error::Driver(cartel_pg::Error::Protocol(reason)) => Some(reason),
                    _ => None,
                }
                .expect("invalid response must preserve its protocol reason");
                match expected {
                    accept::Rejection::Protocol(reason) => assert_eq!(*actual, reason),
                    accept::Rejection::Debug(error) => assert_eq!(format!("{actual:?}"), error),
                }
                assert_eq!(alloc, 0, "driver errors must not allocate while escaping");
            }
            None => {
                assert_eq!(
                    alloc, 0,
                    "unique resident escape transfers without allocation"
                );
                let fields = Fields::from_owned(reply.as_server().expect("server ErrorResponse"));
                assert_eq!(fields.severity(), "ERROR");
                assert_eq!(fields.code().as_str(), "23505");
                assert_eq!(fields.message(), "duplicate key");
                let ctx = fields.context();
                assert_eq!(ctx.detail(), Some("row already exists"));
                assert_eq!(ctx.hint(), Some("use another key"));
                assert_eq!(ctx.position().map(|value| value.get()), Some(42));
                assert_eq!(ctx.inner_pos().map(|value| value.get()), Some(7));
                assert_eq!(ctx.inner_query(), Some("SELECT 1"));
                assert_eq!(ctx.location(), Some("constraint check"));
                assert_eq!(ctx.schema(), Some("public"));
                assert_eq!(ctx.table(), Some("users"));
                assert_eq!(ctx.column(), Some("id"));
                assert_eq!(ctx.data_type(), Some("int8"));
                assert_eq!(ctx.constraint(), Some("users_pkey"));
                assert_eq!(ctx.file(), Some("nbtinsert.c"));
                assert_eq!(ctx.line().map(|value| value.get()), Some(u32::MAX));
                assert_eq!(ctx.routine(), Some("_bt_check_unique"));
            }
        }
    }
}

#[test]
fn connection_errors_preserve_pending_diagnostics() {
    use common::wire::checked::{Database, TextQuery};

    for (detail, buffered, severity) in [
        (
            b"SFATAL\0VFATAL\0C57P01\0Mclosed\0\0".as_slice(),
            false,
            "FATAL",
        ),
        (
            b"SPANIC\0VPANIC\0CXX000\0Mclosed\0\0".as_slice(),
            true,
            "PANIC",
        ),
    ] {
        TcpScript::with(
            move |socket| accept::fatal(socket, detail, buffered),
            |server| {
                let mut caps = Queries::try_reply().expect("reply capacity").capacities();
                caps.inflight = 1;
                caps.responses.items = 1;
                let plan = port::Port::<Database>::plan(
                    Queries::try_config().expect("database configuration"),
                    port::config::Config::new(caps).expect("one retained value"),
                );
                topology::run(plan, server.addr(), (1, 1_000), Throughput, |mut runtime| {
                    let client = runtime.root_client();
                    runtime
                        .block_on(client.status().wait_ready())
                        .expect("ready driver")
                        .ok()
                        .expect("ready PostgreSQL");
                    let mut rows = client.run_stream::<TextQuery>(());
                    if buffered {
                        let row = runtime
                            .block_on(rows.next_row())
                            .expect("row driver")
                            .ok()
                            .flatten()
                            .expect("retained row");
                        fibers::TEST
                            .try_run(&mut runtime, poll_fn(|context| {
                                if client.status().is_ready() {
                                    context.wake();
                                    Poll::Pending
                                } else {
                                    Poll::Ready(())
                                }
                            }))
                            .expect("fatal removes connection while row holds reply credit");
                        assert_eq!(row.decode().expect("retained payload").name, "retained");
                    }
                    let error = runtime
                        .block_on(rows.next_row())
                        .expect("failure driver")
                        .err()
                        .expect("fatal diagnostic");
                    let reply::Error::Server(error) = error else {
                        panic!("fatal diagnostic replaced by driver failure: {:?}", error.kind());
                    };
                    assert_eq!(error.fields().severity(), severity);
                    assert_eq!(error.fields().message(), "closed");
                    assert!(!client.status().is_ready());
                });
                server.finish().expect("fatal peer").expect("fatal protocol");
            },
        )
        .expect("fatal peer startup");
    }
}
