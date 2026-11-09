use crate::common::topology;
use cartel_pg::api::traits::{Client as _, Query as _, Status as _, Stream as _, Unchecked as _};
use cartel_pg::{port, reply::owned};
use common::postgres::{accept, startup::Stream as _};
use dope::{
    core::{
        Backend, Errno,
        dope_sys::io::poll::{Entry, Poll as _},
        driver::{life::recovery, route::Id},
    },
    manifold::app::env::Bundle,
    manifold::timing::Throughput,
    net::tcp::Tcp,
    net::wire::Identity,
    runtime::executor::session::Completion,
};
use dope_fiber::{BlockOn as _, abi::poll_fn};
use dope_test::{
    fibers,
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use std::{os::fd::AsFd as _, task::Poll, time::Duration};
type Env = Bundle<Tcp, Identity, Throughput>;
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
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    cartel_pg::Config::new("user".into(), "password".into(), "database".into())
                        .expect("database configuration"),
                    Queries::try_reply().expect("port configuration"),
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_millis(1_024),
            )
            .expect("startup topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared
                .with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
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
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |outcome| {
                        assert!(
                            outcome.is_ok(),
                            "{:?}",
                            outcome.as_ref().err().map(|error| error.cause())
                        );
                        outcome.ok().expect("startup scope")
                    },
                )
                .expect("startup installation")
                .expect("startup session");
            assert!(reports.events().expect("native reclamation").is_empty());
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
        let prepared = topology::try_new::<_, Env>(
            port::Port::<Mismatch>::plan(
                Queries::try_config().expect("database configuration"),
                Queries::try_reply().expect("port configuration"),
            ),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_millis(1_024),
        )
        .expect("startup topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        let result = prepared
            .with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client = runtime.root_client();
                                runtime
                                    .block_on(client.status().wait_ready())
                                    .expect("startup driver")
                                    .map_err(|error| error.try_own().ok().expect("owned rejection"))
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |outcome| {
                    assert!(
                        outcome.is_ok(),
                        "{:?}",
                        outcome.as_ref().err().map(|error| error.cause())
                    );
                    outcome.ok().expect("startup scope")
                },
            )
            .expect("startup installation");
        assert!(
            result.is_ok(),
            "startup session: {:?}",
            result.as_ref().err().map(|error| error.cause())
        );
        let ready = result.ok().expect("startup session");
        assert!(reports.events().expect("native reclamation").is_empty());
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
                let prepared = topology::try_new::<_, Env>(
                    port::Port::<Queries>::plan(
                        Queries::try_config().expect("database configuration"),
                        Queries::try_reply().expect("test configuration"),
                    ),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_millis(1_024),
                )
                .expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                let result = prepared
                    .with_local(
                        |worker| {
                            worker.lend(|node, stage| {
                                node.install(stage, |host, stage| {
                                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                        let client = runtime.root_client();
                                        assert!(
                                            matches!(
                                                runtime.block_on(client.status().wait_ready()),
                                                Ok(Ok(()))
                                            ),
                                            "PostgreSQL connection ready for {expected:?}"
                                        );
                                        let error = runtime
                                            .block_on(client.unchecked().command("SELECT 1"))
                                            .expect("runtime")
                                            .expect_err("query must be rejected");
                                        Tracker::count(|| {
                                            error.try_own().ok().expect("escape server reply")
                                        })
                                    })
                                })
                            })
                        },
                        recovery::Policy::new(|event| reports.push(event), common::recover),
                        |outcome| {
                            assert!(
                                outcome.is_ok(),
                                "{:?}",
                                outcome.as_ref().err().map(|error| error.cause())
                            );
                            outcome.ok().expect("reply scope")
                        },
                    )
                    .expect("application installation completed")
                    .ok()
                    .expect("application session completed");
                assert!(
                    reports.events().expect("native reclamation").is_empty(),
                    "unexpected events for {expected:?}: {reports:?}"
                );
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
                let prepared = topology::try_new::<_, Env>(
                    port::Port::<Database>::plan(
                        Queries::try_config().expect("database configuration"),
                        port::config::Config::new(caps).expect("one retained value"),
                    ),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_secs(1),
                )
                .expect("fatal topology");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared
                    .with_local(
                        |worker| {
                            worker.lend(|node, stage| {
                                node.install(stage, |host, stage| {
                                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                        let client = runtime.root_client();
                                        runtime
                                            .block_on(client.status().wait_ready())
                                            .expect("ready driver")
                                            .ok()
                                            .expect("ready PostgreSQL");
                                        let mut rows = client.query().run_stream::<TextQuery>(());
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
                                    })
                                })
                            })
                        },
                        recovery::Policy::new(|event| reports.push(event), common::recover),
                        |outcome| outcome.ok().expect("fatal scope"),
                    )
                    .expect("fatal installation")
                    .expect("fatal session");
                assert!(reports.events().expect("native reclamation").is_empty());
                server.finish().expect("fatal peer").expect("fatal protocol");
            },
        )
        .expect("fatal peer startup");
    }
}
