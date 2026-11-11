use cartel_pg::api::traits::{Client as _, Query as _, Unchecked as _};
use dope::core::driver::life::recovery;
use std::{
    net::{SocketAddr, TcpListener},
    pin::pin,
    time::Duration,
};

use cartel_pg::{api, port, port::config, reply};
use common::{port::*, postgres::startup::Stream as _, topology};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{
    BlockOn as _,
    abi::{Task, poll_fn},
    context::raw::Live,
};
use dope_test::{
    fibers,
    harness::{GUARD, peer::AcceptScript},
    reclaim::Reports,
};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn full_request_pool_allows_startup() {
    AcceptScript::with(
        |listener| {
            let mut socket = listener.accept()?;
            socket.try_start(None)?;
            socket.recover()
        },
        |server| {
            let mut capacity = Queries::try_cancel().expect("PG capacity").capacities();
            capacity.inflight = 1;
            capacity.requests.frames = 1;
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    config::Config::new(capacity).expect("single request frame"),
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("PG topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client = runtime.root_client();
                                let query = client.unchecked().command("SELECT 1");
                                let result = fibers::TEST
                                    .try_run(&mut runtime, query)
                                    .expect("admission driver");
                                assert!(
                                    result.is_ok(),
                                    "startup with full request pool: {:?}",
                                    result.as_ref().err().map(reply::Error::kind)
                                );
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
            );
            assert!(reports.events().expect("native reclamation").is_empty());
            server
                .finish()
                .expect("PG peer worker")
                .expect("startup and query wire");
        },
    )
    .expect("PG peer");
}

#[test]
fn admission_uses_window() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let capacity = config::Capacities {
        connections: 1,
        inflight: 1,
        admission_waiters: 2,
        requests: config::Request {
            frames: 1,
            frame_bytes: 128,
        },
        responses: config::Response {
            items: 1,
            bytes: 128,
            frame_bytes: 128,
        },
        notifications: config::Notification {
            items: 1,
            bytes: 128,
        },
    };
    let config = config::Config::with_window(capacity, Positive::from_millis::<5>());
    let config = config.expect("valid pg configuration");
    let ((), events) = {
        let prepared = topology::try_new::<_, Env>(
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
            addr,
            Duration::from_millis(1),
            Duration::from_millis(1_024),
        )
        .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        let scoped = prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {

                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client: api::Client<'_, '_, '_, '_, '_, '_, '_, _, _> = runtime.root_client();

                let mut held = Some(client.unchecked().command("SELECT 1"));
                let mut waiting = pin!(client.query().acquire());
                let acquired = runtime.block_on(poll_fn(|context| {
                    // SAFETY: waiting stays pinned; Ready ends polling authority.
                    let poll = unsafe { Live::new(waiting.as_mut()) }.poll(context);
                    if held.is_some() {
                        assert!(poll.is_pending());
                        drop(held.take());
                    }
                    poll
                })).expect("frame drive").ok().expect("released frame wakes acquisition");
                fibers::TEST.try_pause(&mut runtime, Duration::from_millis(10)).expect("original deadline fires");
                let mut query = pin!(Task::new(acquired.command("SELECT 1")));
                assert!(
                    matches!(
                        runtime.block_on(fibers::TEST.poll_once(query.as_mut())),
                        Ok(Some(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout)))))
                            if timeout.phase() == cartel_pg::RequestPhase::Admission
                    ),
                    "request admission must time out"
                );

                            })
                        })
                    })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
|outcome| {
    assert!(outcome.is_ok(), "{:?}", outcome.as_ref().err().map(|error| error.cause()));
    outcome.ok().expect("application entry")
},
        );

        let result = scoped.expect("application installation completed");
        (result.expect("application session completed"), reports)
    };
    assert!(
        events.events().expect("native reclamation").is_empty(),
        "unexpected events PostgreSQL operations: {events:?}"
    );
}
