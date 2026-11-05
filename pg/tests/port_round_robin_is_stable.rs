use cartel_pg::api::traits::{Client as _, Status as _, Unchecked as _};
use cartel_pg::{api, port, port::config, protocol::PickPolicy};
use common::{
    port::*, postgres::Frame, postgres::accept::Listener, postgres::startup::Stream, topology,
};
use dope::core::dope_sys::io::poll::Poll as _;
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::BlockOn as _;
use dope_test::harness::peer;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::os::fd::AsFd as _;
use std::{
    net::SocketAddr, net::TcpListener, sync::mpsc, sync::mpsc::Receiver, thread, time::Duration,
};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn round_robin_is_stable() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let (server, selected) = round_robin(listener);
    let capacity = config::Capacities {
        connections: 2,
        inflight: 4,
        admission_waiters: 2,
        requests: config::Request {
            frames: 4,
            frame_bytes: 512,
        },
        responses: config::Response {
            items: 4,
            bytes: 512,
            frame_bytes: 128,
        },
        notifications: config::Notification {
            items: 1,
            bytes: 128,
        },
    };
    let config = config::Config::new(capacity);
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
                            let client: api::Client<'_, '_, '_, '_, '_, '_, '_, _, _> =
                                runtime.root_client();

                            assert!(
                                matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ),
                                "both PostgreSQL connections ready"
                            );
                            client.status().set_policy(PickPolicy::RoundRobin);

                            let mut choices = [0; 4];
                            for choice in &mut choices {
                                assert!(
                                    matches!(
                                        runtime.block_on(client.unchecked().command("SELECT 1")),
                                        Ok(Ok(()))
                                    ),
                                    "round-robin command"
                                );
                                *choice = selected
                                    .recv_timeout(Duration::from_secs(1))
                                    .expect("selected connection");
                            }
                            assert_ne!(choices[0], choices[1]);
                            assert_eq!(choices[2], choices[0]);
                            assert_eq!(choices[3], choices[1]);
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
    server.join().expect("server");
}

pub(crate) fn round_robin(listener: TcpListener) -> (thread::JoinHandle<()>, Receiver<usize>) {
    let (report, picks) = mpsc::channel();
    let server = thread::spawn(move || {
        let listener = peer::TcpAcceptor::try_adopt(listener.into()).expect("listener admission");
        let mut sockets = Vec::with_capacity(2);
        for _ in 0..2 {
            let mut socket = Listener::accept_within(&listener, Duration::from_secs(1))
                .expect("peer connection");
            Stream::try_start(&mut socket, None).expect("peer handshake");
            sockets.push(socket);
        }

        let mut entries = [
            dope::core::dope_sys::io::poll::Entry::readable(sockets[0].as_fd()),
            dope::core::dope_sys::io::poll::Entry::readable(sockets[1].as_fd()),
        ];
        for _ in 0..4 {
            let events = dope::core::Backend::wait::<_, dope::core::Errno>(&mut entries, 1_000)
                .expect("round-robin readiness");
            let index = events
                .entries()
                .iter()
                .position(|entry| entry.is_readable())
                .expect("round-robin request");
            let socket = &sockets[index];
            common::postgres::read_empty(socket, b'S').expect("round-robin request");
            report.send(index).expect("round-robin choice receiver");
            socket
                .write_all(&Frame::new(b'C', b"SELECT 1\0").encode())
                .expect("round-robin CommandComplete");
            socket
                .write_all(&Frame::new(b'Z', b"I").encode())
                .expect("round-robin ReadyForQuery");
        }
    });
    (server, picks)
}
