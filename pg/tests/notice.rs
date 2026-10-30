use crate::common::postgres::{accept::Listener, notice, startup::Stream};
use cartel_pg::api::traits::{Client as _, Notify as _, Status as _, Unchecked as _};
use cartel_pg::{api, port, port::config, reply};
use common::{port::Queries, topology};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::{iter, net::SocketAddr, net::TcpListener, sync::mpsc, thread, time::Duration};

pub mod common;

#[test]
fn retains_fields_and_pid() {
    let peer = (TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))))
        .expect("the notification peer must bind an ephemeral port");
    let address = (peer.local_addr()).expect("the notification peer must expose its bound address");
    let (release, done) = mpsc::sync_channel(0);
    let server = thread::spawn(move || {
        let mut socket = Listener::try_accept(&peer, Duration::from_secs(3))
            .expect("notification peer connection");
        Stream::try_start(&mut socket, None).expect("peer handshake");
        notice::try_write(&mut socket, &notice::SEED).expect("notification frames");
        for (cases, ..) in notice::BATCHES {
            notice::try_batch(&mut socket, cases).expect("notification fence");
        }
        while done
            .recv_timeout(Duration::from_secs(3))
            .expect("notification round")
        {
            notice::try_batch(&mut socket, &notice::BUDGET).expect("budget fence");
        }
    });
    let limits = config::Capacities {
        connections: 1,
        inflight: 1,
        admission_waiters: 1,
        requests: config::Request {
            frames: 1,
            frame_bytes: 512,
        },
        responses: config::Response {
            items: 1,
            bytes: 512,
            frame_bytes: 128,
        },
        notifications: config::Notification {
            items: 3,
            bytes: 10,
        },
    };
    let limits = config::Config::new(limits);
    let limits = limits.expect("valid pg configuration");
    let (_, channel, payload) = notice::BUDGET[0];
    let attempts =
        limits.memory_bound().ingress.retained.bytes() / (channel.len() + payload.len()) + 1;
    let login = cartel_pg::Config::new(
        String::from("user"),
        String::from("password"),
        String::from("database"),
    );
    let login = login.expect("valid PostgreSQL credentials");
    let ((), events) = {
        let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Queries>::plan(login, limits),
            address,
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
                                "notification connection ready"
                            );
                            for (cases, range, dropped) in
                                iter::once((&notice::SEED[..], 0..notice::SEED.len(), 0))
                                    .chain(notice::BATCHES)
                            {
                                if dropped != 0 {
                                    assert!(matches!(
                                        runtime
                                            .block_on(client.unchecked().command(notice::COMMAND)),
                                        Ok(Ok(()))
                                    ));
                                }
                                assert_eq!(client.notify().dropped(), dropped);
                                for index in range {
                                    let (pid, channel, payload) = cases[index];
                                    let notice = runtime
                                        .block_on(client.notify().next())
                                        .expect("notification runtime")
                                        .ok()
                                        .expect(
                                            "the public notification API must deliver every frame",
                                        );
                                    assert_eq!(
                                        (notice.pid(), notice.channel(), notice.payload()),
                                        (pid, channel, payload)
                                    );
                                    assert_eq!(size_of_val(&notice), 2 * size_of::<usize>());
                                    assert_eq!(
                                        align_of::<reply::Notice<'_>>(),
                                        align_of::<usize>()
                                    );
                                }
                            }
                            let dropped = client.notify().dropped();
                            let mut held = Vec::new();
                            for _ in 0..attempts {
                                release.send(true).expect("budget round");
                                assert!(matches!(
                                    runtime.block_on(client.unchecked().command(notice::COMMAND)),
                                    Ok(Ok(()))
                                ));
                                if client.notify().dropped() != dropped {
                                    break;
                                }
                                held.push(
                                    runtime
                                        .block_on(client.notify().next())
                                        .expect("notification runtime")
                                        .ok()
                                        .expect("retained notification"),
                                );
                            }
                            assert_eq!(client.notify().dropped(), dropped + 1);
                            drop(held);
                            release.send(true).expect("budget recovery");
                            assert!(matches!(
                                runtime.block_on(client.unchecked().command(notice::COMMAND)),
                                Ok(Ok(()))
                            ));
                            let notice = runtime
                                .block_on(client.notify().next())
                                .expect("notification runtime")
                                .ok()
                                .expect("released notification budget");
                            assert_eq!(
                                (notice.pid(), notice.channel(), notice.payload()),
                                notice::BUDGET[0]
                            );
                            release.send(false).expect("notification server release");
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
    server.join().expect("notification server");
}
