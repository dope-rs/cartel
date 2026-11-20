use crate::common::postgres::{notice, startup::Stream};
use cartel_pg::api::traits::{Client as _, Notify as _, Query as _, Status as _};
use cartel_pg::{api, port, port::config};
use common::{port::Queries, topology};
use dope::manifold::timing::Throughput;
use dope_fiber::BlockOn as _;
use dope_test::harness::peer::AcceptScript;
use std::iter;

pub mod common;

#[test]
fn retains_fields_and_pid() {
    AcceptScript::with(
        |peer| {
            let mut socket = peer.accept().expect("notification peer connection");
            Stream::try_start(&mut socket, None).expect("peer handshake");
            notice::try_write(&socket, &notice::SEED).expect("notification frames");
            for (cases, ..) in notice::BATCHES {
                notice::try_batch(&socket, cases).expect("notification fence");
            }
            while socket.peek(&mut [0]).expect("notification round") != 0 {
                notice::try_batch(&socket, &notice::BUDGET).expect("budget fence");
            }
        },
        |server| {
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
            let attempts = limits.memory_bound().ingress.retained.bytes()
                / (channel.len() + payload.len())
                + 1;
            let login = Queries::try_config().expect("valid PostgreSQL credentials");
            {
                let plan = port::Port::<Queries>::plan(login, limits);
                topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
                    let client: api::Client<
                        api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>,
                    > = runtime.root_client();

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
                                runtime.block_on(client.command(notice::COMMAND)),
                                Ok(Ok(()))
                            ));
                        }
                        assert_eq!(client.dropped(), dropped);
                        for index in range {
                            let (pid, channel, payload) = cases[index];
                            let notice = runtime
                        .block_on(client.next())
                        .expect("notification runtime")
                        .ok()
                        .expect(
                            "the public notification API must deliver every frame",
                        );
                            assert_eq!(
                                (notice.pid(), notice.channel(), notice.payload()),
                                (pid, channel, payload)
                            );
                        }
                    }
                    let dropped = client.dropped();
                    let mut held = Vec::new();
                    for _ in 0..attempts {
                        assert!(matches!(
                            runtime.block_on(client.command(notice::COMMAND)),
                            Ok(Ok(()))
                        ));
                        if client.dropped() != dropped {
                            break;
                        }
                        held.push(
                            runtime
                                .block_on(client.next())
                                .expect("notification runtime")
                                .ok()
                                .expect("retained notification"),
                        );
                    }
                    assert_eq!(client.dropped(), dropped + 1);
                    drop(held);
                    assert!(matches!(
                        runtime.block_on(client.command(notice::COMMAND)),
                        Ok(Ok(()))
                    ));
                    let notice = runtime
                        .block_on(client.next())
                        .expect("notification runtime")
                        .ok()
                        .expect("released notification budget");
                    assert_eq!(
                        (notice.pid(), notice.channel(), notice.payload()),
                        notice::BUDGET[0]
                    );
                });
            }
            server.finish().expect("notification server");
        },
    )
    .expect("spawn notification peer");
}
