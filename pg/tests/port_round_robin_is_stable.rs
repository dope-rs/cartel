use cartel_pg::api::traits::{Client as _, Query as _, Status as _};
use cartel_pg::{api, port, port::config};
use common::{
    port::*,
    postgres::accept::Listener,
    postgres::startup::{self, Stream},
    topology,
};
use dope::core::dope_sys::io::poll::Poll as _;
use dope::manifold::timing::Throughput;
use dope_fiber::{BlockOn as _, abi, context::PollResult, task::batch};
use dope_test::harness::peer::AcceptScript;
use std::os::fd::AsFd as _;
use std::time::Duration;
use std::{pin::pin, task::Poll};

pub mod common;

#[test]
fn round_robin_and_shared_admission_progress() {
    AcceptScript::with(
        |listener| {
            let sockets = [(); 3].map(|()| {
                let mut socket = listener
                    .accept_within(Duration::from_secs(1))
                    .expect("peer connection");
                Stream::try_start(&mut socket, None).expect("peer handshake");
                socket
            });

            let mut entries = sockets.each_ref().map(|socket| {
                dope::core::dope_sys::io::poll::Entry::readable(socket.as_fd())
            });
            let mut choices = [0; 4];
            let complete = b"C\0\0\0\x0dSELECT 1\0Z\0\0\0\x05I";
            for step in 0..6 {
                let events = dope::core::Backend::wait::<_, dope::core::Errno>(&mut entries, 1_000)
                    .expect("round-robin readiness");
                let index = events
                    .entries()
                    .iter()
                    .position(|entry| entry.is_readable())
                    .expect("round-robin request");
                let socket = &sockets[index];
                let mut tag = [0];
                socket.read_exact(&mut tag).expect("Parse tag");
                assert_eq!(tag, [b'P']);
                let parse = startup::read(socket).expect("Parse body");
                let command = usize::from(parse[8] - b'0');
                common::postgres::read_empty(socket, b'S').expect("round-robin request");
                match step {
                    0..4 => {
                        choices[command] = index;
                        if step == 3 {
                            assert_eq!(choices[0], choices[3]);
                            assert_ne!(choices[0], choices[1]);
                            assert_ne!(choices[0], choices[2]);
                            assert_ne!(choices[1], choices[2]);
                            sockets[choices[2]].write_all(complete).expect("release Z reserve");
                        }
                    }
                    4 => {
                        assert_eq!((command, index), (4, choices[2]));
                        // Y has already attempted admission. X returns the shared entry.
                        sockets[choices[0]].write_all(complete).expect("release shared entry");
                    }
                    _ => {
                        assert_eq!(command, 5);
                        for lane in [choices[0], choices[1], index, choices[2]] {
                            sockets[lane].write_all(complete).expect("remaining replies");
                        }
                    }
                }
            }
        },
        |server| {
            let capacity = config::Capacities {
                connections: 3,
                inflight: 4,
                admission_waiters: 2,
                requests: config::Request {
                    frames: 6,
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
            {
                let plan = port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    config,
                );
                topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
                    let client: api::Client<api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>> =
                        runtime.root_client();

                    assert!(
                        matches!(
                            runtime.block_on(client.status().wait_ready()),
                            Ok(Ok(()))
                        ),
                        "all PostgreSQL connections ready"
                    );

                    let mut pool = runtime.block_on(batch::Pool::<4>::acquire())
                        .expect("batch driver").expect("batch slots");
                    let queries = batch::Join::from_map(&mut pool, [0, 1, 2, 3], |index| {
                        dope_fiber::fiber!('_ => async move {
                            let sql = ["SELECT 0", "SELECT 1", "SELECT 2", "SELECT 3"][index];
                            assert!(client.command(sql).await.is_ok());
                            if index == 2 {
                                let (waiting, marker) = abi::Join::new(
                                    client.command("SELECT 4"),
                                    client.command("SELECT 5"),
                                ).await;
                                assert!(waiting.is_ok() && marker.is_ok());
                            }
                        })
                    });
                    let mut queries = pin!(abi::Task::new(queries));
                    runtime.block_on(abi::poll_fn(|mut context| {
                        let result = context.as_mut().try_poll(queries.as_mut());
                        let work = context.as_ref().work();
                        while work.remaining() > 1 {
                            let _ = work.permit();
                        }
                        match result {
                            PollResult::Polled(Poll::Ready(output)) => Poll::Ready(output),
                            PollResult::Polled(Poll::Pending) | PollResult::Exhausted => {
                                context.as_ref().waker().wake();
                                Poll::Pending
                            }
                        }
                    })).expect("saturated driver").expect("independent query wakes");
                });
            }
            server.finish().expect("server");
        },
    )
    .expect("round-robin peer");
}
