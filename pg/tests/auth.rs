use crate::common::{
    postgres::accept::Listener,
    postgres::{Frame, startup},
    wire::auth::Auth,
    wire::auth::{PASSWORDS, ROUNDS, SALT},
    wire::try_mac,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{api, port, port::config};
use common::{port::Queries, topology, wire::auth::CONNECTIONS};
use dope::core::driver::life::recovery;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity, runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, reclaim::Reports};
use sha2::{Digest as _, Sha256};
use std::{io::Read as _, io::Write as _, net::TcpStream, str};
use std::{net::Ipv4Addr, net::TcpListener, sync::mpsc, thread, time::Duration};

pub mod common;

fn read(socket: &mut TcpStream, tag: Option<u8>) -> Vec<u8> {
    if let Some(expected) = tag {
        let mut actual = [0];
        socket.read_exact(&mut actual).expect("frontend tag");
        assert_eq!(actual[0], expected);
    }
    startup::read(socket).expect("frontend frame")
}

fn serve(listener: TcpListener, done: mpsc::Receiver<()>, password: &[u8], connections: usize) {
    let mut peers: Vec<_> = (0..connections)
        .map(|_| {
            let mut socket =
                Listener::try_accept(&listener, Duration::from_secs(10)).expect("peer connection");
            socket.set_nodelay(true).expect("TCP_NODELAY");
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout");
            socket
                .set_write_timeout(Some(Duration::from_secs(10)))
                .expect("write timeout");
            let startup = read(&mut socket, None);
            assert_eq!(&startup[..4], &196608_u32.to_be_bytes());
            Auth::try_auth(&mut socket, 10, b"SCRAM-SHA-256\0\0").expect("authentication message");
            socket
        })
        .collect();
    let messages: Vec<_> = peers
        .iter_mut()
        .map(|socket| {
            let first = read(socket, Some(b'p'));
            let end = first
                .iter()
                .position(|byte| *byte == 0)
                .expect("mechanism terminator");
            assert_eq!(&first[..end], b"SCRAM-SHA-256");
            let body = str::from_utf8(&first[end + 5..]).expect("client first UTF-8");
            assert_eq!(&first[end + 1..end + 5], &(body.len() as i32).to_be_bytes());
            let bare = body.strip_prefix("n,,").expect("GS2 header").to_owned();
            let nonce = bare.strip_prefix("n=,r=").expect("client nonce");
            let server = format!("r={nonce}server,s={},i={ROUNDS}", STANDARD.encode(SALT));
            (bare, server)
        })
        .collect();
    for (socket, (_, first)) in peers.iter_mut().zip(&messages) {
        Auth::try_auth(socket, 11, first.as_bytes()).expect("authentication message");
    }
    let mut salted = [0; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password, SALT, ROUNDS, &mut salted);
    let client = try_mac(&salted, b"Client Key").expect("client key");
    let stored = Sha256::digest(&client);
    let server = try_mac(&salted, b"Server Key").expect("server key");
    for (socket, (first, offer)) in peers.iter_mut().zip(&messages) {
        let body = read(socket, Some(b'p'));
        let reply = str::from_utf8(&body).expect("client final UTF-8");
        let (bare, proof) = reply.rsplit_once(",p=").expect("client proof");
        let message = format!("{first},{offer},{bare}");
        let mask = try_mac(&stored, message.as_bytes()).expect("client MAC");
        let expected: Vec<_> = client
            .iter()
            .zip(mask)
            .map(|(left, right)| left ^ right)
            .collect();
        assert_eq!(STANDARD.decode(proof).expect("proof base64"), expected);
        let reply = format!(
            "v={}",
            STANDARD.encode(try_mac(&server, message.as_bytes()).expect("server MAC"))
        );
        Auth::try_auth(socket, 12, reply.as_bytes()).expect("authentication message");
        Auth::try_auth(socket, 0, &[]).expect("authentication message");
        socket
            .write_all(&Frame::new(b'Z', b"I").encode())
            .expect("ready");
        assert!(read(socket, Some(b'S')).is_empty());
        socket
            .write_all(&Frame::new(b'Z', b"I").encode())
            .expect("prepared");
    }
    done.recv_timeout(Duration::from_secs(10))
        .expect("all authenticated connections observed");
}

#[test]
fn authenticates_peers() {
    for (index, (password, prepared)) in PASSWORDS.into_iter().enumerate() {
        let connections = if index == 0 { CONNECTIONS } else { 1 };
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("listener");
        let address = listener.local_addr().expect("listener address");
        let (release, done) = mpsc::sync_channel(0);
        let server = thread::spawn(move || serve(listener, done, prepared, connections));
        let limits = config::Config::new(config::Capacities {
            connections,
            inflight: connections,
            admission_waiters: connections,
            requests: config::Request {
                frames: connections,
                frame_bytes: 512,
            },
            responses: config::Response {
                items: connections,
                bytes: connections * 512,
                frame_bytes: 512,
            },
            notifications: config::Notification {
                items: 1,
                bytes: 128,
            },
        });
        let limits = limits.expect("valid pg configuration");
        let login = cartel_pg::Config::new("user".into(), password.into(), "database".into())
            .expect("valid PostgreSQL credentials");
        let ((), events) = {
            let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Queries>::plan(login, limits),
                address,
                Duration::from_millis(1),
                Duration::from_millis(1024),
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
                                    "every SCRAM peer becomes ready"
                                );
                                release.send(()).expect("server release");
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
        server.join().expect("SCRAM peer server");
    }
}
