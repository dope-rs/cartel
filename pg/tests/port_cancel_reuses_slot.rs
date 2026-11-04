use crate::common::postgres::{Frame, accept::Listener, read_empty, startup::Stream};
use cartel_pg::api::traits::{Begin as _, Client as _, Status as _, Transaction as _};
use cartel_pg::{api, api::tx::control::error::Kind, port};
use common::{port::*, topology};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::{BlockOn as _, abi::Join};
use dope_test::fibers;
use dope_test::harness::peer;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::{net::SocketAddr, net::TcpListener, thread, time::Duration};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn cancel_reuses_slot() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let server = queued_cancel_reuse(listener);
    let config = Queries::try_cancel().expect("cancel config");
    let ((), events) = {
        let prepared = topology::Prepare::try_new::<Env>(
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
                                "PostgreSQL connection ready"
                            );
                            let tx = runtime
                                .block_on(client.transaction().begin())
                                .expect("runtime")
                                .ok()
                                .expect("tx");
                            let token = tx.cancel_token().expect("backend key data");
                            assert!(
                                matches!(
                                    runtime.block_on(fibers::TEST.poll_once(token.cancel())),
                                    Ok(None)
                                ),
                                "first cancel must be dropped from the queued state"
                            );
                            let (first, second) = runtime
                                .block_on(Join::new(token.cancel(), token.cancel()))
                                .expect("cancellation runtime");
                            assert!(matches!(first, Ok(())), "reused cancel slot");
                            assert!(second.as_ref().is_err_and(|error| matches!(error.kind(), Kind::Capacity { target, slots } if target == token.target() && slots.get() == 1)), "the active cancellation retains the only buffer");
                            assert!(
                                matches!(runtime.block_on(tx.rollback()), Ok(Ok(()))),
                                "rollback"
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
    server.join().expect("server");
}

fn queued_cancel_reuse(listener: TcpListener) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = peer::TcpAcceptor::try_adopt(listener.into()).expect("listener admission");
        const PID: i32 = 0x1020_3040;
        const SECRET: i32 = 0x5060_7080;

        let mut primary =
            Listener::accept_within(&listener, Duration::from_secs(1)).expect("peer connection");
        Stream::try_start(&mut primary, Some((PID, SECRET))).expect("backend key handshake");

        read_empty(&primary, b'S').expect("BEGIN request");
        primary
            .write_all(&Frame::new(b'C', b"BEGIN\0").encode())
            .expect("BEGIN CommandComplete");
        primary
            .write_all(&Frame::new(b'Z', b"T").encode())
            .expect("tx ReadyForQuery");

        {
            let cancel = Listener::accept_within(&listener, Duration::from_secs(1))
                .expect("peer connection");
            let mut frame = [0_u8; 16];
            cancel
                .read_exact(&mut frame)
                .expect("PostgreSQL CancelRequest");
            assert_eq!(
                u32::from_be_bytes(
                    frame[..4]
                        .try_into()
                        .expect("CancelRequest length field is four bytes"),
                ),
                16
            );
            assert_eq!(
                u32::from_be_bytes(
                    frame[4..8]
                        .try_into()
                        .expect("CancelRequest code field is four bytes"),
                ),
                80_877_102
            );
            assert_eq!(
                i32::from_be_bytes(
                    frame[8..12]
                        .try_into()
                        .expect("CancelRequest process id field is four bytes"),
                ),
                PID
            );
            assert_eq!(
                i32::from_be_bytes(
                    frame[12..]
                        .try_into()
                        .expect("CancelRequest secret field is four bytes"),
                ),
                SECRET
            );
        }

        read_empty(&primary, b'S').expect("ROLLBACK request");
        primary
            .write_all(&Frame::new(b'C', b"ROLLBACK\0").encode())
            .expect("ROLLBACK CommandComplete");
        primary
            .write_all(&Frame::new(b'Z', b"I").encode())
            .expect("idle ReadyForQuery");

        thread::sleep(Duration::from_millis(25));
        assert!(
            listener
                .try_accept()
                .expect("cancel admission probe")
                .is_none(),
            "dropped queued cancel must not open an auxiliary socket"
        );
    })
}
