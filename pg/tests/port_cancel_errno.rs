use crate::common::postgres::{Frame, accept::Listener, read_empty, startup::Stream};
use cartel_pg::api::traits::{Begin as _, Client as _, Status as _, Transaction as _};
use cartel_pg::{api, api::tx::control::error::Kind, port};
use common::{port::*, topology};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::client::side,
    manifold::timing::Throughput, net::link::oneshot, net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::BlockOn as _;
use dope_test::harness::{
    peer,
    scenario::run::{Entry, Task as _},
};
use dope_test::{harness::GUARD, reclaim::Reports};
use std::{net::SocketAddr, net::TcpListener, time::Duration};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn cancel_errno() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    Entry(move || {
        let listener = peer::TcpAcceptor::try_adopt(listener.into()).expect("listener admission");
        let mut primary =
            Listener::accept_within(&listener, Duration::from_secs(1)).expect("peer connection");
        Stream::try_start(&mut primary, Some((0x1020_3040, 0x5060_7080)))
            .expect("backend key handshake");
        read_empty(&primary, b'S').expect("BEGIN request");
        drop(listener);
        primary
            .write_all(&Frame::new(b'C', b"BEGIN\0").encode())
            .expect("BEGIN CommandComplete");
        primary
            .write_all(&Frame::new(b'Z', b"T").encode())
            .expect("tx ReadyForQuery");
        read_empty(&primary, b'S').expect("ROLLBACK request");
        primary
            .write_all(&Frame::new(b'C', b"ROLLBACK\0").encode())
            .expect("ROLLBACK CommandComplete");
        primary
            .write_all(&Frame::new(b'Z', b"I").encode())
            .expect("idle ReadyForQuery");
        primary
            .read_closed()
            .expect("rollback and native shutdown observed");
    })
    .with(|| {
        let ((), events) = {
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    Queries::try_cancel().expect("cancel config"),
                ),
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
                                let client: api::Client<
                                    api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>,
                                > = runtime.root_client();

                                assert!(matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ));
                                let tx = runtime
                                    .block_on(client.transaction().begin())
                                    .expect("runtime")
                                    .ok()
                                    .expect("tx");
                                let token = tx.cancel_token().expect("backend key data");
                                let failure = runtime
                                    .block_on(token.cancel())
                                    .expect("cancellation runtime");
                                assert!(
                                failure.as_ref().is_err_and(
                                    |error| matches!(error.kind(), Kind::Delivery { target, cause }
                if target == token.target()
                    && matches!(cause, side::Error::Delivery(oneshot::Error::Connect(errno))
                        if errno.is_refused()))
                                ),
                                "exact cancellation target and native refusal"
                            );
                                assert!(matches!(runtime.block_on(tx.rollback()), Ok(Ok(()))));
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |outcome| outcome.map_err(common::entry_error).expect("application entry"),
            );

            let result = scoped.expect("application installation completed");
            (result.expect("application session completed"), reports)
        };
        assert!(
            events.events().expect("native reclamation").is_empty(),
            "unexpected events PostgreSQL operations: {events:?}"
        );
    })
    .expect("server start and join");
}
