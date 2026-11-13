use cartel_pg::{
    api::traits::{Client as _, Notify as _, Query as _, Status as _},
    port,
    protocol::Reason,
    reply,
    schema::TypedQuery as _,
};
use dope::{
    core::driver::{life::recovery, route::Id},
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{
    BlockOn as _,
    abi::{Join, Task},
};
use dope_test::{
    fibers,
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use std::{pin::pin, time::Duration};

use common::{
    port::Queries,
    required::{Case, Database, Item, ItemRename},
    topology,
};

pub mod common;

#[test]
fn required_update() {
    assert_eq!(ItemRename::N_RESULT_COLS, 0);
    assert_eq!(
        ItemRename::SQL,
        "UPDATE \"items\" AS q0_0 SET \"name\" = $2 WHERE q0_0.\"id\" = $1"
    );
    for case in [Case::Settle, Case::Malformed, Case::Empty] {
        TcpScript::with(move |listener| case.serve(listener), |server| {
            let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Database>::plan(
                    Queries::try_config().expect("credentials"),
                    Queries::try_cancel().expect("capacity"),
                ),
                server.addr(), Duration::from_millis(1), Duration::from_secs(1),
            ).expect("topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(|worker| worker.lend(|node, stage| {
                node.install(stage, |host, stage| {
                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                        let client = runtime.root_client();
                        runtime.block_on(client.status().wait_ready()).expect("ready driver").unwrap_or_else(|error| panic!("{:?}", error.kind()));
                        if matches!(case, Case::Settle) {
                            for late in [false, true] {
                                let input = String::from("next");
                                let query = client.acquire();
                                let query = runtime.block_on(query).expect("admission driver").unwrap_or_else(|error| panic!("{:?}", error.kind()));
                                let first = Item::rename(query, 1, &input);
                                drop(input);
                                let mut first = pin!(Task::new(first));
                                assert!(runtime.block_on(fibers::TEST.poll_once(first.as_mut())).expect("poll driver").is_none());
                                let marker = runtime.block_on(client.next()).expect("marker driver").unwrap_or_else(|error| panic!("{:?}", error.kind()));
                                assert_eq!(marker.channel(), "required");
                                drop(marker);
                                assert!(runtime.block_on(fibers::TEST.poll_once(first.as_mut())).expect("pending driver").is_none());
                                let (first, second) = runtime.block_on(Join::new(first.as_mut(), Item::rename(client, 2, "after"))).expect("pipeline driver");
                                if late {
                                    assert!(matches!(first, Err(reply::Error::Server(error)) if error.fields().code().as_str() == "23505"));
                                } else {
                                    assert!(matches!(first, Err(reply::Error::Driver(cartel_pg::Error::NotFound))));
                                }
                                assert!(second.is_ok());
                            }
                            {
                                let mut detached = pin!(Task::new(Item::rename(client, 1, "cancel")));
                                assert!(runtime.block_on(fibers::TEST.poll_once(detached.as_mut())).expect("detach driver").is_none());
                                runtime.block_on(client.next()).expect("marker driver").unwrap_or_else(|error| panic!("{:?}", error.kind()));
                            }
                            assert!(runtime.block_on(Item::rename(client, 2, "after cancel")).expect("reused driver").is_ok());
                            assert!(runtime.block_on(client.run_no_rows::<ItemRename>((1, "optional"))).expect("ordinary driver").is_ok());
                        } else {
                            let result = runtime.block_on(Item::rename(client, 1, "bad")).expect("failure driver");
                            match case {
                                Case::Malformed => assert!(matches!(result, Err(reply::Error::Driver(cartel_pg::Error::Protocol(Reason::ControlPayload { tag: b'C', actual }))) if actual.get() == 10)),
                                Case::Empty => assert!(matches!(result, Err(reply::Error::Driver(cartel_pg::Error::UnexpectedReadyMessage(b'I'))))),
                                Case::Settle => unreachable!("case"),
                            }
                        }
                    })
                })
            }), recovery::Policy::new(|event| reports.push(event), common::recover),
            |outcome| outcome.map_err(common::entry_error).expect("entry")).expect("install").expect("session");
            assert!(reports.events().expect("native reclamation").is_empty());
            server.finish().expect("peer lifecycle").expect("peer protocol");
        }).expect("peer");
    }
}
