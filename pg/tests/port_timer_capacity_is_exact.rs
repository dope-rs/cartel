use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Stream as _, Unchecked as _,
};
use cartel_pg::{api, port, port::config, protocol::encode, reply};
use common::wire::checked::{Database, TextQuery, TextRow};
use common::{port::*, topology};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, core::driver::schedule::clock::timer::Exhausted,
    manifold::app::env::Bundle, manifold::timing::Throughput, net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::{
    BlockOn as _,
    abi::poll_fn,
    context::{PollResult, raw::Live},
};
use dope_test::{
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use std::{net::SocketAddr, pin::pin, task::Poll, time::Duration};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn timer_capacity_is_exact() {
    let capacity = config::Capacities {
        connections: 1,
        inflight: 1,
        admission_waiters: 1,
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
    let config = config::Config::new(capacity);
    let config = config.expect("valid pg configuration");
    let ((), events) = {
        let prepared = topology::try_new::<_, Env>(
            port::Port::<Database>::plan(Queries::try_config().expect("database config"), config),
            SocketAddr::from(([127, 0, 0, 1], 0)),
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
                        let timer = stage.driver().driver_ref().timer();
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client: api::Client<
                                api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>,
                            > = runtime.root_client();

                            let mut alarms = Vec::new();
                            let failure = loop {
                                match timer.try_alarm() {
                                    Ok(alarm) => alarms.push(alarm),
                                    Err(error) => break error,
                                }
                            };
                            assert!(matches!(failure, Exhausted::Slots));
                            let invalid = runtime
                                .block_on(client.command("SELECT\0"))
                                .expect("invalid query driver");
                            let group = runtime
                                .block_on(client.run_group::<TextQuery, 1>([()]))
                                .expect("disabled group driver")
                                .err()
                                .expect("group diagnostics disabled");
                            let pending = client.command("SELECT 1");
                            let begin = runtime
                                .block_on(client.transaction().begin())
                                .expect("begin driver");
                            drop(pending);
                            assert!(matches!(
                                invalid,
                                Err(reply::Error::Driver(cartel_pg::Error::Request(
                                    encode::Error::StatementSqlNul { position: 6 }
                                )))
                            ));
                            assert!(matches!(
                                group.cause(),
                                Some(reply::Error::Driver(cartel_pg::Error::GroupDisabled))
                            ));
                            assert!(matches!(
                                begin,
                                Err(reply::Error::Driver(cartel_pg::Error::RequestCapacity))
                            ));
                            assert!(matches!(
                                runtime.block_on(client.command("SELECT 1")),
                                Ok(Err(reply::Error::Driver(cartel_pg::Error::Deadline(
                                    Exhausted::Slots
                                ))))
                            ));
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |outcome| {
                outcome
                    .map_err(|error| format!("{:?}", error.cause()))
                    .expect("application entry")
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

#[test]
fn typed_queries_free_timer() {
    let bytes = TextRow::wire(7, b"retained");
    TcpScript::with(
        |socket| Database::serve_timer_queries(socket, &bytes),
        |server| {
            let login = cartel_pg::Config::new("user".into(), "password".into(), "database".into())
                .expect("database credentials");
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Database>::plan(login, Database::config().expect("database config")),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("prepared PostgreSQL topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared
                .with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                let timer = stage.driver().driver_ref().timer();
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let client: api::Client<
                                        api::Context<'_, '_, '_, '_, '_, '_, '_, _, Database>,
                                    > = runtime.root_client();
                                    runtime
                                        .block_on(client.status().wait_ready())
                                        .expect("driver ready")
                                        .ok()
                                        .expect("PostgreSQL ready");
                                    let mut alarms = Vec::new();
                                    while let Ok(alarm) = timer.try_alarm() {
                                        alarms.push(alarm);
                                    }
                                    drop(alarms.pop().expect("last available timer"));

                                    let acquired = runtime
                                        .block_on(client.acquire())
                                        .expect("frame drive")
                                        .ok()
                                        .expect("request frame");
                                    assert!(matches!(timer.try_alarm(), Err(Exhausted::Slots)));
                                    let mut stream = acquired.run_stream::<TextQuery>(());
                                    drop(
                                        timer.try_alarm().expect("stream releases acquired timer"),
                                    );
                                    {
                                        let mut next = pin!(stream.next_row());
                                        assert!(matches!(
                                            runtime.block_on(poll_fn(|context| {
                                                // SAFETY: the pinned operation remains owned here, and Ready
                                                // ends this closure's polling authority.
                                                unsafe { Live::new(next.as_mut()) }.poll(context)
                                            })),
                                            Ok(Ok(Some(_)))
                                        ));
                                        let _alarm =
                                            timer.try_alarm().expect("completed next timer");
                                    }
                                    assert!(matches!(
                                        runtime.block_on(stream.next_row()),
                                        Ok(Ok(None))
                                    ));
                                    {
                                        let _alarm =
                                            timer.try_alarm().expect("completed stream timer");
                                        assert!(matches!(timer.try_alarm(), Err(Exhausted::Slots)));
                                        assert!(matches!(
                                            runtime.block_on(stream.next_row()),
                                            Ok(Ok(None))
                                        ));
                                    }

                                    let mut one = pin!(client.run_one::<TextQuery>(()));
                                    assert!(matches!(timer.try_alarm(), Err(Exhausted::Slots)));
                                    let row = runtime
                                        .block_on(poll_fn(|context| {
                                            // SAFETY: the pinned query remains owned here, and Ready
                                            // ends this closure's polling authority.
                                            unsafe { Live::new(one.as_mut()) }.poll(context)
                                        }))
                                        .expect("driver query");
                                    assert!(
                                        row.is_ok(),
                                        "required query: {:?}",
                                        row.as_ref().err().map(reply::Error::kind)
                                    );
                                    let row = row.ok().expect("required row");
                                    let alarm = timer.try_alarm().expect("completed run_one timer");
                                    let view = row.decode().expect("retained required row");
                                    assert_eq!((view.lease, view.name), (7, "retained"));
                                    drop(row);
                                    drop(alarm);

                                    let mut first = pin!(client.run_first::<TextQuery>(()));
                                    let mut all = pin!(None);
                                    assert!(matches!(timer.try_alarm(), Err(Exhausted::Slots)));
                                    runtime
                                        .block_on(poll_fn(|mut context| {
                                            // SAFETY: the pinned query remains owned here, and Ready
                                            // ends this closure's polling authority.
                                            let Poll::Ready(row) =
                                                unsafe { Live::new(first.as_mut()) }
                                                    .poll(context.as_mut())
                                            else {
                                                return Poll::Pending;
                                            };
                                            assert!(
                                                row.is_ok(),
                                                "optional query: {:?}",
                                                row.as_ref().err().map(reply::Error::kind)
                                            );
                                            let row = row
                                                .ok()
                                                .expect("optional query")
                                                .expect("optional row");
                                            let alarm = timer
                                                .try_alarm()
                                                .expect("completed run_first timer");
                                            let view = row.decode().expect("retained optional row");
                                            assert_eq!((view.lease, view.name), (7, "retained"));
                                            drop(row);
                                            drop(alarm);

                                            all.as_mut().set(Some(client.run_all::<TextQuery>(())));
                                            assert!(matches!(
                                                timer.try_alarm(),
                                                Err(Exhausted::Slots)
                                            ));
                                            assert!(context.as_ref().work().remaining() >= 2);
                                            // SAFETY: this first poll must stay Pending; the pinned
                                            // query is retained until the later terminal poll below.
                                            assert!(matches!(
                                                unsafe {
                                                    Live::new(
                                                        all.as_mut()
                                                            .as_pin_mut()
                                                            .expect("next query"),
                                                    )
                                                }
                                                .try_poll(context),
                                                PollResult::Polled(Poll::Pending)
                                            ));
                                            Poll::Ready(())
                                        }))
                                        .expect("driver query");
                                    runtime
                                        .block_on(client.next())
                                        .expect("notification driver")
                                        .ok()
                                        .expect("next request reached peer");
                                    let mut all = all.as_mut().as_pin_mut().expect("next query");
                                    let rows = runtime
                                        .block_on(poll_fn(|context| {
                                            // SAFETY: the pinned query remains owned here, and Ready
                                            // ends this closure's polling authority.
                                            unsafe { Live::new(all.as_mut()) }.poll(context)
                                        }))
                                        .expect("driver query");
                                    assert!(
                                        rows.is_ok(),
                                        "collection query: {:?}",
                                        rows.as_ref().err().map(reply::Error::kind)
                                    );
                                    let rows = rows.ok().expect("collection query");
                                    let alarm = timer.try_alarm().expect("completed run_all timer");
                                    let rows = rows.validate().expect("retained collection");
                                    let view = rows.iter().next().expect("collected row");
                                    assert_eq!((view.lease, view.name), (7, "retained"));
                                    drop(rows);
                                    drop(alarm);
                                    let failure = runtime
                                        .block_on(client.run_first::<TextQuery>(()))
                                        .expect("late failure driver")
                                        .err()
                                        .expect("late Sync failure");
                                    assert!(matches!(failure,
                                        reply::Error::Server(error)
                                            if error.fields().code().as_str() == "23505"
                                                && error.fields().message() == "duplicate"
                                    ));
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |outcome| {
                        outcome
                            .map_err(|error| format!("{:?}", error.cause()))
                            .expect("application entry")
                    },
                )
                .expect("scoped PostgreSQL")
                .expect("application scope");
            assert!(
                reports.events().expect("native reclamation").is_empty(),
                "unexpected native recovery"
            );
            server
                .finish()
                .expect("script execution")
                .expect("PostgreSQL script");
        },
    )
    .expect("script listener");
}
