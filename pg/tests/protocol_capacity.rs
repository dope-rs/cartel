use cartel_pg::{
    api::{
        self,
        traits::{Client as _, Query as _, Status as _},
    },
    port,
    protocol::{Codec, encode, frame::Head},
    reply,
};
use common::{
    port::Queries,
    postgres::Frame,
    topology,
    wire::{
        capacity,
        checked::{Bind, BindCase, Database},
    },
};
use dope::{
    core::driver::{life::recovery, route::Id},
    manifold::{app::env::Bundle, client::session::codec::Parse, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use std::{cell::Cell, convert::Infallible, num::NonZero, time::Duration};

pub mod common;

#[test]
fn row_borrows_ingress() {
    let config = capacity::try_frame(32);
    let config = config.expect("valid test capacity");
    let codec = Codec::new(&config);
    let ingress =
        (Frame::new(b'D', b"payload").try_ingress(4096)).expect("fixture ingress allocation");
    capacity::try_parse(&codec, &ingress, |parsed| {
        let (head, consumed) = match parsed {
            Parse::Item { head, consumed } => Some((head, consumed)),
            Parse::More(_) | Parse::Blocked | Parse::CapacityExhausted => None,
        }
        .expect("complete frame must parse");
        let message = match head {
            Head::Message(message) => Ok(message),
            Head::Fatal(error) => Err(error),
        }
        .expect("valid row must remain a message");
        assert_eq!(consumed.count().get() as usize, 12);
        assert_eq!(message.payload.as_slice(), b"payload");
        assert_eq!(
            message.payload.as_slice().as_ptr(),
            ingress.as_slice().as_ptr().wrapping_add(5),
            "the codec must not retain or copy a complete frame"
        );
    })
    .expect("PostgreSQL source proof");
}

#[test]
fn header_rejects_oversize() {
    let config = capacity::try_frame(12);
    let config = config.expect("valid test capacity");
    let codec = Codec::new(&config);
    let ingress =
        (Frame::new(b'D', b"12345678").try_ingress(4096)).expect("fixture ingress allocation");
    capacity::try_parse(&codec, &ingress, |parsed| {
        let (head, consumed) = match parsed {
            Parse::Item { head, consumed } => Some((head, consumed)),
            Parse::More(_) | Parse::Blocked | Parse::CapacityExhausted => None,
        }
        .expect("complete frame must parse");
        assert!(matches!(head, Head::Fatal(_)));
        assert_eq!(consumed.count().get() as usize, 5);
    })
    .expect("PostgreSQL source proof");
}

#[test]
fn bind_failures_recover() {
    TcpScript::with(Bind::serve, |server| {
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Bind>::plan(
                Queries::try_config().expect("database credentials"),
                Database::config().expect("one request frame"),
            ),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("PostgreSQL topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client: api::Client<
                                api::Context<'_, '_, '_, '_, '_, '_, '_, _, Bind>,
                            > = runtime.root_client();
                            let ready = runtime
                                .block_on(client.status().wait_ready())
                                .expect("ready driver");
                            assert!(
                                ready.is_ok(),
                                "{:?}",
                                ready.as_ref().err().map(reply::Error::kind)
                            );
                            let observed = [
                                BindCase::MissingText,
                                BindCase::MissingArray,
                                BindCase::Retry,
                            ]
                            .map(|case| {
                                let errors = [Cell::new(None), Cell::new(None)];
                                let result = runtime
                                    .block_on(client.run_no_rows::<Bind>((case, &errors)))
                                    .expect("bind driver");
                                (result, errors.map(Cell::into_inner))
                            });
                            assert!(
                                matches!(
                                    observed[0].0,
                                    Err(reply::Error::Driver(cartel_pg::Error::Request(
                                        encode::Error::ParameterValueCount {
                                            expected: 3,
                                            actual: 1
                                        }
                                    )))
                                ) && matches!(
                                    observed[1].0,
                                    Err(reply::Error::Driver(cartel_pg::Error::Request(
                                        encode::Error::ParameterValueCount {
                                            expected: 3,
                                            actual: 2
                                        }
                                    )))
                                ) && observed[2].0.is_ok(),
                                "{:?}",
                                observed.each_ref().map(|(result, errors)| (
                                    result.as_ref().err().map(reply::Error::kind),
                                    errors
                                )),
                            );
                            let required = NonZero::new(512).expect("nonzero fixture length");
                            let text = encode::Error::Capacity {
                                required,
                                capacity: 483,
                            };
                            let array = encode::Error::Capacity {
                                required,
                                capacity: 445,
                            };
                            assert_eq!(
                                observed.each_ref().map(|(_, errors)| *errors),
                                [
                                    [Some(text), None],
                                    [None, Some(array)],
                                    [Some(text), Some(array)],
                                ]
                            );
                        })
                    })
                })
            },
            recovery::Policy::new(
                |event| reports.push(event),
                |failure: recovery::Recovery<'_, '_>| -> Infallible { failure.abort() },
            ),
            |outcome| {
                outcome
                    .as_ref()
                    .map_err(|error| error.cause())
                    .expect("application entry")
                    .as_ref()
                    .expect("PostgreSQL installation")
                    .as_ref()
                    .expect("PostgreSQL scope");
            },
        );
        let wire = server
            .finish()
            .expect("script execution")
            .expect("bind peer");
        assert_eq!(wire, [Bind::BODY]);
        assert!(reports.events().expect("native reclamation").is_empty());
    })
    .expect("script listener");
}
