use cartel_pg::{api, api::Client, port, port::config, protocol::logic};
use dope::{
    core::{
        driver::route::{KeyTag, table::conn},
        io::transfer,
    },
    manifold::{client::side, service::bindings},
};
use o3::base::time::Positive;
use std::num::NonZero;

use crate::common::port::Queries;

pub mod common;

#[test]
fn views_keep_owner_layout() {
    use api::ops;

    fn check<B: 'static, const ID: u8>() {
        use std::alloc::Layout;

        let client = Layout::new::<
            Client<'static, 'static, 'static, 'static, 'static, 'static, 'static, B, Queries, ID>,
        >();
        assert_eq!(client, Layout::new::<&'static ()>());
        assert_eq!(
            Layout::new::<
                ops::Status<
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    B,
                    Queries,
                    ID,
                >,
            >(),
            client
        );
        let query = Layout::new::<
            ops::Query<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                B,
                Queries,
                ID,
            >,
        >();
        for layout in [
            Layout::new::<
                ops::Unchecked<
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    B,
                    Queries,
                    ID,
                >,
            >(),
            Layout::new::<
                ops::Transfer<
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    B,
                    Queries,
                    ID,
                >,
            >(),
            Layout::new::<
                ops::Notify<
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    'static,
                    B,
                    Queries,
                    ID,
                >,
            >(),
        ] {
            assert_eq!(layout, query);
        }
    }
    check::<
        bindings::Bound<
            'static,
            'static,
            port::Port<'static, 'static, 'static, 'static, 'static, 'static, 'static, Queries>,
            logic::Session,
            KeyTag<0>,
            { u32::MAX as usize },
        >,
        0,
    >();
    check::<
        bindings::Bound<
            'static,
            'static,
            port::Port<'static, 'static, 'static, 'static, 'static, 'static, 'static, Queries, 7>,
            logic::Session,
            KeyTag<7>,
            { u32::MAX as usize },
        >,
        7,
    >();
}

#[test]
fn factory_keeps_config() {
    let conns = 257;
    let config = cartel_pg::Config::new(
        String::from("user"),
        String::from("password"),
        String::from("database"),
    )
    .expect("valid database configuration");
    let settings = config::Config::new(config::Capacities {
        connections: conns,
        inflight: conns * 2,
        admission_waiters: conns,
        requests: config::Request {
            frames: conns * 3,
            frame_bytes: 4096,
        },
        responses: config::Response {
            items: conns * 2,
            bytes: conns * 64,
            frame_bytes: 69,
        },
        notifications: config::Notification {
            items: 32,
            bytes: 4096,
        },
    })
    .expect("port config");
    let expected = settings.clone();
    let plan = port::Port::<Queries>::plan(config, settings);
    ::core::assert!(plan.config().capacities() == expected.capacities());
    ::core::assert!(plan.config().memory_bound() == expected.memory_bound());
}

#[test]
fn validates_capacities() {
    let valid = config::Capacities {
        connections: 2,
        inflight: 3,
        admission_waiters: 5,
        requests: config::Request {
            frames: 4,
            frame_bytes: 1024,
        },
        responses: config::Response {
            items: 8,
            bytes: 4096,
            frame_bytes: 1024,
        },
        notifications: config::Notification {
            items: 7,
            bytes: 2048,
        },
    };
    let invalid = conn::Capacity::try_from(0).err();
    assert!(invalid.is_some(), "test precondition");
    ::core::assert!(
        config::Config::new(config::Capacities {
            connections: 0,
            ..valid
        })
        .err()
            == invalid.map(|error| config::Error::Connections(config::Connection::Primary(error)))
    );
    let primary = (conn::Capacity::try_from(0x0080_0000)).expect("test precondition");
    let invalid = side::Capacity::try_new(primary).err();
    assert!(invalid.is_some(), "test precondition");
    ::core::assert!(
        config::Config::new(config::Capacities {
            connections: 0x0080_0000,
            ..valid
        })
        .err()
            == invalid.map(|error| config::Error::Connections(config::Connection::Side(error)))
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            inflight: 1,
            ..valid
        })
        .err()
            == Some(config::Error::Inflight {
                actual: 1,
                minimum: 2,
                maximum: 4,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            admission_waiters: 0,
            ..valid
        })
        .err()
            == Some(config::Error::AdmissionWaitersZero)
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            requests: config::Request {
                frames: 2,
                ..valid.requests
            },
            ..valid
        })
        .err()
            == Some(config::Error::Inflight {
                actual: 3,
                minimum: 2,
                maximum: 2,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            requests: config::Request {
                frame_bytes: 0,
                ..valid.requests
            },
            ..valid
        })
        .err()
            == Some(config::Error::RequestFrameBytes {
                actual: 0,
                maximum: transfer::MAX_BYTES,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            responses: config::Response {
                items: 2,
                ..valid.responses
            },
            ..valid
        })
        .err()
            == Some(config::Error::ResponseItems {
                actual: 2,
                minimum: 3,
                maximum: u32::MAX as usize,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            responses: config::Response {
                bytes: 0,
                ..valid.responses
            },
            ..valid
        })
        .err()
            == Some(config::Error::ResponseBytesZero)
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            responses: config::Response {
                frame_bytes: 4,
                ..valid.responses
            },
            ..valid
        })
        .err()
            == Some(config::Error::ResponseFrameBytes {
                actual: 4,
                minimum: 5,
                maximum: transfer::MAX_BYTES,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            responses: config::Response {
                frame_bytes: 4097,
                ..valid.responses
            },
            ..valid
        })
        .err()
            == Some(config::Error::ResponseFrameExceedsLaneCapacity {
                frame_bytes: 4097,
                payload_bytes: 4092,
                available_payload_bytes: 3072,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            responses: config::Response {
                frame_bytes: 3500,
                ..valid.responses
            },
            ..valid
        })
        .err()
            == Some(config::Error::ResponseFrameExceedsLaneCapacity {
                frame_bytes: 3500,
                payload_bytes: 3495,
                available_payload_bytes: 3072,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            responses: config::Response {
                frame_bytes: 4102,
                ..valid.responses
            },
            ..valid
        })
        .err()
            == Some(config::Error::ResponseFrameExceedsCapacity {
                frame_bytes: 4102,
                payload_bytes: 4097,
                available_payload_bytes: 4096,
            })
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            notifications: config::Notification {
                items: 0,
                ..valid.notifications
            },
            ..valid
        })
        .err()
            == Some(config::Error::NotificationItemsZero)
    );
    ::core::assert!(
        config::Config::new(config::Capacities {
            notifications: config::Notification {
                bytes: 0,
                ..valid.notifications
            },
            ..valid
        })
        .err()
            == Some(config::Error::NotificationBytesZero)
    );
    ::core::assert!(matches!(
        config::Config::new(config::Capacities {
            notifications: config::Notification {
                bytes: usize::MAX,
                ..valid.notifications
            },
            ..valid
        })
        .err(),
        Some(config::Error::Memory(_))
    ));
    let maximum = transfer::MAX_BYTES;
    let mut boundary = config::Capacities {
        connections: 1,
        inflight: 1,
        requests: config::Request {
            frames: 1,
            frame_bytes: maximum,
        },
        responses: config::Response {
            items: 1,
            bytes: maximum,
            frame_bytes: maximum,
        },
        ..valid
    };
    assert!(config::Config::new(boundary).is_ok());
    boundary.requests.frame_bytes += 1;
    assert_eq!(
        config::Config::new(boundary).err(),
        Some(config::Error::RequestFrameBytes {
            actual: maximum + 1,
            maximum,
        })
    );
    boundary.requests.frame_bytes = maximum;
    boundary.responses.frame_bytes += 1;
    assert_eq!(
        config::Config::new(boundary).err(),
        Some(config::Error::ResponseFrameBytes {
            actual: maximum + 1,
            minimum: 5,
            maximum,
        })
    );
    let config = config::Config::new(valid).expect("valid port config");
    assert!(config.capacities() == valid);
    ::core::assert_eq!(config.socket_limit(), 4);
    ::core::assert_eq!(config.memory_bound().request_bytes, 10240);
    ::core::assert_eq!(config.memory_bound().cancel_bytes, 32);
    ::core::assert_eq!(config.memory_bound().response_bytes, 4096);
    ::core::assert_eq!(config.memory_bound().ingress.active, 2048);
    ::core::assert_eq!(config.memory_bound().ingress.owners.get(), 17);
    ::core::assert_eq!(config.memory_bound().resident_bytes.bytes(), 14_336);
    ::core::assert_eq!(config.memory_bound().notification_bytes, 2048);
    ::core::assert_eq!(config.memory_bound().diagnostic_bytes, 1024);
    let ready = 2 * 136 + 2 * 20 + 5 * 4 + 4 + 8 + 8;
    ::core::assert_eq!(
        config.memory_bound().total_bytes.bytes(),
        14_336 + 10240 + 32 + ready + 1024
    );
    let window = Positive::from_millis::<17>();
    assert!(
        config::Config::with_window(valid, window).is_ok(),
        "timed port config"
    );

    let one = NonZero::<usize>::MIN;
    let large = config::Groups {
        active: one.saturating_add(1),
        records: one.saturating_add(5),
        bytes: one.saturating_add(3071),
    };
    let small = config::Groups {
        active: one,
        records: one,
        bytes: one,
    };
    let initial = config.clone().with_groups(large).expect("large groups");
    let replaced = initial.with_groups(small).expect("replace groups");
    let direct = config.with_groups(small).expect("small groups");
    assert_eq!(replaced.groups(), Some(small));
    assert!(replaced.capacities() == valid);
    assert!(replaced.memory_bound() == direct.memory_bound());
}
