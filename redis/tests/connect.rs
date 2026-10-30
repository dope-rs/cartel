use cartel_redis::{
    client,
    client::{
        settings,
        settings::{Builder, Bulk},
    },
};
use dope_fiber::wait;
use o3::base::time::Positive;

#[test]
fn client_is_pointer_sized() {
    use std::mem::size_of;

    assert_eq!(
        size_of::<client::Redis<'static, 'static, 'static, 'static, 'static, ()>>(),
        size_of::<&'static ()>()
    );
}

#[test]
fn capacities_match() {
    let cap = settings::Capacities {
        connections: 2,
        admission_waiters: 3,
        inflight: 4,
        request_frame_bytes: 6,
        response_bytes: 8,
        response_values: 9,
        response_frame_bytes: 6,
        response_frame_values: 7,
    };
    let window = Positive::from_millis::<17>();
    let config = Builder::new(cap).window(window).build();
    let config = config.expect("valid Redis capacities must build");
    assert_eq!(config.connections(), 2);
    assert!(config.waiters() == wait::Capacity::new(3));
    assert_eq!(config.inflight(), 4);
    assert_eq!(config.max_frame().get(), 6);
    assert_eq!(config.frame_values().get(), 7);
    assert_eq!(config.memory_bound().request_bytes, 24);
    assert_eq!(
        config.memory_bound().ingress.owners.get(),
        cap.connections + cap.inflight
    );
}

#[test]
fn parser_tape_bound() {
    let cap = settings::Capacities {
        connections: 2,
        admission_waiters: 2,
        inflight: 2,
        request_frame_bytes: 1,
        response_bytes: 10,
        response_values: 9,
        response_frame_bytes: 8,
        response_frame_values: 7,
    };
    let config = Builder::new(cap).parser_lanes(1).build();
    let config = config.expect("one parser lane must be valid");
    assert_eq!(config.memory_bound().parse_tape_bytes, 16 * 16);
    assert!(matches!(
        client::settings::Builder::new(cap).parser_lanes(0).build(),
        Err(client::settings::Error::Zero(
            client::settings::Field::ParserLane,
        ))
    ));
    assert!(matches!(
        client::settings::Builder::new(cap).parser_lanes(3).build(),
        Err(client::settings::Error::Above {
            field: client::settings::Field::ParserLane,
            actual: 3,
            maximum: 2,
        })
    ));
}

#[test]
fn bulk_memory_is_separate() {
    let cap = settings::Capacities {
        connections: 2,
        admission_waiters: 3,
        inflight: 4,
        request_frame_bytes: 6,
        response_bytes: 8,
        response_values: 9,
        response_frame_bytes: 6,
        response_frame_values: 7,
    };
    let config = Builder::new(cap).bulk(Bulk::new(2, 100)).build();
    let config = config.expect("valid Redis bulk capacities must build");
    assert_eq!(config.memory_bound().request_bytes, 224);

    assert!(matches!(
        client::settings::Builder::new(cap)
            .bulk(client::settings::Bulk::new(1, 6))
            .build(),
        Err(client::settings::Error::NotGreater {
            field: client::settings::Field::BulkFrame,
            actual: 6,
            base: 6,
        })
    ));
    assert!(matches!(
        client::settings::Builder::new(cap)
            .bulk(client::settings::Bulk::new(
                1,
                client::settings::MAX_FRAME_CAPACITY + 1,
            ))
            .build(),
        Err(client::settings::Error::Above {
            field: client::settings::Field::BulkFrame,
            actual,
            maximum: client::settings::MAX_FRAME_CAPACITY,
        }) if actual == client::settings::MAX_FRAME_CAPACITY + 1
    ));
    assert!(matches!(
        client::settings::Builder::new(cap)
            .bulk(client::settings::Bulk::new(usize::MAX, 100))
            .build(),
        Err(client::settings::Error::BulkPool(
            o3::buffer::pool::LayoutError::SlotsTooLarge,
        ))
    ));
}

#[test]
fn egress_credit_bounds() {
    let overflow = Builder::new(settings::Capacities {
        connections: 1,
        admission_waiters: 1,
        inflight: 257,
        request_frame_bytes: settings::MAX_FRAME_CAPACITY,
        response_bytes: 1024,
        response_values: 257,
        response_frame_bytes: 1024,
        response_frame_values: 257,
    })
    .build();
    let cause = overflow.err();
    assert!(
        matches!(
            cause,
            Some(client::settings::Error::Above {
                field: client::settings::Field::EgressResident,
                ..
            })
        ),
        "{cause:?}"
    );
}

#[test]
fn invalid_capacity_fails() {
    assert!(matches!(
        client::settings::Config::new(client::settings::Capacities {
            connections: 1,
            admission_waiters: usize::MAX / 4 + 2,
            inflight: 1,
            request_frame_bytes: 1,
            response_bytes: 1,
            response_values: 1,
            response_frame_bytes: 1,
            response_frame_values: 1,
        }),
        Err(client::settings::Error::Above {
            field: client::settings::Field::Waiter,
            actual,
            maximum,
        }) if actual == usize::MAX / 4 + 2 && maximum == usize::MAX / 4 + 1
    ));
    assert!(matches!(
        client::settings::Config::new(client::settings::Capacities {
            connections: 0,
            admission_waiters: 1,
            inflight: 1,
            request_frame_bytes: 1,
            response_bytes: 1,
            response_values: 1,
            response_frame_bytes: 1,
            response_frame_values: 1,
        }),
        Err(client::settings::Error::Zero(
            client::settings::Field::Connection,
        ))
    ));
    assert!(matches!(
        client::settings::Config::new(client::settings::Capacities {
            connections: 1,
            admission_waiters: 1,
            inflight: 1,
            request_frame_bytes: 1,
            response_bytes: client::settings::MAX_FRAME_CAPACITY + 1,
            response_values: 1,
            response_frame_bytes: client::settings::MAX_FRAME_CAPACITY + 1,
            response_frame_values: 1,
        }),
        Err(client::settings::Error::Above {
            field: client::settings::Field::ResponseFrame,
            actual,
            maximum: client::settings::MAX_FRAME_CAPACITY,
        }) if actual == client::settings::MAX_FRAME_CAPACITY + 1
    ));
}
