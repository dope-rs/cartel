use std::mem::{align_of, size_of};

use cartel_redis::{Capacities, Config, ConfigError, Connect, MAX_FRAME_CAPACITY};

#[test]
fn connect_is_topology_only() {
    assert_eq!(size_of::<Connect<usize>>(), size_of::<usize>());
    assert_eq!(align_of::<Connect<usize>>(), align_of::<usize>());
}

#[test]
fn config_exposes_every_capacity() {
    let config = Config::new(Capacities {
        connection: 2,
        waiters: 3,
        inflight: 4,
        request_entries: 5,
        request_bytes: 6,
        response_bytes: 8,
        response_values: 9,
        max_frame_bytes: 8,
    })
    .expect("config");
    assert_eq!(config.connection_capacity(), 2);
    assert_eq!(config.waiter_capacity(), 3);
    assert_eq!(config.inflight_capacity(), 4);
    assert_eq!(config.request_capacity(), 5);
    assert_eq!(config.request_byte_capacity(), 6);
    assert_eq!(config.response_byte_capacity(), 8);
    assert_eq!(config.response_value_capacity(), 9);
    assert_eq!(config.max_frame_capacity(), 8);
}

#[test]
fn config_rejects_invalid_capacity() {
    assert_eq!(
        Config::new(Capacities {
            connection: 0,
            waiters: 1,
            inflight: 1,
            request_entries: 1,
            request_bytes: 1,
            response_bytes: 1,
            response_values: 1,
            max_frame_bytes: 1,
        }),
        Err(ConfigError::ZeroConnectionCapacity)
    );
    assert_eq!(
        Config::new(Capacities {
            connection: 1,
            waiters: 1,
            inflight: 1,
            request_entries: 1,
            request_bytes: 1,
            response_bytes: MAX_FRAME_CAPACITY + 1,
            response_values: 1,
            max_frame_bytes: MAX_FRAME_CAPACITY + 1,
        }),
        Err(ConfigError::MaxFrameCapacityExceeded)
    );
    assert_eq!(
        Config::new(Capacities {
            connection: 2,
            waiters: 2,
            inflight: 2,
            request_entries: 1,
            request_bytes: 1,
            response_bytes: 1,
            response_values: 2,
            max_frame_bytes: 1,
        }),
        Err(ConfigError::RequestBelowConnectionCapacity)
    );
}
