use std::cell::Cell;
use std::mem::{align_of, size_of};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use cartel_redis::{Capacities, Config, ConfigError, Connect, MAX_FRAME_CAPACITY};
use dope::driver;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::Executor;
use dope::runtime::profile::Throughput;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use dope_net::wire::send::{Plain, Prepared, Storage, Vectored};
use dope_net::wire::{ReadyOpen, Reclaim, RuntimeLimits, Wire};

struct CoreLocalConfig {
    initialized: Rc<Cell<usize>>,
}

struct CoreLocalWire(Identity);

impl Wire for CoreLocalWire {
    type InitConfig = CoreLocalConfig;
    type RuntimeContext = ();
    type Open<'a> = ReadyOpen<Self>;
    type Recv<'a> = <Identity as Wire>::Recv<'a>;
    type SendStorage = <Identity as Wire>::SendStorage;

    const RECLAIM: Reclaim = Identity::RECLAIM;
    const RAW_RECV: bool = Identity::RAW_RECV;

    fn runtime_context(
        _: RuntimeLimits,
        config: Self::InitConfig,
    ) -> std::io::Result<Self::RuntimeContext> {
        config.initialized.set(config.initialized.get() + 1);
        Ok(())
    }

    fn prepare_open(_: &mut Self::RuntimeContext) -> Option<Self::Open<'_>> {
        Some(ReadyOpen::new(Self(Identity), ()))
    }

    fn process_recv<'a>(
        &mut self,
        runtime: &mut Self::RuntimeContext,
        bytes: &'a [u8],
    ) -> Option<Self::Recv<'a>> {
        self.0.process_recv(runtime, bytes)
    }

    fn prepare_send<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        self.0.prepare_send(send, plain)
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        self.0.prepare_send_vectored(send, plain)
    }

    fn after_send<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        n: usize,
    ) -> Prepared<'a> {
        self.0.after_send(send, n)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a> {
        self.0.flush_pending(send)
    }
}

fn config() -> Config {
    Config::new(Capacities {
        connection: 1,
        waiters: 1,
        inflight: 1,
        request_entries: 1,
        request_bytes: 1,
        response_bytes: 1,
        response_values: 1,
        max_frame_bytes: 1,
    })
    .expect("config")
}

#[test]
fn connect_is_topology_only() {
    assert_eq!(size_of::<Connect<usize>>(), size_of::<usize>());
    assert_eq!(align_of::<Connect<usize>>(), align_of::<usize>());
}

#[test]
fn configured_connect_consumes_non_send_config_once_per_core() {
    type Env = Bundle<Tcp, CoreLocalWire, Throughput>;

    let initialized = Rc::new(Cell::new(0));
    let executor = Executor::new(driver::Config::for_tcp_profile::<Throughput>(8))
        .expect("driver")
        .with_storage_factory(config().factory());

    executor.enter(|mut session| {
        let backoff = session.seed().derive(dope::hash::domain::BACKOFF).state();
        let topology = Static::<Tcp>::new(
            vec![SocketAddr::from(([127, 0, 0, 1], 9))],
            Duration::from_millis(1),
            backoff,
        );
        let redis = session.storage().redis();
        let connector = {
            let mut driver = session.driver_access();
            redis
                .connect_configured::<0, _, Env>(
                    Connect { topology },
                    CoreLocalConfig {
                        initialized: Rc::clone(&initialized),
                    },
                    &mut driver,
                )
                .expect("connector")
        };

        assert_eq!(initialized.get(), 1);
        drop(connector);
    });
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
