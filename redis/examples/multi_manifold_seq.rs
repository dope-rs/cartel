use std::net::SocketAddr;
use std::pin::pin;

use cartel_redis::{Capacities, Config, ConfigError, Connect, DEFAULT_BACKOFF, Ops};
use dope::driver;
use dope::manifold::Manifold;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::Executor;
use dope::runtime::profile::Throughput;
use dope_fiber::SessionExt as _;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::cell::BrandCell;

type Env = Bundle<Tcp, Identity, Throughput>;

fn redis_config() -> Result<Config, ConfigError> {
    Config::new(Capacities {
        connection: 1,
        waiters: 16,
        inflight: 256,
        request_entries: 256,
        request_bytes: 64 * 1024,
        response_bytes: 64 * 1024 * 1024,
        response_values: 65_536,
        max_frame_bytes: 16 * 1024 * 1024,
    })
}

#[cartel_gen::dispatcher(new)]
struct Multi<'d, A, B>
where
    A: Manifold<'d>,
    B: Manifold<'d>,
{
    #[manifold]
    a: A,
    #[manifold]
    b: B,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()?;
    let driver = driver::Config::for_tcp_profile::<Throughput>(16);
    let exec = Executor::new(driver)?
        .with_storage_factory((redis_config()?.factory(), redis_config()?.factory()));
    exec.enter(|mut session| -> Result<(), Box<dyn std::error::Error>> {
        let a_backoff = session.seed().derive(dope::hash::domain::BACKOFF).state();
        let b_backoff = session
            .seed()
            .derive(dope::hash::domain::BACKOFF ^ 1)
            .state();
        let (a_store, b_store) = session.storage();
        let mut driver = session.driver_access();
        let a = a_store.redis();
        let b = b_store.redis();
        let (a_connector, b_connector) = {
            let a_connector = a.connect::<0, _, Env>(
                Connect {
                    topology: Static::<Tcp>::new(vec![addr], DEFAULT_BACKOFF, a_backoff),
                },
                &mut driver,
            )?;
            let b_connector = b.connect::<1, _, Env>(
                Connect {
                    topology: Static::<Tcp>::new(vec![addr], DEFAULT_BACKOFF, b_backoff),
                },
                &mut driver,
            )?;
            (a_connector, b_connector)
        };

        let commands = dope_fiber::fiber!('_ => async move {
            let a_active = a.wait_active();
            a_active.await?;
            let b_active = b.wait_active();
            b_active.await?;
            let first = a.set(b"mm:k1", b"v1");
            first.await?;
            let second = a.set(b"mm:k2", b"v2");
            second.await?;
            let third = a.set(b"mm:k3", b"v3");
            third.await?;
            Ok::<_, cartel_redis::Error>(())
        });
        let dispatcher = pin!(BrandCell::new(Multi::new(a_connector, b_connector)));
        session.block_on(dispatcher.as_ref(), commands)??;
        Ok(())
    })
}
