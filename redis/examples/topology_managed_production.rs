use std::net::SocketAddr;
use std::time::Instant;

use cartel_redis::{Capacities, Config, ConfigError, Connect, DEFAULT_BACKOFF, Ops};
use dope::driver;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::Executor;
use dope::runtime::profile::Throughput;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()?;

    let driver = driver::Config::for_tcp_profile::<Throughput>(16);
    let exec = Executor::new(driver)?.with_storage_factory(redis_config()?.factory());
    exec.enter(|mut session| -> Result<(), Box<dyn std::error::Error>> {
        let backoff = session.seed().derive(dope::hash::domain::BACKOFF).state();
        let redis = session.storage().redis();
        let connector = {
            let mut driver = session.driver_access();
            redis
                .connect::<0, _, Env>(
                    Connect {
                        topology: Static::<Tcp>::new(vec![addr], DEFAULT_BACKOFF, backoff),
                    },
                    &mut driver,
                )?
        };

        let probe = dope_fiber::fiber!('_ => async move {
            redis.wait_active().await?;
            for _ in 0..16 {
                redis.ping().await?;
            }
            let started = Instant::now();
            let mut hits = 0u64;
            for _ in 0..64 {
                redis.ping().await?;
                hits += 1;
            }
            let elapsed = started.elapsed();
            let id = redis.client_id().await?;
            let info = redis.info(Some(b"server")).await?;
            Ok::<_, cartel_redis::Error>((hits, elapsed, id, info))
        });
        let (hits, elapsed, id, info) =
            dope_extra::runtime::AppRuntime::enter(&mut session, connector, |mut runtime| {
                runtime.block_on(probe)
            })??;
        let info_summary = std::str::from_utf8(info.as_slice())
            .ok()
            .and_then(|text| {
                text.lines()
                    .find(|line| line.starts_with("redis_version:"))
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| String::from("redis_version: unknown"));
        println!(
            "production probe: client_id={id} pings={hits} elapsed_ms={} per_op_us={:.1} {info_summary}",
            elapsed.as_millis(),
            elapsed.as_micros() as f64 / hits as f64,
        );
        Ok(())
    })
}
