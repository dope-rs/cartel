use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

use cartel_redis::{DEFAULT_BACKOFF, Ops};
use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverCfg, DriverConfig, Executor};

type RedisConn =
    Connector<0, cartel_redis::Session, Static<Tcp>, Bundle<Tcp, Identity, Throughput>>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct RedisDispatcher {
    #[pin]
    #[manifold]
    redis: RedisConn,
}

struct ProductionDemo {
    exec: Box<Executor>,
    dispatcher: Pin<Box<RedisDispatcher>>,
}

impl ProductionDemo {
    fn new(addr: SocketAddr) -> std::io::Result<Self> {
        let cfg = DriverCfg::for_tcp_profile::<Throughput>(16);
        let mut exec = Box::new(Executor::new(cfg)?);

        let driver = exec.driver_mut();
        let dispatcher = Box::pin(RedisDispatcher {
            redis: RedisConn::new(
                cartel_redis::Session::new(),
                Static::<Tcp>::new(vec![addr], DEFAULT_BACKOFF),
                1,
                driver,
            ),
        });
        Ok(Self { exec, dispatcher })
    }

    fn block_on<F: Future>(&mut self, fut: F) -> F::Output {
        dope_extra::block_on(
            &mut self.exec,
            self.dispatcher.as_mut(),
            dope::fiber::Fiber::new(fut),
        )
    }

    fn client<'a>(&mut self) -> Holding<'a, RedisConn> {
        self.dispatcher.as_mut().redis_handle()
    }
}

fn main() {
    let addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()
        .expect("invalid REDIS_ADDR");

    let mut demo = ProductionDemo::new(addr).expect("driver init");
    let client = demo.client();

    demo.block_on(async {
        client.wait_active().await.expect("redis connect");
        let started = Instant::now();
        let probes = 64;
        let mut hits = 0u64;
        for _ in 0..probes {
            client.ping().await.expect("ping");
            hits += 1;
        }
        let elapsed = started.elapsed();

        let id = client.client_id().await.expect("client id");
        let info = client
            .info(Some(b"server"))
            .await
            .expect("info server");
        let info_summary = std::str::from_utf8(info.as_slice())
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|line| line.starts_with("redis_version:"))
                    .map(|line| line.to_string())
            })
            .unwrap_or_else(|| String::from("redis_version: unknown"));

        println!(
            "production demo: client_id={id} pings={hits} elapsed_ms={} per_op_us={:.1} {info_summary}",
            elapsed.as_millis(),
            elapsed.as_micros() as f64 / hits as f64,
        );
    });
}
