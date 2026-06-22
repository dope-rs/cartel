//! Minimal reproducer: two Redis connectors in one dispatcher (multi-manifold),
//! then two SEQUENTIAL awaited commands on the first one. The single-manifold
//! example does 64 sequential commands fine; this isolates whether a second
//! manifold field breaks the second sequential command.
//!
//! Run: REDIS_ADDR=127.0.0.1:6379 cargo run -p cartel-redis --example multi_manifold_seq

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use cartel_redis::{DEFAULT_BACKOFF, Ops};
use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverCfg, DriverConfig, Executor};

type RedisA = Connector<0, cartel_redis::Session, Static<Tcp>, Bundle<Tcp, Identity, Throughput>>;
type RedisB = Connector<1, cartel_redis::Session, Static<Tcp>, Bundle<Tcp, Identity, Throughput>>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Multi {
    #[pin]
    #[manifold]
    a: RedisA,
    #[pin]
    #[manifold]
    b: RedisB,
}

struct Demo {
    exec: Box<Executor>,
    dispatcher: Pin<Box<Multi>>,
}

impl Demo {
    fn new(addr: SocketAddr) -> std::io::Result<Self> {
        let cfg = DriverCfg::for_tcp_profile::<Throughput>(16);
        let mut exec = Box::new(Executor::new(cfg)?);
        let driver = exec.driver_mut();
        let dispatcher = Box::pin(Multi {
            a: RedisA::new(
                cartel_redis::Session::new(),
                Static::<Tcp>::new(vec![addr], DEFAULT_BACKOFF),
                1,
                driver,
            ),
            b: RedisB::new(
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

    fn client_a<'a>(&mut self) -> Holding<'a, RedisA> {
        self.dispatcher.as_mut().a_handle()
    }

    fn client_b<'a>(&mut self) -> Holding<'a, RedisB> {
        self.dispatcher.as_mut().b_handle()
    }
}

fn main() {
    let addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()
        .expect("invalid REDIS_ADDR");
    let mut demo = Demo::new(addr).expect("driver init");
    let ca = demo.client_a();
    let cb = demo.client_b();

    demo.block_on(async {
        eprintln!("STAGE: a.wait_active");
        ca.wait_active().await.expect("a connect");
        eprintln!("STAGE: b.wait_active");
        cb.wait_active().await.expect("b connect");
        eprintln!("STAGE: a.set k1");
        ca.set(b"mm:k1", b"v1").await.expect("set1");
        eprintln!("STAGE: a.set k2 (the one that hangs in the feed)");
        ca.set(b"mm:k2", b"v2").await.expect("set2");
        eprintln!("STAGE: a.set k3");
        ca.set(b"mm:k3", b"v3").await.expect("set3");
        eprintln!("STAGE: DONE — multi-manifold sequential commands OK");
    });
}
