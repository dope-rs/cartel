use cartel_redis::{
    Error,
    value::{self, reply},
};
use common::{Attempts, recover};
use dope::{
    core::driver::{
        life::{creation::Attempt, recovery::Policy},
        settings,
    },
    manifold::timing::Throughput,
    net::link::pool::change::opening,
    runtime::executor::Executor,
};
use dope_test::alloc::Tracker;
use o3::depot::capacity;
use std::{convert::Infallible, mem};

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn preserves_attempt() {
    let mut events = Vec::new();
    let mut attempt = Attempt::new();
    let executor = Executor::new(&mut attempt, settings::Config::for_profile::<Throughput>())
        .ok()
        .expect("test executor");
    let result = executor
        .with_plan(Attempts::<17>::new(capacity::Count::new(1)))
        .run(|executor| {
            executor.try_create(
            |scope| {
                let source = scope.storage();
                let reserve = source
                    .try_reserve()
                    .expect("an attempt generation must be available");
                let key = reserve.id();
                let cause = opening::Failure::<Infallible>::IdentityExhausted.into_wire();
                let cause = match cause {
                    Err(cause) => cause,
                    Ok(never) => match never {},
                };
                let ((), counts) = Tracker::usage(|| {
                    let error = Error::ConnectionExhausted { key, cause };
                    let error: reply::Failure<cartel_redis::Error<'_, 17>, value::Bytes<'_, '_, '_>> = error.into();
                    assert!(matches!(
                        error,
                        reply::Failure::Control(Error::ConnectionExhausted { key: actual, .. })
                            if actual == key
                    ));
                });
                assert_eq!(counts, (0, 0));
            },
            Policy::new(|report| events.push(report), recover),
        ).is_ok()
        });
    assert!(
        result,
        "attempt storage generation and finalization must succeed"
    );
    assert!(
        events
            .iter()
            .all(|report| matches!(report, dope::core::io::Reclaimed::Shutdown)),
        "attempt storage does not submit network operations: {events:?}"
    );
}

#[test]
fn route_layout() {
    assert_eq!(
        mem::size_of::<Error<'static, 0>>(),
        mem::size_of::<Error<'static, 17>>()
    );
    assert_eq!(
        mem::size_of::<
            reply::Failure<
                cartel_redis::Error<'static, 0>,
                value::Bytes<'static, 'static, 'static>,
            >,
        >(),
        mem::size_of::<
            reply::Failure<
                cartel_redis::Error<'static, 17>,
                value::Bytes<'static, 'static, 'static>,
            >,
        >()
    );
}
