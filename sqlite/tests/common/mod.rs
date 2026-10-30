use dope_core::{driver::life::recovery::Recovery, io};
use std::convert::Infallible;

pub mod catalog;

pub mod database;

pub mod query;

pub mod schema;

pub mod engine;

pub mod wal;

pub mod plan;

pub mod forget;

pub fn assert_reclaimed(event: io::Reclaimed) {
    assert!(
        matches!(event, io::Reclaimed::Shutdown),
        "unexpected SQLite reclaim: {event:?}"
    );
}

pub fn assert_retirement(failure: Recovery<'_, '_>) -> Infallible {
    panic!("SQLite retirement failed: {:?}", failure.cause())
}
