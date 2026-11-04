use dope::core::driver::life::recovery::Recovery;
use std::convert::Infallible;

pub mod protocol;
pub mod rejected;

pub mod topology;

mod attempts;
pub use attempts::Attempts;

pub mod session;

pub mod command;
pub mod counted;

pub fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native recovery failed: {:?}", failure.cause())
}
