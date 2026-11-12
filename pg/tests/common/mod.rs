//! Fixtures shared by the independently compiled integration tests.

pub mod counted;
pub mod env;
pub mod external;
pub mod port;
pub mod postgres;
pub mod topology;
pub mod transaction;
pub mod types;
pub mod wire;

pub fn recover(
    failure: dope::core::driver::life::recovery::Recovery<'_, '_>,
) -> std::convert::Infallible {
    panic!("{:?}", failure.cause())
}

pub fn entry_error<O, T>(
    error: dope::runtime::topology::EnterError<'_, std::convert::Infallible, O, T>,
) -> String {
    format!("{:?}", error.cause())
}
