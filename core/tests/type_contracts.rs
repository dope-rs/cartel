//! Driver identity and sealed SQL assignment contracts.
//!
//! ```
//! use cartel_core::deadline::{Elapsed, Guard, RequestPhase};
//! use dope_fiber::context::Context;
//! use std::{pin::Pin, task::Poll};
//!
//! fn poll<'d>(
//!     guard: Pin<&mut Guard<'d>>,
//!     context: Pin<&mut Context<'_, 'd>>,
//! ) -> Poll<Elapsed> {
//!     guard.poll_expired(context, RequestPhase::Response)
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_core::deadline::{Elapsed, Guard, RequestPhase};
//! use dope_fiber::context::Context;
//! use std::{pin::Pin, task::Poll};
//!
//! fn poll<'d, 'other>(
//!     guard: Pin<&mut Guard<'d>>,
//!     context: Pin<&mut Context<'_, 'other>>,
//! ) -> Poll<Elapsed> {
//!     guard.poll_expired(context, RequestPhase::Response)
//! }
//! ```
//!
//! ```compile_fail,E0277
//! use cartel_core::sql::{Assign, Target};
//! struct Value;
//! impl Assign<Value> for Target<i64> {
//!     fn assign(self, _: Value) {}
//! }
//! # fn main() {}
//! ```

#[cfg(test)]
use dope_test::checks::{Error, Suite};

#[test]
fn contracts() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["cartel-core", "dope-fiber"],
    );
    let exe = std::env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let suite = suite.try_collect(&exe, output)?;
    assert_eq!(suite.try_errors(include_str!("type_contracts.rs"))?, 1);
    let status = suite.try_rustdoc()?.status()?;
    assert!(status.success(), "type contracts: {status}");
    Ok(())
}
