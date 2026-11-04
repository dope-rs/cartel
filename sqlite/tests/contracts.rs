//! Compile-time ownership and API contracts.
//!
//! ```compile_fail,E0599
//! use cartel_sqlite::Transaction;
//!
//! fn bypass(tx: &Transaction<'_, '_, '_>) {
//!     let _ = tx.raw();
//! }
//! ```
//!
//! ```
//! # pub mod common;
//! use cartel_sqlite::Connection;
//! use common::catalog::Seed;
//!
//! fn query(connection: &mut Connection<'_, '_, '_, Seed>) {
//!     drop(connection.query().run_one::<i64, _, _>(Seed::COUNT, ()));
//! }
//! # fn main() {}
//! ```
//!
//! Handwritten mappings are admitted once, including calls from ordinary runtime functions.
//!
//! ```
//! # pub mod common;
//! use cartel_sqlite::Statement;
//! use common::catalog::{Manual, Query};
//! use std::mem;
//! fn main() {
//!     let statement = Statement::<Manual<0>, Query<false, 0, 1>>::of();
//!     assert_eq!(mem::size_of_val(&statement), 0);
//! }
//! ```
//!
//! ```compile_fail,E0080
//! # pub mod common;
//! use cartel_sqlite::Statement;
//! use common::catalog::{Manual, Query};
//! fn main() {
//!     let _ = Statement::<Manual<1>, Query<false, 0, 1>>::of();
//! }
//! ```
//!
//! ```compile_fail,E0080
//! # pub mod common;
//! use cartel_sqlite::Statement;
//! use common::catalog::{Manual, Query};
//! fn main() {
//!     let _ = Statement::<Manual<0>, Query<true, 0, 1>>::of();
//! }
//! ```
//!
//! ```compile_fail,E0080
//! # pub mod common;
//! use cartel_sqlite::Statement;
//! use common::catalog::{Manual, Query};
//! fn main() {
//!     let _ = Statement::<Manual<0>, Query<false, 1, 1>>::of();
//! }
//! ```
//!
//! ```compile_fail,E0080
//! # pub mod common;
//! use cartel_sqlite::Statement;
//! use common::catalog::{Manual, Query};
//! fn main() {
//!     let _ = Statement::<Manual<0>, Query<false, 0, 0>>::of();
//! }
//! ```
//!
//! ```compile_fail,E0308
//! # pub mod common;
//! use cartel_sqlite::Connection;
//! use common::catalog::{Seed, Stable};
//!
//! fn query(connection: &mut Connection<'_, '_, '_, Seed>) {
//!     drop(connection.query().run_one::<i64, _, _>(Stable::COUNT, ()));
//! }
//! # fn main() {}
//! ```
//!
//! ```
//! # pub mod common;
//! use cartel_sqlite::Catalog;
//! use common::engine::mutation::Commands;
//! fn main() {
//!     let _: &[cartel_sqlite::Entry<'_>] = Commands::STATEMENTS;
//! }
//! ```
//!
//! Qualified fields and aggregate shorthand retain the Rust field proof; text bindings may borrow while assignments retain field types.
//!
//! ```compile_fail,E0609
//! # pub mod common;
//! use common::schema::User;
//!
//! #[cartel_sqlite::query]
//! fn unknown(id: i64) -> Vec<i64> {
//!     User::filter(User::id == id).rows().select(User::missing).all()
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0609
//! # pub mod common;
//! use common::schema::User;
//!
//! #[cartel_sqlite::query]
//! fn unknown_sum() -> Option<i64> {
//!     User::filter(true).sum(User::missing)
//! }
//! # fn main() {}
//! ```
//!
//! ```
//! # pub mod common;
//! use common::schema::User;
//!
//! #[cartel_sqlite::query]
//! fn rename(id: i64, name: &str) {
//!     User::filter(|scope| scope.id == id).update(|scope| scope.name = name)
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0308
//! # pub mod common;
//! use common::schema::User;
//!
//! #[cartel_sqlite::query]
//! fn widen_age(id: i64, age: i64) {
//!     User::filter(|row| row.id == id).update(|row| row.age = age)
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0599
//! use cartel_sqlite::Savepoint;
//!
//! fn bypass(save: &Savepoint<'_, '_, '_>) {
//!     let _ = save.raw();
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_sqlite::Connection;
//!
//! fn widen<'app, 'local: 'app, 'd: 'local>(
//!     connection: Connection<'app, 'local, 'd>,
//! ) -> Connection<'static, 'static, 'static> {
//!     connection
//! }
//! ```
//!
//! ```compile_fail,E0505
//! use cartel_sqlite::{Catalog, Limits, Memory, OpenOptions, runtime::Runtime};
//!
//! fn release<I: Catalog>(runtime: Runtime, limits: Limits, memory: Memory) {
//!     let sql = String::from("CREATE TABLE retained(id INTEGER)");
//!     let commands = [sql.as_str()];
//!     let target = OpenOptions::new().open_memory(runtime).unwrap()
//!         .commands(&commands).unwrap();
//!     let plan = target.plan::<I>(limits, memory).unwrap();
//!     drop(sql);
//!     drop(plan);
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0277
//! # pub mod common;
//! use cartel_sqlite::Installed;
//! use common::catalog::Manual;
//! use std::pin::Pin;
//! fn unpin<'a, 'local: 'a, 'd: 'local>(
//!     owner: Pin<&'a mut Installed<'local, 'd, Manual<0>>>,
//! ) -> &'a mut Installed<'local, 'd, Manual<0>> {
//!     Pin::into_inner(owner)
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # pub mod common;
//! use common::schema::User;
//! #[cartel_sqlite::query]
//! fn cte_delete() {
//!     let users = User::filter(|row| row.active).cte();
//!     users.filter(|row| row.id > 0).delete()
//! }
//! # fn main() {}
//! ```

#[test]
fn contracts() -> Result<(), dope_test::checks::Error> {
    use std::env;

    use dope_test::checks::Suite;
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "cartel-sqlite",
            "dope-core",
            "dope-fiber",
            "dope-manifold",
            "dope-runtime",
            "dope-test",
            "libsqlite3-sys",
        ],
    );
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let artifacts = suite.try_collect(&exe, output)?;
    assert_eq!(
        artifacts.try_errors(include_str!("contracts.rs"), compile)?,
        12
    );
    let status = artifacts.try_rustdoc()?.status().expect("run rustdoc");
    assert!(status.success(), "compile-time contracts: {status}");
    Ok(())
}

fn compile(command: &mut std::process::Command, source: &str) -> std::process::Output {
    let mut child = command.spawn().expect("start compiler");
    let written = child
        .stdin
        .take()
        .map(|mut input| std::io::Write::write_all(&mut input, source.as_bytes()));
    let output = child.wait_with_output().expect("reap compiler");
    written
        .expect("compiler stdin")
        .expect("write compiler source");
    output
}
