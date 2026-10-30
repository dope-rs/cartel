//! Query contracts preserve exact assignment types, source identity and borrowed inputs.
//! Hygiene reuses C, N, ID and bound lifetimes; row10 forces the shorthand row binding to freshen.
//! Taking a constructor's address forces its nonempty-group proof to instantiate.
//!
//! ```
//! #![deny(unused_variables, unused_assignments)]
//! use cartel_pg::PgTable;
//!
//! #[derive(PgTable)]
//! pub struct Entry {
//!     pub id: i32,
//!     pub value: i32,
//! }
//!
//! pub enum Rows {}
//!
//! #[cartel_gen::query_group]
//! impl Rows {
//!     pub fn revise(ids: &[i32], values: &[i32]) {
//!         Entry::filter_each((&ids, &values), |row, id, value| row.id == id)
//!             .update(|row, id, value| row.value = value)
//!     }
//! }
//! ```
//!
//!
//! ```
//! # mod common { pub mod types; }
//! use cartel_pg::schema::TypedQuery;
//! use common::types::{ProjectionRenameRef, ProjectionRenameBulk};
//! fn main() {
//!     let _: &'static str = ProjectionRenameRef::SQL;
//!     let _: &'static str = ProjectionRenameBulk::SQL;
//! }
//! ```
//!
//! ```compile_fail,E0277
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Change as _;
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn invalid(name: i64) {
//!         User::filter(User::id == 1).update(User::name = name)
//!     }
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail,E0277
//! # mod common { pub mod types; }
//! use common::types::{Converted, User};
//! fn invalid(scope: &cartel_pg::dsl::Scope, row: &mut User, value: Converted) {
//!     cartel_pg::sql::Assign::assign(scope.target(&mut row.name), value);
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail,E0308
//! use cartel_pg::PgTable;
//!
//! #[derive(PgTable)]
//! pub struct Entry {
//!     pub id: i32,
//!     pub value: i32,
//! }
//!
//! pub enum Rows {}
//!
//! #[cartel_gen::query_group]
//! impl Rows {
//!     pub fn revise(ids: &[i32], values: &[i64]) {
//!         Entry::filter_each((&ids, &values), |row, id, value| row.id == id)
//!             .update(|row, id, value| row.value = value)
//!     }
//! }
//! ```

//! ```
//! use cartel_pg::{Jsonb, protocol::value::decode::RowReader, schema::Row};
//! fn optional<'row>(reader: &mut RowReader<'row>) -> Result<Option<&'row str>, cartel_pg::Error> {
//!     <Option<Jsonb> as Row>::decode(reader)
//! }
//! ```
//!
//!
//! ```
//! # mod common { pub mod types; }
//! use cartel_pg::schema::TypedQuery;
//! use common::types::ProjectionShorthand;
//! fn main() {
//!     let _: &'static str = ProjectionShorthand::SQL;
//! }
//! ```
//!
//!
//! ```compile_fail,E0609
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Fetch as _, Project as _};
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn unknown(id: i64) -> Vec<i64> {
//!         User::filter(User::id == id).select(User::missing).all()
//!     }
//! }
//! # fn main() {}
//! ```

//!
//! ```compile_fail,E0308
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Reduce as _;
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn wrong_group() -> Vec<(i64, i64)> {
//!         User::filter(true).group_by(User::name).count()
//!     }
//! }
//! # fn main() {}
//! ```

//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::schema::TypedQuery;
//! type UserAlias = User;
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn ambiguous_alias() -> Vec<UserAlias> {
//!         User::join::<User>(|left, right| left.id < right.id)
//!             .filter(|_left, _right| true)
//!             .all()
//!     }
//! }
//! # fn main() { let _ = UserAmbiguousAlias::SQL; }
//! ```

//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn ambiguous_row() -> Vec<User> {
//!         User::join::<User>(|left, right| left.id < right.id)
//!             .filter(|_left, _right| true)
//!             .all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::{Post, User};
//! use cartel_pg::dsl::query::ops::{Fetch as _, Project as _};
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn foreign(id: i64) -> Vec<i64> {
//!         User::filter(Post::id == id).select(User::id).all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::{Post, User};
//! use cartel_pg::dsl::query::ops::{Fetch as _, Project as _};
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn ambiguous() -> Vec<i64> {
//!         User::join::<Post>(|user, post| user.id == post.author_id)
//!             .filter(|user, _post| user.id > 0)
//!             .select(User::id)
//!             .all()
//!     }
//! }
//! # fn main() {}
//! ```

//!
//! ```
//! # mod common { pub mod types; }
//! use std::{cell::Cell, marker::PhantomData};
//! use cartel_pg::{api::traits::Query, schema::HasGroup};
//! use common::types::{C as NamedClient, N, Projection};
//!
//! fn grouped<'d, C, const ID: u8>(client: C, closure: &Cell<dyn Fn(&str)>)
//! where
//!     C: Query<'d, ID>,
//!     C::Schema: HasGroup<Projection>,
//! {
//!     let name = String::from("borrowed");
//!     let one = Projection::named_group(client, [name.as_str()]);
//!     let two = Projection::named_id_group(client, [(1, name.as_str())]);
//!     let zero = Projection::required_group(client, [(); 2]);
//!     let named = Projection::by_name_group(client, [(name.as_str(), N)]);
//!     let lexical = Projection::lexical(
//!         client, 1, NamedClient, [0], PhantomData, PhantomData, closure,
//!     );
//!     let lexical_group = Projection::lexical_group(
//!         client, [(1, NamedClient, [0], PhantomData, PhantomData, closure)],
//!     );
//!     drop(name);
//!     drop((one, two, zero, named, lexical, lexical_group));
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Fetch as _;
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn lookup_group() -> User {
//!         User::filter(User::id == 1).one()
//!     }
//!
//!     pub fn lookup() -> User {
//!         User::filter(User::id == 1).one()
//!     }
//! }
//! # fn main() {}
//! ```

//!
//! ```
//! # mod common { pub mod types; }
//! use std::{cell::Cell, marker::PhantomData};
//! use cartel_pg::{api::traits::Query, schema::HasGroup};
//! use dope_fiber::task::batch;
//! use common::types::{C as NamedClient, Projection};
//!
//! fn independent<'d, C, const ID: u8>(
//!     client: C, pool: &mut batch::Pool<'d, 1>, closure: &Cell<dyn Fn(&str)>,
//! )
//! where
//!     C: Query<'d, ID>,
//!     C::Schema: HasGroup<Projection>,
//! {
//!     let one = {
//!         let name = String::from("borrowed");
//!         Projection::named_each(client, pool, [name.as_str()])
//!     };
//!     drop(one);
//!     let first = {
//!         let name = String::from("borrowed");
//!         Projection::first_named_each(client, pool, [name.as_str()])
//!     };
//!     drop(first);
//!     let all = {
//!         let name = String::from("borrowed");
//!         Projection::all_named_each(client, pool, [name.as_str()])
//!     };
//!     drop(all);
//!     let command = {
//!         let name = String::from("borrowed");
//!         Projection::rename_ref_each(client, pool, [(1, name.as_str())])
//!     };
//!     drop(command);
//!     let lexical = Projection::lexical_each(
//!         client, pool, [(1, NamedClient, [0], PhantomData, PhantomData, closure)],
//!     );
//!     drop(lexical);
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Fetch as _;
//!
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn lookup() -> Vec<User> {
//!         User::filter(User::id == 1).all()
//!     }
//!
//!     pub fn r#lookup_each() -> Vec<User> {
//!         User::filter(User::id == 1).all()
//!     }
//! }
//! # fn main() {}
//! ```

//!
//! ```
//! # mod common { pub mod postgres; pub mod wire; }
//! use common::wire::checked::{Database, TextQuery};
//! use cartel_pg::api::traits::Query as _;
//! type Query = cartel_pg::api::ops::Query<
//!     'static, 'static, 'static, 'static, 'static, 'static, 'static, (), Database,
//! >;
//! fn main() {
//!     std::hint::black_box(Query::run_group::<TextQuery, 1> as fn(Query, [(); 1]) -> _);
//! }
//! ```
//!
//! ```compile_fail,E0080
//! # mod common { pub mod postgres; pub mod wire; }
//! use common::wire::checked::{Database, TextQuery};
//! use cartel_pg::api::traits::Query as _;
//! type Query = cartel_pg::api::ops::Query<
//!     'static, 'static, 'static, 'static, 'static, 'static, 'static, (), Database,
//! >;
//! fn main() {
//!     std::hint::black_box(Query::run_group::<TextQuery, 0> as fn(Query, [(); 0]) -> _);
//! }
//! ```

//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Fetch as _;
//! #[cartel_gen::query_group]
//! impl User where User: Copy {
//!     pub fn guarded() -> User { User::filter(User::id == 1).one() }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Fetch as _;
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn guarded() -> User where User: Copy { User::filter(User::id == 1).one() }
//! }
//! # fn main() {}
//! ```

//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Fetch as _;
//! #[cartel_gen::query_group]
//! impl User {
//!     #[automatically_derived]
//!     pub fn guarded() -> User { User::filter(User::id == 1).one() }
//! }
//! # fn main() {}
//! ```

//!
//! ```compile_fail,E0080
//! # mod common { pub mod wire { pub mod prepare; } }
//! use cartel_pg::schema;
//! use common::wire::prepare::NegParams;
//! fn main() {
//!     std::hint::black_box(schema::QueryMeta::fixed::<NegParams>());
//! }
//! ```
//!
//! ```compile_fail,E0080
//! # mod common { pub mod postgres; pub mod wire; }
//! use cartel_pg::schema;
//! use common::wire::checked::Database;
//! fn main() {
//!     std::hint::black_box(schema::Prepare::fixed::<Database, 0>());
//! }
//! ```

//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Arrange as _, Reduce as _};
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn locked_count() -> i64 {
//!         User::filter(|_row| true).for_update().count()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Arrange as _, Fetch as _, Set as _};
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn locked_set() -> Vec<User> {
//!         User::filter(|row| row.id > 0)
//!             .union(User::filter(|row| row.id < 10))
//!             .for_share().all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Arrange as _, Fetch as _};
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn locked_distinct() -> Vec<User> {
//!         User::filter(|_row| true).distinct().for_update().all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Arrange as _, Fetch as _, Project as _};
//! #[cartel_gen::query_group]
//! impl User {
//!     pub fn locked_window() -> Vec<i64> {
//!         User::filter(|_row| true).for_share()
//!             .select(|row| Dialect::row_number().over(|w| w.order_by(row.id))).all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Change as _, Project as _};
//! #[cartel_pg::query_group]
//! impl User {
//!     pub fn cte_update() {
//!         let users = User::filter(|row| row.age > 0).cte();
//!         users.filter(|row| row.id > 0).update(|row| row.age = 1)
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Fetch as _, Project as _};
//! #[cartel_pg::query_group]
//! impl User {
//!     pub fn duplicate_cte() -> Vec<User> {
//!         let selected = User::filter(|_row| true).cte();
//!         let r#selected = User::filter(|_row| true).cte();
//!         selected.filter(|row| row.age > 0).all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::Fetch as _;
//! #[cartel_pg::query_group]
//! impl User {
//!     pub fn compared_columns() -> Vec<User> {
//!         User::filter(|row| row.id < row.id).all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0308
//! # mod common { pub mod types; }
//! use common::types::{User, UserId};
//! use cartel_pg::dsl::query::ops::Change as _;
//! #[cartel_pg::query_group]
//! impl User {
//!     pub fn wrong_return() -> Option<UserId> {
//!         User::filter(|row| row.id > 0).delete().returning_first()
//!     }
//! }
//! # fn main() {}
//! ```

use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn checks_bulk_columns() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "cartel-pg",
            "cartel-gen",
            "cartel-core",
            "dope",
            "dope-fiber",
            "dope-test",
            "o3",
            "hmac",
            "sha2",
        ],
    );
    let exe = env::current_exe()?;
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success(), "build original library: {status}");
    assert_eq!(
        suite.try_errors(&exe, include_str!("query_contracts.rs"))?,
        9
    );
    let status = suite.try_rustdoc(&exe)?.status()?;
    assert!(status.success(), "bulk update type contracts: {status}");
    Ok(())
}
