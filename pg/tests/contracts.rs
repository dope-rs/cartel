//! Compile-time contracts preserve API visibility, schema membership, routes and driver lifetimes.
//! Views and pending operations borrow their owners; completed rows and COPY chunks retain ownership.
//! Transactions, savepoints, listeners and cancellation tokens preserve each parent guard.
//!
//! ```compile_fail
//! fn trace(config: &cartel_pg::Config) {
//!     format!("{config:?}");
//! }
//! ```
//!
//! ```compile_fail,E0599
//! use cartel_pg::{port::Port, schema::QuerySet};
//!
//! fn raw<'app, 'g, 'r, 's, 'b, 'c, 'd, I: QuerySet>(port: &'app Port<'app, 'g, 'r, 's, 'b, 'c, 'd, I>) {
//!     port.client_provider();
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Transaction as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized + 'app, I: QuerySet + 'd>(
//!     tx: api::tx::Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> api::ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I> {
//!     tx.query()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Transaction as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized + 'app, I: QuerySet + 'd>(
//!     tx: api::tx::Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> Option<api::tx::control::CancelToken<'app, 'd>> {
//!     tx.cancel_token()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Savepoint as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized + 'app, I: QuerySet + 'd>(
//!     save: api::tx::control::Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> api::ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I> {
//!     save.query()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Listener as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized + 'app, I: QuerySet + 'd>(
//!     listener: api::tx::control::ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> api::ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I> {
//!     listener.query()
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use cartel_pg::{api::ops::Query, schema::QuerySet};
//!
//! fn cross_route<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized + 'app, I: QuerySet + 'd>(
//!     query: Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, 2>,
//! ) -> Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, 1> {
//!     query
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_pg::{api::ops::Query, schema::QuerySet};
//!
//! fn escape_driver<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized + 'app, I: QuerySet + 'static>(
//!     query: Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> Query<'app, 'g, 'r, 's, 'b, 'c, 'static, B, I> {
//!     query
//! }
//! ```
//!
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api::results, port::replies::Payload, schema::TypedQuery};
//!
//! fn escape<'view, 'b: 'view, 'c: 'view, 'd: 'view, Q: TypedQuery<Row = String>>(
//!     row: results::Row<Payload<'b, 'c, 'd>, Q>,
//! ) -> Result<&'view str, cartel_pg::Error> {
//!     row.decode()
//! }
//! ```
//!
//!
//! ```
//! use cartel_pg::{api::results, port::replies::Payload, schema::TypedQuery};
//!
//! fn own<'b, 'c, 'd, Q: TypedQuery<Row = String>>(
//!     row: results::Row<Payload<'b, 'c, 'd>, Q>,
//! ) -> Result<String, cartel_pg::Error> {
//!     row.into_owned()
//! }
//! ```
//!
//!
//! ```
//! use cartel_pg::{
//!     api::{traits::{Begin as _, Client, Query as _, Transaction as _}, results},
//!     reply, schema,
//! };
//! use dope_fiber::abi::Fiber;
//!
//! fn retain<'d, C, Q, const ID: u8>(
//!     client: C,
//! ) -> impl Fiber<'d, Output = Result<results::Row<C::Payload, Q>, reply::Error<'d, ID>>>
//! where
//!     C: Client<'d, ID>,
//!     C::Schema: schema::HasGroup<Q::Group>,
//!     Q: schema::TypedQuery<Result = schema::result::Rows>,
//!     Q::Group: schema::QueryGroup,
//!     for<'p> Q::Params<'p>: Default,
//! {
//!     dope_fiber::fiber!('d => async move {
//!         let tx = client.transaction().begin().await?;
//!         let row = tx.query().run_one::<Q>(Default::default()).await?;
//!         tx.commit().await?;
//!         Ok(row)
//!     })
//! }
//! ```
//!
//!
//! ```compile_fail,E0505
//! use cartel_pg::{
//!     api::traits::{Begin as _, Client, Query as _, Transaction as _},
//!     reply, schema,
//! };
//! use dope_fiber::abi::Fiber;
//!
//! fn retain<'d, C, Q, const ID: u8>(
//!     client: C,
//! ) -> impl Fiber<'d, Output = Result<(), reply::Error<'d, ID>>>
//! where
//!     C: Client<'d, ID>,
//!     C::Schema: schema::HasGroup<Q::Group>,
//!     Q: schema::TypedQuery<Result = schema::result::Rows>,
//!     Q::Group: schema::QueryGroup,
//!     for<'p> Q::Params<'p>: Default,
//! {
//!     dope_fiber::fiber!('d => async move {
//!         let tx = client.transaction().begin().await?;
//!         let rows = tx.query().run_all::<Q>(Default::default()).await?;
//!         tx.commit().await?;
//!         drop(rows);
//!         Ok(())
//!     })
//! }
//! ```
//!
//!
//! ```compile_fail,E0277
//! use cartel_pg::{api::traits::Query, schema};
//!
//! fn unregistered<'d, C, Q, const ID: u8>(query: C, params: Q::Params<'_>)
//! where
//!     C: Query<'d, ID>,
//!     Q: schema::TypedQuery<Result = schema::result::Rows>,
//!     Q::Group: schema::QueryGroup,
//! {
//!     <C as Query<'d, ID>>::run_one::<Q>(query, params);
//! }
//! ```
//!
//!
//! ```compile_fail,E0515
//! use cartel_pg::{
//!     api::results::{self, Validated, source::Payload},
//!     protocol::value::decode::checked::Fields,
//!     schema::{Project, Row, TypedQuery},
//! };
//!
//! fn escape<'view, P: Payload + 'view, Q>(row: Validated<results::Row<P, Q>>) -> &'view str
//! where
//!     Q: TypedQuery,
//!     Q::Row: Project + for<'row> Row<View<'row> = &'row str>,
//!     <Q::Row as Project>::Fields: Fields,
//! {
//!     row.view()
//! }
//! ```
//!
//!
//! ```
//! use cartel_pg::{
//!     api::traits::{Begin as _, Client, Savepoint as _, Transaction as _, copy::{Stream as _, Transfer as _}},
//!     Identifier, reply,
//! };
//! use dope_fiber::abi::Fiber;
//!
//! fn retain<'d, C: Client<'d>>(
//!     client: C, name: Identifier, nested: Identifier,
//! ) -> impl Fiber<'d, Output = Result<Option<C::CopyChunk>, reply::Error<'d>>> {
//!     dope_fiber::fiber!('d => async move {
//!         let mut tx = client.transaction().begin().await?;
//!         let mut save = tx.savepoint(name).await?;
//!         let child = save.savepoint(nested).await?;
//!         let mut stream = child.copy().copy_out("COPY items TO STDOUT");
//!         let first = stream.next_chunk().await?;
//!         drop(stream.next_chunk().await?);
//!         drop(stream);
//!         child.release().await?;
//!         save.rollback().await?;
//!         tx.commit().await?;
//!         Ok(first)
//!     })
//! }
//! ```
//!
//!
//! ```
//! use cartel_pg::{
//!     api::traits::{Listener as _, Notify, Query as _, Savepoint as _, Transaction},
//!     Identifier, reply, schema,
//! };
//! use dope_fiber::abi::Fiber;
//!
//! fn finish<'d, T, Q>(mut tx: T, name: Identifier, nested: Identifier)
//!     -> impl Fiber<'d, Output = Result<(), reply::Error<'d>>>
//! where
//!     T: Transaction<'d>,
//!     T::Schema: schema::HasGroup<Q::Group>,
//!     Q: schema::TypedQuery<Result = schema::result::Rows>,
//!     Q::Group: schema::QueryGroup,
//!     for<'p> Q::Params<'p>: Default,
//! {
//!     dope_fiber::fiber!('d => async move {
//!         let mut save = tx.savepoint(name).await?;
//!         let child = save.savepoint(nested).await?;
//!         let outcome = child.query().run_group::<Q, 2>([Default::default(), Default::default()]).await;
//!         child.release().await?;
//!         save.release().await?;
//!         drop(tx.finish(outcome).await);
//!         Ok(())
//!     })
//! }
//!
//! fn unlisten<'d, N, Q>(notify: N, channel: Identifier)
//!     -> impl Fiber<'d, Output = Result<(), reply::Error<'d>>>
//! where
//!     N: Notify<'d>,
//!     N::Schema: schema::HasGroup<Q::Group>,
//!     Q: schema::TypedQuery<Result = schema::result::Rows>,
//!     Q::Group: schema::QueryGroup,
//!     for<'p> Q::Params<'p>: Default,
//! {
//!     dope_fiber::fiber!('d => async move {
//!         let listener = notify.listen(channel).await?;
//!         let outcome = listener.query().run_group::<Q, 2>([Default::default(), Default::default()]).await;
//!         listener.unlisten().await?;
//!         drop(outcome);
//!         Ok(())
//!     })
//! }
//! ```
//!
//!
//! ```
//! use cartel_pg::api::traits::{Unchecked, copy::Transfer};
//!
//! fn inputs<'d, U: Unchecked<'d>, T: Transfer<'d>>(unchecked: U, transfer: T) {
//!     let sql = String::from("SELECT 1");
//!     let command = unchecked.command(&sql);
//!     let data = vec![1, 2, 3];
//!     let copy = transfer.copy_in(&sql, &data);
//!     let sink = transfer.copy_stream(&sql);
//!     let stream = transfer.copy_out(&sql);
//!     drop((sql, data));
//!     drop((command, copy, sink, stream));
//! }
//! ```
//!
//!
//! ```compile_fail,E0505
//! use cartel_pg::api::traits::copy::Sink;
//!
//! fn input<'d, S: Sink<'d>>(mut sink: S) {
//!     let data = vec![1];
//!     let pending = sink.write(&data);
//!     drop(data);
//!     drop(pending);
//! }
//! ```
//!
//! ```compile_fail,E0505
//! use cartel_pg::api::traits::copy::Sink;
//!
//! fn finish<'d, S: Sink<'d>>(mut sink: S) {
//!     let pending = sink.write(&[]);
//!     drop(sink.finish());
//!     drop(pending);
//! }
//! ```
//!
//!
//! ```compile_fail,E0515
//! use cartel_pg::api::traits::copy::Stream;
//!
//! fn escape<'view, 'd: 'view, S: Stream<'d> + 'view>(chunk: S::Chunk) -> &'view [u8] {
//!     chunk.as_ref()
//! }
//! ```
//!
//!
//! ```compile_fail,E0505
//! use cartel_pg::{api::traits::Transaction, Identifier};
//!
//! fn pending<'d, T: Transaction<'d>>(mut tx: T, name: Identifier) {
//!     let child = tx.savepoint(name);
//!     drop(tx.commit());
//!     drop(child);
//! }
//! ```
//!
//! ```compile_fail,E0505
//! use cartel_pg::{api::traits::{Savepoint as _, Transaction}, Identifier, reply};
//! use dope_fiber::abi::Fiber;
//!
//! fn child<'d, T: Transaction<'d>>(mut tx: T, name: Identifier)
//!     -> impl Fiber<'d, Output = Result<(), reply::Error<'d>>>
//! {
//!     dope_fiber::fiber!('d => async move {
//!         let child = tx.savepoint(name).await?;
//!         tx.commit().await?;
//!         child.release().await
//!     })
//! }
//! ```
//!
//! ```compile_fail,E0505
//! use cartel_pg::{api::traits::Savepoint, Identifier};
//!
//! fn nested<'d, S: Savepoint<'d>>(mut save: S, name: Identifier) {
//!     let child = save.savepoint(name);
//!     drop(save.release());
//!     drop(child);
//! }
//! ```
//!
//! ```compile_fail,E0499
//! use cartel_pg::{api::traits::Transaction, Identifier};
//!
//! fn pending<'d, T: Transaction<'d>>(mut tx: T, a: Identifier, b: Identifier) {
//!     let first = tx.savepoint(a);
//!     let second = tx.savepoint(b);
//!     drop((first, second));
//! }
//! ```
//!
//! ```compile_fail,E0499
//! use cartel_pg::{api::traits::Savepoint, Identifier, reply};
//! use dope_fiber::abi::Fiber;
//!
//! fn siblings<'d, S: Savepoint<'d>>(mut save: S, a: Identifier, b: Identifier)
//!     -> impl Fiber<'d, Output = Result<(), reply::Error<'d>>>
//! {
//!     dope_fiber::fiber!('d => async move {
//!         let first = save.savepoint(a).await?;
//!         let second = save.savepoint(b).await?;
//!         second.release().await?;
//!         first.release().await
//!     })
//! }
//! ```
//!
//!
//! ```compile_fail,E0505
//! use cartel_pg::api::traits::{Listener, Notify as _};
//!
//! fn pending<'d, L: Listener<'d>>(listener: L) {
//!     let next = listener.notify().next();
//!     drop(listener.unlisten());
//!     drop(next);
//! }
//! ```
//!
//! ```compile_fail,E0505
//! use cartel_pg::api::traits::Transaction;
//!
//! fn token<'d, T: Transaction<'d>>(tx: T) {
//!     let token = tx.cancel_token();
//!     drop(tx.commit());
//!     drop(token);
//! }
//! ```
//!
//!
//! ```compile_fail,E0277
//! use cartel_pg::api::traits::{Client, copy::Transfer};
//!
//! fn cross_route<'d, C: Client<'d, 2>>(client: C) -> impl Transfer<'d, 1> {
//!     client.copy()
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_pg::{api, schema::QuerySet};
//!
//! fn escape<'app, I: QuerySet + 'static>(
//!     client: api::Client<'app, 'static, 'static, 'static, 'static, 'static, 'static, (), I>,
//! ) -> impl api::traits::Unchecked<'static> + 'static {
//!     api::traits::Client::unchecked(client)
//! }
//! ```
//!
//!
//! ```compile_fail
//! use cartel_pg::{api::traits::{Client, Unchecked as _}, reply};
//! use dope_fiber::abi::Fiber;
//!
//! fn cross_driver<'a, 'b, C: Client<'a>>(client: C)
//!     -> impl Fiber<'b, Output = Result<(), reply::Error<'b>>>
//! {
//!     client.unchecked().command("SELECT 1")
//! }
//! ```
//!

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

//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Arrange as _, Fetch as _, Set as _};
//! #[cartel_pg::query_group]
//! impl User {
//!     pub fn distinct_set() -> Vec<User> {
//!         User::filter(|row| row.id > 0)
//!             .union_all(User::filter(|row| row.id > 0))
//!             .distinct().all()
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! # mod common { pub mod types; }
//! use common::types::User;
//! use cartel_pg::dsl::query::ops::{Arrange as _, Reduce as _};
//! #[cartel_pg::query_group]
//! impl User {
//!     pub fn distinct_count() -> i64 {
//!         User::filter(|_row| true).distinct_on(|row| row.age).count()
//!     }
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
        &[
            "cartel-pg",
            "cartel-gen",
            "dope",
            "dope-fiber",
            "dope-test",
            "o3",
            "hmac",
            "sha2",
        ],
    );
    let exe = std::env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    assert_eq!(suite.try_errors(include_str!("contracts.rs"), compile)?, 30);
    let status = suite.try_rustdoc()?.status().expect("run rustdoc");
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
