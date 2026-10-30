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
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: QuerySet + 'd>(
//!     tx: api::tx::Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> api::ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I> {
//!     tx.query()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Transaction as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: QuerySet + 'd>(
//!     tx: api::tx::Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> Option<api::tx::control::CancelToken<'app, 'd>> {
//!     tx.cancel_token()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Savepoint as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: QuerySet + 'd>(
//!     save: api::tx::control::Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> api::ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I> {
//!     save.query()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_pg::{api, api::traits::Listener as _, schema::QuerySet};
//!
//! fn escape<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: QuerySet + 'd>(
//!     listener: api::tx::control::ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I>,
//! ) -> api::ops::Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I> {
//!     listener.query()
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use cartel_pg::{api::ops::Query, schema::QuerySet};
//!
//! fn cross_route<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: QuerySet + 'd>(
//!     query: Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, 2>,
//! ) -> Query<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, 1> {
//!     query
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_pg::{api::ops::Query, schema::QuerySet};
//!
//! fn escape_driver<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: QuerySet + 'static>(
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
//! fn escape<'view, 'b, 'c, 'd, Q: TypedQuery<Row = String>>(
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
//! fn escape<'view, P: Payload, Q>(row: Validated<results::Row<P, Q>>) -> &'view str
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
//! fn escape<'view, 'd, S: Stream<'d>>(chunk: S::Chunk) -> &'view [u8] {
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
#[test]
fn contracts() {
    use std::env;

    use dope_test::checks::Suite;
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["cartel-pg", "dope-fiber"],
    );
    let exe = env::current_exe().expect("test executable");
    let status = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))
        .expect("build command")
        .status()
        .expect("build original library");
    assert!(status.success(), "build original library: {status}");
    assert_eq!(
        suite
            .try_errors(&exe, include_str!("contracts.rs"))
            .expect("exact compiler errors"),
        21
    );
    let status = suite
        .try_rustdoc(&exe)
        .expect("rustdoc command")
        .status()
        .expect("compiler contracts");
    assert!(status.success(), "compile-time contracts: {status}");
}
