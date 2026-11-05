//! Compile-time ownership and API contracts.
//!
//! ```compile_fail,E0308
//! use cartel_redis::Error;
//! fn relabel<'d>(error: Error<'d, 1>) -> Error<'d, 2> { error }
//! ```
//!
//! ```compile_fail
//! use cartel_redis::Error;
//! fn shorten<'a: 'b, 'b>(error: Error<'a, 17>) -> Error<'b, 17> { error }
//! ```
//!
//! ```compile_fail,E0599
//! use cartel_redis::Port;
//!
//! fn raw(port: &Port<'_, '_, '_, '_, '_>) {
//!     let _ = port.redis();
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_redis::{api::Client, client::Redis};
//! use dope_fiber::abi;
//!
//! fn escape_app<'app>(
//!     redis: Redis<'app, 'static, 'static, 'static, 'static, ()>,
//! ) -> impl abi::Fiber<'static, Output = Result<(), cartel_redis::Error<'static>>> + 'static {
//!     Client::wait_active(redis)
//! }
//! ```
//!
//! ```compile_fail,E0382
//! use cartel_redis::client::conn::Connection;
//!
//! fn cannot_copy<B: ?Sized>(connection: Connection<'_, '_, '_, '_, '_, B>) {
//!     let moved = connection;
//!     let _again = connection;
//!     drop(moved);
//! }
//! ```
//!
//! ```compile_fail,E0277
//! use cartel_redis::protocol::parser::storage::Storage;
//! fn send<T: Send>() {}
//! send::<Storage<'static>>();
//! ```
//!
//! ```compile_fail,E0277
//! use cartel_redis::protocol::parser::storage::Storage;
//! fn shared<T: Sync>() {}
//! shared::<Storage<'static>>();
//! ```
//!
//! ```compile_fail,E0308
//! use cartel_redis::{Error, value::{Bytes, reply::Failure}};
//! fn relabel<'a, 'c, 'd>(
//!     error: Failure<Error<'d, 1>, Bytes<'a, 'c, 'd>>,
//! ) -> Failure<Error<'d, 2>, Bytes<'a, 'c, 'd>> { error }
//! ```
//!
//! ```
//! use cartel_redis::{api::Client, client::nonempty};
//! use cartel_redis::value::source::ScoredText;
//!
//! fn commands<'d, C: Client<'d, 17>>(client: C) {
//!     let key: &[u8] = b"board";
//!     let _ = client.basic().incr(key);
//!     let _ = client.basic().del(nonempty::Slice::from_ref(&key));
//!     let _ = client.sorted_sets().add(key, 42.5, b"alice");
//!     let _ = client.sorted_sets().len(key);
//!     let _ = client.sorted_sets().rev_range(key, 0, -1);
//!     let _ = client.sorted_sets().reverse_rank(key, b"alice");
//! }
//!
//! fn first<S: ScoredText>(rows: &S) -> Option<&str> {
//!     rows.iter().next().map(|(member, _)| member)
//! }
//! ```
//!
//! ```compile_fail,E0277
//! use cartel_redis::{api::Client, client::Redis};
//!
//! fn route<'d>(_: impl Client<'d, 1>) {}
//!
//! fn relabel<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized>(
//!     client: Redis<'app, 's, 'b, 'c, 'd, B, 2>,
//! ) {
//!     route(client);
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_redis::value::source::ScoredText;
//!
//! fn escape<S: ScoredText + 'static>(rows: S) -> &'static str {
//!     rows.iter().next().expect("member").0
//! }
//! ```

//!
//! Command futures retain no encoded inputs: generic members, script keys, or arguments.
//!
//! ```
//! use cartel_redis::api::{Client, Storage, decode::{self, Decode}};
//! use cartel_redis::client::nonempty;
//! use cartel_redis::value::source::{Bulks as _, Pairs as _, Scan as _};
//!
//! fn commands<'d, C: Client<'d, 17>>(client: C) {
//!     let key = String::from("key");
//!     let value = String::from("value");
//!     let set = client.basic().set(key.as_bytes(), value.as_bytes());
//!     let add = client.sorted_sets().add(key.as_bytes(), 1.0, value.as_bytes());
//!     let hash = client.hashes().set(key.as_bytes(), b"field", value.as_bytes());
//!     let pairs = [(b"field".as_slice(), value.as_bytes())];
//!     let hash_iter = client.hashes().set_iter(key.as_bytes(), std::num::NonZero::new(1).expect("one"), pairs.into_iter());
//!     let keys = [key.as_bytes()];
//!     let members = nonempty::Slice::new(&keys).expect("keys");
//!     let set_add = client.sets().add(key.as_bytes(), members);
//!     let set_iter = client.sets().add_iter(key.as_bytes(), std::num::NonZero::<usize>::MIN, keys.into_iter());
//!     let del_iter = client.basic().del_iter(std::num::NonZero::<usize>::MIN, keys.into_iter());
//!     let script = String::from("return 1");
//!     let args = [value.as_bytes()];
//!     let eval = client.scripts().eval::<u64, _, _>(script.as_bytes(), &keys, &args);
//!     let scan = client.keyspace().scan(0, Some(key.as_bytes()), None);
//!     let publish = client.server().publish(key.as_bytes(), value.as_bytes());
//!     drop((key, value, script));
//!     drop((set, add, hash, hash_iter, set_add, set_iter, del_iter, eval, scan, publish));
//! }
//!
//! fn exact_decoder<'d, C, D>(client: C)
//! where C: Client<'d, 17>, D: Decode<C> {
//!     let _ = client.scripts().eval::<D, &[u8], &[u8]>(b"return 1", &[], &[]);
//! }
//!
//! fn concrete<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized>(
//!     client: cartel_redis::client::Redis<'app, 's, 'b, 'c, 'd, B, 17>,
//! ) {
//!     let _ = client.scripts().eval::<decode::Bulks, &[u8], &[u8]>(b"return {}", &[], &[]);
//! }
//!
//! fn owners<S: Storage>(bytes: &S::Bytes, rows: &S::Bulks, pairs: &S::Pairs, scan: &S::Scan) {
//!     let _ = bytes.as_ref();
//!     let _ = rows.iter();
//!     let _ = pairs.iter();
//!     let _ = scan.keys().iter();
//! }
//! ```
//!
//! Restored command families preserve eager input encoding and exact result owners.
//!
//! ```
//! use cartel_redis::api::Client;
//! use cartel_redis::client::{nonempty, ops::commands::{strings::Operation, geo::Unit}};
//!
//! fn commands<'d, C: Client<'d, 17>>(client: C) {
//!     let key = String::from("key");
//!     let value = String::from("value");
//!     let items = [value.as_bytes()];
//!     let nonempty = nonempty::Slice::new(&items).expect("items");
//!     let ttl = std::num::NonZero::<u64>::MIN;
//!     let milliseconds = client.basic().set_millis(key.as_bytes(), value.as_bytes(), ttl);
//!     let conditional = client.basic().set_absent(key.as_bytes(), value.as_bytes());
//!     let expiry = client.basic().expire(key.as_bytes(), ttl);
//!     let increment = client.basic().incr_float(key.as_bytes(), 1.25);
//!     let scored = client.sorted_sets().scored_range(key.as_bytes(), 0, -1);
//!     let range = client.sorted_sets().score_range(key.as_bytes(), 0.0, 1.0);
//!     let push = client.lists().push_front(key.as_bytes(), nonempty);
//!     let pop = client.lists().pop_back(key.as_bytes());
//!     let count = client.bits().count(key.as_bytes());
//!     let combine = client.bits().combine(Operation::And, key.as_bytes(), nonempty);
//!     let estimate = client.hyperloglog().add(key.as_bytes(), &items);
//!     let coordinates = client.geo().positions(key.as_bytes(), &items);
//!     let distance = client.geo().distance(key.as_bytes(), value.as_bytes(), b"other", Some(Unit::Meters));
//!     drop((key, value));
//!     drop((milliseconds, conditional, expiry, increment, scored, range, push, pop, count, combine, estimate, coordinates, distance));
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_redis::value::source::Coordinates;
//! fn escape<S: Coordinates + 'static>(owner: S) -> impl Iterator<Item = Option<cartel_redis::client::GeoCoord>> + 'static {
//!     owner.iter()
//! }
//! ```
//!
//! ```
//! use cartel_redis::api::Client;
//! use cartel_redis::client::ops::scripts::Invocation;
//!
//! fn encoded<'d, C, I>(client: C, invocation: I)
//! where C: Client<'d, 17>, I: Invocation {
//!     let call = client.scripts().eval_call::<u64, _>(&invocation);
//!     drop(invocation);
//!     drop(call);
//! }
//! ```
//!
//! ```
//! use cartel_redis::value::{reply, source::Optional};
//! fn pair<S: Optional>(owner: &S) -> Result<[Option<&[u8]>; 2], reply::Decode<S::Bytes>> {
//!     owner.try_array::<2>()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_redis::value::source::Optional;
//! fn escape<S: Optional + 'static>(owner: S) -> [Option<&'static [u8]>; 2] {
//!     owner.try_array::<2>().ok().expect("pair")
//! }
//! ```
//!
//! ```compile_fail,E0499
//! use cartel_redis::api::Connection;
//! fn overlap<'d, C: Connection<'d, 17>>(mut connection: C) {
//!     let first = connection.ping();
//!     let second = connection.client_id();
//!     drop((first, second));
//! }
//! ```
//!
//! ```
//! use cartel_redis::value::{list::pair, source::{Bulks, Chunks as _, Exact as _}};
//! fn checked<A: Bulks>(owner: A) -> Result<usize, pair::VisitError<A::Bytes, (), ()>> {
//!     let chunks = owner.try_chunks::<3>().ok().expect("triples");
//!     let owner = chunks.try_visit(|[_a, _b, _c]| Ok(()))?.into_bulks();
//!     let exact = owner.try_exact::<3>().ok().expect("one triple");
//!     exact.try_map(|[a, b, c]| Ok(a.len() + b.len() + c.len()))
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use cartel_redis::value::source::Exact;
//! fn escape<A: Exact<3> + 'static>(owner: A) -> [&'static [u8]; 3] {
//!     owner.as_array()
//! }
//! ```
//!
//! ```compile_fail
//! use cartel_redis::value::source::Exact;
//! fn escape<A: Exact<1> + 'static>(owner: A) -> &'static [u8] {
//!     owner.try_map::<_, (), ()>(|[value]| Ok(value)).ok().expect("value")
//! }
//! ```
//!
//! ```compile_fail,E0080
//! use cartel_redis::value::list::bulks::Select;
//! const INVALID: Select<(), (), 0> = Select::domain(());
//! ```
//!
//! ```compile_fail,E0080
//! use cartel_redis::value::list::bulks::Select;
//! const INVALID: Select<(), (), 2> = Select::at::<2>(());
//! ```

#[test]
fn contracts() -> Result<(), dope_test::checks::Error> {
    use std::env;

    use dope_test::checks::Suite;

    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["cartel-redis", "dope-fiber"],
    );
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let artifacts = suite.try_collect(&exe, output)?;
    assert_eq!(
        artifacts.try_errors(include_str!("contracts.rs"), compile)?,
        14
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
