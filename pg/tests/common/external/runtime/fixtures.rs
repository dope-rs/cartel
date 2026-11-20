use super::{Harness, Limits, Session};
use crate::common::{
    env,
    external::{
        Db,
        seed::{ROWS, SCHEMA},
    },
    port::{Queries, Reply},
};
use cartel_pg::api::traits::Query as _;
use dope::core::random::Seed;
use dope::runtime::executor::drive;
use dope_fiber::BlockOn as _;
use std::{ffi::OsString, iter, marker::PhantomData, num::NonZero};

pub fn run(
    values: [Option<OsString>; 6],
    limits: Limits,
    f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
        &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, Db>,
    ) -> Result<
        (),
        drive::Error,
    >,
) {
    let input = env::Env::try_load(values).expect("PostgreSQL environment");
    let config = input.try_config().expect("PostgreSQL config");
    let boot = input.try_config().expect("PostgreSQL seed config");
    let name = format!("cartel_pg_test_{}", try_suffix().expect("fixture suffix"));
    let address = input.address();
    Session::<Queries>::new(address, boot, NonZero::<usize>::MIN, name.clone(), false)
        .try_run(|rt| {
            rt.try_ready()?;
            let client = rt.client();
            let create = format!("CREATE SCHEMA {}", rt.schema);
            rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                for sql in iter::once(create.as_str()).chain(SCHEMA.iter().chain(ROWS).copied()) {
                    let result = client.command(sql).await;
                    assert!(result.is_ok(), "{sql}: {:?}", result.as_ref().err().map(Reply::from));
                }
            }))
        })
        .expect("seed PostgreSQL schema");
    Session::<Db> {
        address,
        config,
        limits,
        schema: name,
        cleanup: true,
        marker: PhantomData,
    }
    .try_run(|rt| {
        rt.try_ready()?;
        f(rt)
    })
    .expect("PostgreSQL runtime");
}

pub fn try_suffix() -> Result<String, env::Error> {
    let seed = Seed::acquire().map_err(env::Error::Entropy)?;
    let [first, second] = *seed.as_ref();
    Ok(format!("{first:016x}{second:016x}"))
}
