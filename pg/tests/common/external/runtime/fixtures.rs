use crate::common::external::runtime::Plan;
use crate::{
    common::env, common::external::Db, common::external::seed::ROWS,
    common::external::seed::SCHEMA, common::port::Queries,
};
use dope::core::random::Seed;

impl Plan<'_, Db, Queries> {
    pub fn try_new(input: &env::Env) -> Result<Self, env::Error> {
        Ok(Plan::new(
            input.address(),
            input.try_config()?,
            input.try_config()?,
            SCHEMA,
            ROWS,
            format!("cartel_pg_test_{}", try_suffix()?),
        ))
    }
}

pub fn try_suffix() -> Result<String, env::Error> {
    let seed = Seed::acquire().map_err(env::Error::Entropy)?;
    let [first, second] = *seed.as_ref();
    Ok(format!("{first:016x}{second:016x}"))
}
