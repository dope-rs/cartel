use cartel_pg::api::traits::{Client as _, Query as _, Status as _};
use dope::core::driver::life::recovery;
use dope_fiber::BlockOn as _;
use std::num::NonZero;

use cartel_pg::{
    protocol::{encode, value::encode::BindWriter},
    schema::{self, groups, result},
};
use dope_fiber::task::time::sleep;
use dope_test::fibers;

use crate::common::env;

use crate::common::external::{
    own,
    own::Convert as _,
    runtime::{self, Session},
};

pub struct Observer;

impl schema::TypedQuery for Observer {
    type Params<'p> = i32;
    type Row = bool;
    type Result = result::Rows;
    type Group = Observer;

    const STATEMENT_NAME: &str = "sleeping";
    const SQL: &str = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1 AND state = 'active' AND wait_event_type = 'Timeout' AND wait_event = 'PgSleep')";
    const PARAM_OIDS: &[u32] = &[23];
    const N_PARAMS: i16 = 1;
    const N_RESULT_COLS: u16 = 1;

    fn encode(pid: i32, writer: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        writer.scalar().write_i32(pid)
    }
}

impl schema::QueryGroup for Observer {
    type Items = groups::Item<Self, groups::Empty>;
    const QUERIES: &[schema::QueryMeta<'_>] = &[schema::QueryMeta::fixed::<Self>()];
}

impl schema::QuerySet for Observer {
    type Groups = groups::Cons<Self, groups::End>;
    const QUERIES: &[&[schema::QueryMeta<'_>]] = &[<Self as schema::QueryGroup>::QUERIES];
    const PREPARE: schema::Prepare<'_> = {
        const LEN: usize = schema::Prepare::fixed_len::<Observer>();
        static WIRE: schema::Wire<'_, LEN> = schema::Prepare::fixed::<Observer, LEN>();
        WIRE.prepare()
    };
}

impl schema::HasGroup<Observer> for Observer {}

type Observed<'d> = Result<Option<Result<(), own::Error<'d>>>, sleep::Error>;

impl Observer {
    /// Each SELECT ends at Sync, so activity is read in a fresh transaction.
    pub fn observe<R>(
        input: env::Env,
        pid: i32,
        inspect: impl for<'d> FnOnce(Observed<'d>),
        recover: impl recovery::Recover + Copy,
        check: impl Fn(runtime::Check<'_, '_, R>) -> R,
    ) -> Result<R, runtime::Error<R>> {
        Session::<Self>::new(
            input.address(),
            input.try_config().map_err(runtime::Error::Environment)?,
            NonZero::<usize>::MIN,
            "public".to_owned(),
            false,
        )
        .try_run(
            |rt| {
                let client = rt.client();
                let observed = rt.runtime.block_on(fibers::TEST.within(
                    dope_fiber::fiber!('_ => async move {
                        client.status().wait_ready().await.map_err(own::Error::Query)?;
                        loop {
                            if client.query().run_one::<Self>(pid).await.try_own()? {
                                return Ok::<(), own::Error<'_>>(());
                            }
                        }
                    }),
                ))?;
                inspect(observed);
                Ok(())
            },
            recover,
            check,
        )
    }
}
