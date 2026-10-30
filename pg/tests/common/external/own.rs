use cartel_pg::api::results::source::{Batch, Payload};
use cartel_pg::{api::results, reply, schema, schema::result};

pub enum Error<'d> {
    Query(reply::Error<'d>),
    Decode(cartel_pg::Error),
}

pub trait Convert<'d> {
    type Owned;
    fn try_own(self) -> Result<Self::Owned, Error<'d>>;
}

impl<'d> Convert<'d> for Result<(), reply::Error<'d>> {
    type Owned = ();
    fn try_own(self) -> Result<(), Error<'d>> {
        self.map_err(Error::Query)
    }
}

impl<'d, P: Payload, Q: schema::TypedQuery<Result = result::Rows>> Convert<'d>
    for Result<results::Row<P, Q>, reply::Error<'d>>
{
    type Owned = Q::Row;
    fn try_own(self) -> Result<Self::Owned, Error<'d>> {
        let row = self.map_err(Error::Query)?;
        row.into_owned().map_err(Error::Decode)
    }
}

impl<'d, B: Batch, Q: schema::TypedQuery<Result = result::Rows>> Convert<'d>
    for Result<results::Collection<B, Q>, reply::Error<'d>>
{
    type Owned = Vec<Q::Row>;
    fn try_own(self) -> Result<Self::Owned, Error<'d>> {
        let row = self.map_err(Error::Query)?;
        row.into_owned().map_err(Error::Decode)
    }
}

impl<'d, P: Payload, Q: schema::TypedQuery<Result = result::Rows>> Convert<'d>
    for Result<Option<results::Row<P, Q>>, reply::Error<'d>>
{
    type Owned = Option<Q::Row>;
    fn try_own(self) -> Result<Self::Owned, Error<'d>> {
        let row = self.map_err(Error::Query)?;
        row.map(results::Row::into_owned)
            .transpose()
            .map_err(Error::Decode)
    }
}
