use cartel_pg::api::results::source::{Batch, Payload};
use cartel_pg::{api::results, reply, schema, schema::result};

pub enum Error<'d, const ID: u8 = 0> {
    Query(reply::Error<'d, ID>),
    Decode(cartel_pg::Error),
}

pub trait Convert<'d, const ID: u8 = 0> {
    type Owned;
    fn try_own(self) -> Result<Self::Owned, Error<'d, ID>>;
}

impl<'d, const ID: u8> Convert<'d, ID> for Result<(), reply::Error<'d, ID>> {
    type Owned = ();
    fn try_own(self) -> Result<(), Error<'d, ID>> {
        self.map_err(Error::Query)
    }
}

impl<'d, P: Payload, Q: schema::TypedQuery<Result = result::Rows>, const ID: u8> Convert<'d, ID>
    for Result<results::Row<P, Q>, reply::Error<'d, ID>>
{
    type Owned = Q::Row;
    fn try_own(self) -> Result<Self::Owned, Error<'d, ID>> {
        let row = self.map_err(Error::Query)?;
        row.into_owned().map_err(Error::Decode)
    }
}

impl<'d, B: Batch, Q: schema::TypedQuery<Result = result::Rows>, const ID: u8> Convert<'d, ID>
    for Result<results::Collection<B, Q>, reply::Error<'d, ID>>
{
    type Owned = Vec<Q::Row>;
    fn try_own(self) -> Result<Self::Owned, Error<'d, ID>> {
        let row = self.map_err(Error::Query)?;
        row.into_owned().map_err(Error::Decode)
    }
}

impl<'d, P: Payload, Q: schema::TypedQuery<Result = result::Rows>, const ID: u8> Convert<'d, ID>
    for Result<Option<results::Row<P, Q>>, reply::Error<'d, ID>>
{
    type Owned = Option<Q::Row>;
    fn try_own(self) -> Result<Self::Owned, Error<'d, ID>> {
        let row = self.map_err(Error::Query)?;
        row.map(results::Row::<_, Q>::into_owned)
            .transpose()
            .map_err(Error::Decode)
    }
}

impl<'d, P: Payload, Q: schema::TypedQuery<Result = result::Rows<true>>, const ID: u8>
    Convert<'d, ID> for Result<Option<results::Row<P, Q, true>>, reply::Error<'d, ID>>
{
    type Owned = Option<Option<Q::Row>>;
    fn try_own(self) -> Result<Self::Owned, Error<'d, ID>> {
        let row = self.map_err(Error::Query)?;
        row.map(results::Row::<_, Q, true>::into_owned)
            .transpose()
            .map_err(Error::Decode)
    }
}
