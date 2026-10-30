//! Static result decoders for a client's exact retained storage family.

use crate::{
    Error,
    api::Storage,
    client,
    value::{
        self,
        list::{self, bulks, coords, pair},
        reply,
    },
};
use std::marker::PhantomData;

/// Decodes retained storage after response bookkeeping completes in the same poll.
/// The reply retains RESP errors until the decoder classifies them.
pub trait Decode<S: Storage> {
    type Output;
    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error>;
}

/// Adapts a normalized decoder to an eagerly encoded command's exact storage.
#[doc(hidden)]
#[derive(Debug)]
pub struct Adapter<S, D>(PhantomData<fn() -> (S, D)>);

impl<'app, 's, 'b, 'c: 'b, 'd: 'app + 'c, B: ?Sized, D, const ID: u8>
    value::Decode<'s, 'b, 'c, 'd, ID> for Adapter<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>, D>
where
    D: Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>>,
{
    type Output = D::Output;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self::Output, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        D::decode(reply)
    }
}

impl<S: Storage> Decode<S> for () {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::unit(reply)
    }
}

impl<S: Storage> Decode<S> for i64 {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::signed(reply)
    }
}

impl<S: Storage> Decode<S> for u64 {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::unsigned(reply)
    }
}

impl<S: Storage> Decode<S> for bool {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::boolean(reply)
    }
}

impl<S: Storage> Decode<S> for f64 {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::float(reply)
    }
}

impl<S: Storage> Decode<S> for Option<u64> {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::optional_u64(reply)
    }
}

impl<S: Storage> Decode<S> for Option<f64> {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::optional_f64(reply)
    }
}

impl<S: Storage> Decode<S> for client::GeoCoord {
    type Output = Self;

    fn decode(reply: S::Reply) -> Result<Self, S::Error> {
        S::coordinate(reply)
    }
}

#[derive(Debug)]
pub struct Bytes;

impl<S: Storage> Decode<S> for Bytes {
    type Output = S::Bytes;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::bytes::<false>(reply)
    }
}

#[derive(Debug)]
pub struct MaybeBytes;

impl<S: Storage> Decode<S> for MaybeBytes {
    type Output = Option<S::Bytes>;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::maybe_bytes(reply)
    }
}

#[derive(Debug)]
pub struct Bulks;

impl<S: Storage> Decode<S> for Bulks {
    type Output = S::Bulks;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::bulks(reply)
    }
}

#[derive(Debug)]
pub struct Optional;

impl<S: Storage> Decode<S> for Optional {
    type Output = S::Optional;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::optional(reply)
    }
}

#[derive(Debug)]
pub struct Pairs;

impl<S: Storage> Decode<S> for Pairs {
    type Output = S::Pairs;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::pairs(reply)
    }
}

#[derive(Debug)]
pub struct Scan;

impl<S: Storage> Decode<S> for Scan {
    type Output = S::Scan;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::scan(reply)
    }
}

#[derive(Debug)]
pub struct Reply;

impl<S: Storage> Decode<S> for Reply {
    type Output = S::Reply;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        Ok(reply)
    }
}

#[derive(Debug)]
pub struct Scored;

impl<S: Storage> Decode<S> for Scored {
    type Output = S::Scored;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::scored(reply)
    }
}

#[derive(Debug)]
pub struct Coordinates;

impl<S: Storage> Decode<S> for Coordinates {
    type Output = S::Coordinates;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::coordinates(reply)
    }
}

#[derive(Debug)]
pub struct Changed;

impl<S: Storage> Decode<S> for Changed {
    type Output = bool;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::signed(reply).map(|value| value != 0)
    }
}

#[derive(Debug)]
pub struct Kind;

impl<S: Storage> Decode<S> for Kind {
    type Output = S::Bytes;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        S::bytes::<true>(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for value::Bytes<'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for Option<value::Bytes<'b, 'c, 'd>>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>>
    for (value::Bytes<'b, 'c, 'd>, value::Bytes<'b, 'c, 'd>)
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for bulks::Array<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for bulks::Optional<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for pair::BulkPairs<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for pair::ScoredBulks<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for coords::Coordinates<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for list::Scan<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Decode<client::Redis<'app, 's, 'b, 'c, 'd, B, ID>> for value::Reply<'s, 'b, 'c, 'd>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'b, 'c, 'd>,
    ) -> Result<Self, reply::Failure<Error<'d, ID>, value::Bytes<'b, 'c, 'd>>> {
        <Self as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }
}
