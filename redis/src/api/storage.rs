use crate::{
    Error,
    api::Sealed,
    client,
    value::{
        self, Bytes,
        list::{self, bulks, coords, pair},
        reply, source,
    },
};

/// Reply owners retaining their original storage, credits, and route.
pub trait Storage: Sealed + Sized {
    type DriverError: source::Control;
    type Bytes: source::Bytes;
    type Error: source::Error<Bytes = Self::Bytes, DriverError = Self::DriverError>
        + From<Self::DriverError>
        + From<reply::Decode<Self::Bytes>>;
    type Bulks: source::Bulks<Bytes = Self::Bytes>;
    type Optional: source::Optional<Bytes = Self::Bytes>;
    type Pairs: source::Pairs<Bytes = Self::Bytes>;
    type Scan: source::Scan<Bulks = Self::Bulks>;
    type Reply: source::Reply;
    type Text: source::ScoredText;
    type Scored: source::Scored<Text = Self::Text>;
    type Coordinates: source::Coordinates;

    #[doc(hidden)]
    fn unit(reply: Self::Reply) -> Result<(), Self::Error>;
    #[doc(hidden)]
    fn signed(reply: Self::Reply) -> Result<i64, Self::Error>;
    #[doc(hidden)]
    fn unsigned(reply: Self::Reply) -> Result<u64, Self::Error>;
    #[doc(hidden)]
    fn boolean(reply: Self::Reply) -> Result<bool, Self::Error>;
    #[doc(hidden)]
    fn float(reply: Self::Reply) -> Result<f64, Self::Error>;
    #[doc(hidden)]
    fn optional_u64(reply: Self::Reply) -> Result<Option<u64>, Self::Error>;
    #[doc(hidden)]
    fn optional_f64(reply: Self::Reply) -> Result<Option<f64>, Self::Error>;
    #[doc(hidden)]
    fn coordinate(reply: Self::Reply) -> Result<client::GeoCoord, Self::Error>;
    #[doc(hidden)]
    fn maybe_bytes(reply: Self::Reply) -> Result<Option<Self::Bytes>, Self::Error>;
    #[doc(hidden)]
    fn bulks(reply: Self::Reply) -> Result<Self::Bulks, Self::Error>;
    #[doc(hidden)]
    fn optional(reply: Self::Reply) -> Result<Self::Optional, Self::Error>;
    #[doc(hidden)]
    fn pairs(reply: Self::Reply) -> Result<Self::Pairs, Self::Error>;
    #[doc(hidden)]
    fn scan(reply: Self::Reply) -> Result<Self::Scan, Self::Error>;
    #[doc(hidden)]
    fn scored(reply: Self::Reply) -> Result<Self::Scored, Self::Error>;
    #[doc(hidden)]
    fn coordinates(reply: Self::Reply) -> Result<Self::Coordinates, Self::Error>;
    #[doc(hidden)]
    fn bytes<const TYPE_NAME: bool>(reply: Self::Reply) -> Result<Self::Bytes, Self::Error>;
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Storage
    for client::Redis<'app, 's, 'b, 'c, 'd, B, ID>
{
    type DriverError = Error<'d, ID>;
    type Bytes = value::Bytes<'b, 'c, 'd>;
    type Error = reply::Failure<Error<'d, ID>, Self::Bytes>;
    type Bulks = bulks::Array<'s, 'b, 'c, 'd>;
    type Optional = bulks::Optional<'s, 'b, 'c, 'd>;
    type Pairs = pair::BulkPairs<'s, 'b, 'c, 'd>;
    type Scan = list::Scan<'s, 'b, 'c, 'd>;
    type Reply = value::Reply<'s, 'b, 'c, 'd>;
    type Text = pair::ScoredText<'s, 'b, 'c, 'd>;
    type Scored = pair::ScoredBulks<'s, 'b, 'c, 'd>;
    type Coordinates = coords::Coordinates<'s, 'b, 'c, 'd>;

    fn unit(reply: Self::Reply) -> Result<(), Self::Error> {
        <() as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn signed(reply: Self::Reply) -> Result<i64, Self::Error> {
        <i64 as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn unsigned(reply: Self::Reply) -> Result<u64, Self::Error> {
        <u64 as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn boolean(reply: Self::Reply) -> Result<bool, Self::Error> {
        <bool as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn float(reply: Self::Reply) -> Result<f64, Self::Error> {
        <f64 as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn optional_u64(reply: Self::Reply) -> Result<Option<u64>, Self::Error> {
        <Option<u64> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn optional_f64(reply: Self::Reply) -> Result<Option<f64>, Self::Error> {
        <Option<f64> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn coordinate(reply: Self::Reply) -> Result<client::GeoCoord, Self::Error> {
        <client::GeoCoord as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn maybe_bytes(reply: Self::Reply) -> Result<Option<Self::Bytes>, Self::Error> {
        <Option<value::Bytes<'b, 'c, 'd>> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn bulks(reply: Self::Reply) -> Result<Self::Bulks, Self::Error> {
        <bulks::Array<'s, 'b, 'c, 'd> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn optional(reply: Self::Reply) -> Result<Self::Optional, Self::Error> {
        <bulks::Optional<'s, 'b, 'c, 'd> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn pairs(reply: Self::Reply) -> Result<Self::Pairs, Self::Error> {
        <pair::BulkPairs<'s, 'b, 'c, 'd> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn scan(reply: Self::Reply) -> Result<Self::Scan, Self::Error> {
        <list::Scan<'s, 'b, 'c, 'd> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn scored(reply: Self::Reply) -> Result<Self::Scored, Self::Error> {
        <pair::ScoredBulks<'s, 'b, 'c, 'd> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn coordinates(reply: Self::Reply) -> Result<Self::Coordinates, Self::Error> {
        <coords::Coordinates<'s, 'b, 'c, 'd> as value::Decode<'s, 'b, 'c, 'd, ID>>::decode(reply)
    }

    fn bytes<const TYPE_NAME: bool>(reply: Self::Reply) -> Result<Self::Bytes, Self::Error> {
        Bytes::try_decode::<TYPE_NAME, ID>(reply)
    }
}
