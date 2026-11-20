use crate::{
    api::Sealed,
    client,
    value::{reply, source},
};

/// Reply owners retaining their original storage, credits, and route.
pub trait Storage: Sealed + Sized {
    type DriverError: source::Control + From<crate::request::Error>;
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
