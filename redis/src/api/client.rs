use crate::{
    api::{Connection, Storage, decode::Decode},
    client::ops::{
        commands::{self, geo, strings},
        scripts,
    },
    request::{self, encode},
};
use dope_fiber::abi::Fiber;

/// One exact runtime-issued client, with storage represented by associated owners.
pub trait Client<'d, const ID: u8 = 0>: Storage + Copy {
    type Call<D: Decode<Self>>: Fiber<'d, Output = Result<D::Output, Self::Error>>;
    type Connection: Connection<'d, ID, Error = Self::Error, Bytes = Self::Bytes>;

    fn basic(self) -> strings::Basic<Self, ID> {
        strings::Basic(self)
    }

    fn hashes(self) -> commands::Hashes<Self, ID> {
        commands::Hashes(self)
    }

    fn sets(self) -> commands::Sets<Self, ID> {
        commands::Sets(self)
    }

    fn sorted_sets(self) -> commands::Sorted<Self, ID> {
        commands::Sorted(self)
    }

    fn streams(self) -> commands::Streams<Self, ID> {
        commands::Streams(self)
    }

    fn keyspace(self) -> commands::Keyspace<Self, ID> {
        commands::Keyspace(self)
    }

    fn server(self) -> commands::Server<Self, ID> {
        commands::Server(self)
    }

    fn scripts(self) -> scripts::Calls<Self, ID> {
        scripts::Calls(self)
    }

    fn lists(self) -> commands::Lists<Self, ID> {
        commands::Lists(self)
    }

    fn bits(self) -> strings::Bits<Self, ID> {
        strings::Bits(self)
    }

    fn hyperloglog(self) -> strings::Cardinal<Self, ID> {
        strings::Cardinal(self)
    }

    fn geo(self) -> geo::Geo<Self, ID> {
        geo::Geo(self)
    }

    fn connection(self) -> impl Fiber<'d, Output = Result<Self::Connection, Self::DriverError>>;
    fn wait_active(self) -> impl Fiber<'d, Output = Result<(), Self::DriverError>>;

    #[doc(hidden)]
    fn encode<D: Decode<Self>>(
        self,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D>;

    #[doc(hidden)]
    fn encode_sized<D: Decode<Self>>(
        self,
        len: Result<encode::Length, request::Error>,
        encode: impl FnOnce(encode::Writer) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D>;
}
