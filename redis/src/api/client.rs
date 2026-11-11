use crate::{
    api::{Connection, Storage, decode::Decode},
    client::ops::{
        commands::{self, geo, strings},
        scripts,
    },
    request::{self, encode},
};
use cartel_core::{deadline, frame::Sensitivity};
use dope_fiber::abi::Fiber;
use o3::{
    buffer::pool,
    mem::{Lease, Return, lease},
};
use std::num::NonZero;

pub trait Client<'d, const ID: u8 = 0>:
    Storage + Copy + Commands<'d, ID, Client = Self> + Return<Item = pool::Span>
{
    type Connection: Connection<'d, ID, Error = Self::Error, Bytes = Self::Bytes>;

    fn connection(self) -> impl Fiber<'d, Output = Result<Self::Connection, Self::DriverError>>;
    fn wait_active(self) -> impl Fiber<'d, Output = Result<(), Self::DriverError>>;
    fn acquire(self) -> impl Fiber<'d, Output = Result<Acquired<'d, Self>, Self::DriverError>>;
    fn try_acquire_bulk(
        self,
        required: NonZero<usize>,
    ) -> Result<
        impl Fiber<'d, Output = Result<Acquired<'d, Self>, Self::DriverError>>,
        request::Error,
    >;

    #[doc(hidden)]
    fn encode_acquired<D: Decode<Self>>(
        request: Acquired<'d, Self>,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D>;
}

pub trait Commands<'d, const ID: u8 = 0>: crate::api::Sealed + Sized {
    type Client: Storage;
    type Call<D: Decode<Self::Client>>: Fiber<'d, Output = Result<D::Output, <Self::Client as Storage>::Error>>;

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

    #[doc(hidden)]
    fn encode<D: Decode<Self::Client>>(
        self,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D>;

    #[doc(hidden)]
    fn encode_sized<D: Decode<Self::Client>>(
        self,
        length: Result<encode::Length, request::Error>,
        encode: impl FnOnce(&mut encode::Writer) -> Result<(), request::Error>,
    ) -> Self::Call<D> {
        self.encode(|out| out.admit_with(length?, Sensitivity::Plain, encode))
    }
}

pub struct Acquired<'d, C: Return<Item = pool::Span>> {
    pub(crate) frame: Lease<C, lease::Open>,
    pub(crate) deadline: deadline::Guard<'d>,
}

impl<C: Return<Item = pool::Span>> crate::api::Sealed for Acquired<'_, C> {}

impl<'d, C: Client<'d, ID>, const ID: u8> Commands<'d, ID> for Acquired<'d, C> {
    type Client = C;
    type Call<D: Decode<C>> = C::Call<D>;

    fn encode<D: Decode<C>>(
        self,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D> {
        C::encode_acquired(self, encode)
    }
}
