//! Redis EVAL requests and their borrowed invocation contract.

use crate::api::{Client, decode::Decode};
use crate::request::{self, encode, encode::write::script};

pub use script::wire_len;

/// A complete Redis EVAL invocation whose backing bytes remain owned by the
/// caller until eager request encoding has completed.
pub trait Invocation {
    type Key: AsRef<[u8]>;
    type Argument: request::Bulk;

    fn script(&self) -> &[u8];
    fn keys(&self) -> &[Self::Key];
    fn arguments(&self) -> &[Self::Argument];

    fn wire_len(&self) -> Result<usize, request::Error> {
        script::length(self.script(), self.keys(), self.arguments()).map(encode::Length::get)
    }

    fn prepare(&self) -> Result<Prepared<'_, Self::Key, Self::Argument>, request::Error> {
        let (source, keys, args) = (self.script(), self.keys(), self.arguments());
        Ok(Prepared {
            length: script::length(source, keys, args)?,
            source,
            keys,
            args,
        })
    }
}

pub struct Prepared<'a, K, A> {
    source: &'a [u8],
    keys: &'a [K],
    args: &'a [A],
    length: encode::Length,
}

impl<K, A> Prepared<'_, K, A> {
    pub const fn wire_len(&self) -> std::num::NonZero<usize> {
        self.length.nonzero()
    }
}

impl<K: AsRef<[u8]>, A: request::Bulk> Prepared<'_, K, A> {
    fn encode(self, out: &mut encode::Writer) -> Result<(), request::Error> {
        script::eval(out, self.source, self.keys, self.args)
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Calls<C, const ID: u8 = 0>(pub(crate) C);

/// One reserved bulk frame, consumed by eager EVAL encoding.
pub trait Reserved<'d, C: Client<'d, ID>, const ID: u8 = 0>: crate::api::Sealed {
    fn eval_prepared<R: Decode<C>, K: AsRef<[u8]>, A: request::Bulk>(
        self,
        call: Prepared<'_, K, A>,
    ) -> C::Call<R>;
}

pub struct Reservation<C: o3::mem::Return<Item = o3::buffer::pool::Span>>(
    o3::mem::Lease<C, o3::mem::lease::Open>,
);

impl<C: o3::mem::Return<Item = o3::buffer::pool::Span>> Reservation<C> {
    pub(crate) const fn new(client: C, frame: o3::buffer::pool::Span) -> Self {
        Self(o3::mem::Lease::open(client, frame))
    }
}

impl<C: o3::mem::Return<Item = o3::buffer::pool::Span>> crate::api::Sealed for Reservation<C> {}

impl<'app, 's, 'b, 'c: 'b, 'd: 'app + 'c, B: ?Sized, const ID: u8>
    Reserved<'d, crate::client::Redis<'app, 's, 'b, 'c, 'd, B, ID>, ID>
    for Reservation<crate::client::Redis<'app, 's, 'b, 'c, 'd, B, ID>>
{
    fn eval_prepared<R, K, A>(
        self,
        call: Prepared<'_, K, A>,
    ) -> <crate::client::Redis<'app, 's, 'b, 'c, 'd, B, ID> as Client<'d, ID>>::Call<R>
    where
        R: Decode<crate::client::Redis<'app, 's, 'b, 'c, 'd, B, ID>>,
        K: AsRef<[u8]>,
        A: request::Bulk,
    {
        use crate::client::dispatch::command::{Command, Pooled};
        use cartel_core::frame::Sensitivity;

        let (client, frame) = self.0.into_parts();
        let frame = encode::Encoder::new(frame)
            .admit_with(call.length, Sensitivity::Plain, |out| call.encode(out))
            .inspect_err(|_| client.port.activity.wake_send());
        Command::new(Pooled::new(client), frame)
    }
}

impl<'d, C: Client<'d, ID>, const ID: u8> Calls<C, ID> {
    /// Waits for one bulk frame whose capacity covers `required`.
    pub fn try_reserve_bulk(
        self,
        required: std::num::NonZero<usize>,
    ) -> Result<
        impl dope_fiber::abi::Fiber<'d, Output = Result<C::Reserved, C::DriverError>>,
        request::Error,
    > {
        self.0.try_reserve_bulk(required)
    }

    pub fn eval_prepared<R, K, A>(self, call: Prepared<'_, K, A>) -> C::Call<R>
    where
        R: Decode<C>,
        K: AsRef<[u8]>,
        A: request::Bulk,
    {
        self.0
            .encode_sized::<R>(Ok(call.length), |out| call.encode(out))
    }

    /// Evaluates a script with a type-safe split between Redis keys and ARGV;
    /// the declared key count is derived from `keys`.
    pub fn eval<R, K, A>(self, script: &[u8], keys: &[K], args: &[A]) -> C::Call<R>
    where
        R: Decode<C>,
        K: AsRef<[u8]>,
        A: request::Bulk,
    {
        let len = script::length(script, keys, args);
        self.0
            .encode_sized::<R>(len, move |out| script::eval(out, script, keys, args))
    }

    /// Borrows one invocation's views once before eager length validation and encoding.
    pub fn eval_call<R, I>(self, call: &I) -> C::Call<R>
    where
        R: Decode<C>,
        I: Invocation,
    {
        self.eval::<R, _, _>(call.script(), call.keys(), call.arguments())
    }
}
