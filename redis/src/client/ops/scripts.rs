//! Redis EVAL requests and their borrowed invocation contract.

use crate::api::{Commands, decode::Decode};
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

impl<'d, C: Commands<'d, ID>, const ID: u8> Calls<C, ID> {
    pub fn eval_prepared<R, K, A>(self, call: Prepared<'_, K, A>) -> C::Call<R>
    where
        R: Decode<C::Client>,
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
        R: Decode<C::Client>,
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
        R: Decode<C::Client>,
        I: Invocation,
    {
        self.eval::<R, _, _>(call.script(), call.keys(), call.arguments())
    }
}
