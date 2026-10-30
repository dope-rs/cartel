//! Redis EVAL requests and their borrowed invocation contract.

use crate::api::{Client, decode::Decode};
use crate::request::{self, encode, encode::write::script};

/// A complete Redis EVAL invocation whose backing bytes remain owned by the
/// caller until eager request encoding has completed.
pub trait Invocation {
    type Key: AsRef<[u8]>;
    type Argument: request::Bulk;

    fn script(&self) -> &[u8];
    fn keys(&self) -> &[Self::Key];
    fn arguments(&self) -> &[Self::Argument];

    fn wire_len(&self) -> Result<usize, request::Error> {
        script::Writer::length(self.script(), self.keys(), self.arguments())
            .map(encode::Length::get)
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Calls<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Calls<C, ID> {
    /// Evaluates a script with a type-safe split between Redis keys and ARGV;
    /// the declared key count is derived from `keys`.
    pub fn eval<R, K, A>(self, script: &[u8], keys: &[K], args: &[A]) -> C::Call<R>
    where
        R: Decode<C>,
        K: AsRef<[u8]>,
        A: request::Bulk,
    {
        let len = script::Writer::length(script, keys, args);
        self.0.encode_sized::<R>(len, move |out| {
            script::Writer::new(out).eval(script, keys, args)
        })
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
