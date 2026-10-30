use crate::api::{Client, decode};
use crate::client::nonempty;
use crate::request::encode::write::{key, space, value::bits};
use std::num;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Basic<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Basic<C, ID> {
    pub fn get(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| key::Writer::new(out).get(key))
    }

    pub fn set(self, key: &[u8], value: &[u8]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| key::Writer::new(out).set(key, value))
    }

    pub fn set_seconds(self, key: &[u8], value: &[u8], ttl: num::NonZero<u64>) -> C::Call<()> {
        let seconds = ttl.get();
        self.0
            .encode::<()>(move |out| key::Writer::new(out).set_ex(key, value, seconds))
    }

    pub fn set_millis(self, key: &[u8], value: &[u8], ttl: num::NonZero<u64>) -> C::Call<()> {
        let millis = ttl.get();
        self.0
            .encode::<()>(move |out| key::Writer::new(out).set_px(key, value, millis))
    }

    pub fn set_absent(self, key: &[u8], value: &[u8]) -> C::Call<bool> {
        self.0
            .encode::<bool>(move |out| key::Writer::new(out).set_nx(key, value))
    }

    pub fn del(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let keys = keys.as_slice();
        self.0.encode::<u64>(move |out| out.slices(&[b"DEL"], keys))
    }

    /// Encodes keys directly; the call retains no iterator or input.
    /// Both traversals must match the declared count before submission.
    pub fn del_iter<I, K>(self, count: num::NonZero<usize>, keys: I) -> C::Call<u64>
    where
        I: Iterator<Item = K> + Clone,
        K: AsRef<[u8]>,
    {
        self.0
            .encode::<u64>(move |out| key::Writer::new(out).del(count.get(), keys))
    }

    pub fn exists(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let keys = keys.as_slice();
        self.0
            .encode::<u64>(move |out| key::Writer::new(out).exists(keys))
    }

    pub fn incr(self, key: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| key::Writer::new(out).incr(key))
    }

    pub fn decr(self, key: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| key::Writer::new(out).decr(key))
    }

    pub fn incr_by(self, key: &[u8], by: i64) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| key::Writer::new(out).incr_by(key, by))
    }

    pub fn incr_float(self, key: &[u8], by: f64) -> C::Call<f64> {
        self.0
            .encode::<f64>(move |out| key::Writer::new(out).incr_float(key, by))
    }

    pub fn expire(self, key: &[u8], ttl: num::NonZero<u64>) -> C::Call<bool> {
        let seconds = ttl.get();
        self.0
            .encode::<bool>(move |out| space::Writer::new(out).expire(key, seconds))
    }

    pub fn ttl(self, key: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| space::Writer::new(out).ttl(key))
    }

    pub fn mget(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<decode::Optional> {
        let keys = keys.as_slice();
        self.0
            .encode::<decode::Optional>(move |out| key::Writer::new(out).mget(keys))
    }

    pub fn mset(self, kv: nonempty::Slice<'_, (&[u8], &[u8])>) -> C::Call<()> {
        let kv = kv.as_slice();
        self.0
            .encode::<()>(move |out| key::Writer::new(out).mset(kv))
    }
}

/// An op combining one or more source strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    And,
    Or,
    Xor,
}

impl Operation {
    const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::And => b"AND",
            Self::Or => b"OR",
            Self::Xor => b"XOR",
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Bits<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Bits<C, ID> {
    pub fn count(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| bits::Writer::new(out).count(key))
    }

    /// Counts set bits in the inclusive byte range.
    pub fn count_range(self, key: &[u8], start: i64, end: i64) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| bits::Writer::new(out).count_range(key, start, end))
    }

    /// Sets one bit and returns its previous value.
    pub fn set(self, key: &[u8], offset: u64, value: bool) -> C::Call<decode::Changed> {
        self.0
            .encode::<decode::Changed>(move |out| bits::Writer::new(out).set(key, offset, value))
    }

    pub fn get(self, key: &[u8], offset: u64) -> C::Call<decode::Changed> {
        self.0
            .encode::<decode::Changed>(move |out| bits::Writer::new(out).get(key, offset))
    }

    pub fn combine(
        self,
        op: Operation,
        dest: &[u8],
        sources: nonempty::Slice<'_, &[u8]>,
    ) -> C::Call<u64> {
        let sources = sources.as_slice();
        self.0
            .encode::<u64>(move |out| bits::Writer::new(out).combine(op.as_bytes(), dest, sources))
    }

    pub fn not(self, dest: &[u8], source: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| bits::Writer::new(out).not(dest, source))
    }

    pub fn position(self, key: &[u8], value: bool) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| bits::Writer::new(out).position(key, value))
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Cardinal<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Cardinal<C, ID> {
    pub fn add(self, key: &[u8], elements: &[&[u8]]) -> C::Call<decode::Changed> {
        self.0
            .encode::<decode::Changed>(move |out| out.slices(&[b"PFADD", key], elements))
    }

    pub fn count(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let keys = keys.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"PFCOUNT"], keys))
    }

    pub fn merge(self, dest: &[u8], sources: &[&[u8]]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| out.slices(&[b"PFMERGE", dest], sources))
    }
}
