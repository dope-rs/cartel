use crate::api::{Client, decode};
use crate::client::nonempty;
use crate::request::encode;
use std::num;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Basic<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Basic<C, ID> {
    pub fn get(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| out.command(&[b"GET", key]))
    }

    pub fn set(self, key: &[u8], value: &[u8]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| out.command(&[b"SET", key, value]))
    }

    pub fn set_seconds(self, key: &[u8], value: &[u8], ttl: num::NonZero<u64>) -> C::Call<()> {
        let seconds = ttl.get();
        self.0
            .encode::<()>(move |out| out.set_ttl(key, value, b"EX", seconds))
    }

    pub fn set_millis(self, key: &[u8], value: &[u8], ttl: num::NonZero<u64>) -> C::Call<()> {
        let millis = ttl.get();
        self.0
            .encode::<()>(move |out| out.set_ttl(key, value, b"PX", millis))
    }

    pub fn set_absent(self, key: &[u8], value: &[u8]) -> C::Call<bool> {
        self.0
            .encode::<bool>(move |out| out.command(&[b"SET", key, value, b"NX"]))
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
            .encode::<u64>(move |out| out.sequence(&[b"DEL"], count.get(), keys.map(|key| [key])))
    }

    pub fn exists(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let keys = keys.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"EXISTS"], keys))
    }

    pub fn incr(self, key: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| out.command(&[b"INCR", key]))
    }

    pub fn decr(self, key: &[u8]) -> C::Call<i64> {
        self.0
            .encode::<i64>(move |out| out.command(&[b"DECR", key]))
    }

    pub fn incr_by(self, key: &[u8], by: i64) -> C::Call<i64> {
        self.0.encode::<i64>(move |out| {
            let mut buffer = core::fmt::NumBuffer::new();
            let by = by.format_into(&mut buffer).as_bytes();
            out.command(&[b"INCRBY", key, by])
        })
    }

    pub fn incr_float(self, key: &[u8], by: f64) -> C::Call<f64> {
        self.0.encode::<f64>(move |out| {
            let mut buffer = ryu::Buffer::new();
            let by = encode::format_float(&mut buffer, by);
            out.command(&[b"INCRBYFLOAT", key, by])
        })
    }

    pub fn expire(self, key: &[u8], ttl: num::NonZero<u64>) -> C::Call<bool> {
        let seconds = ttl.get();
        self.0.encode::<bool>(move |out| {
            let mut buffer = core::fmt::NumBuffer::new();
            let seconds = seconds.format_into(&mut buffer).as_bytes();
            out.command(&[b"EXPIRE", key, seconds])
        })
    }

    pub fn ttl(self, key: &[u8]) -> C::Call<i64> {
        self.0.encode::<i64>(move |out| out.command(&[b"TTL", key]))
    }

    pub fn mget(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<decode::Optional> {
        let keys = keys.as_slice();
        self.0
            .encode::<decode::Optional>(move |out| out.slices(&[b"MGET"], keys))
    }

    pub fn mset(self, kv: nonempty::Slice<'_, (&[u8], &[u8])>) -> C::Call<()> {
        let kv = kv.as_slice();
        self.0
            .encode::<()>(move |out| out.sequence(&[b"MSET"], kv.len(), kv.iter().copied()))
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
            .encode::<u64>(move |out| out.command(&[b"BITCOUNT", key]))
    }

    /// Counts set bits in the inclusive byte range.
    pub fn count_range(self, key: &[u8], start: i64, end: i64) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.range(b"BITCOUNT", &[], key, start, end))
    }

    /// Sets one bit and returns its previous value.
    pub fn set(self, key: &[u8], offset: u64, value: bool) -> C::Call<decode::Changed> {
        self.0.encode::<decode::Changed>(move |out| {
            let mut buffer = core::fmt::NumBuffer::new();
            let offset = offset.format_into(&mut buffer).as_bytes();
            let value: &[u8] = if value { b"1" } else { b"0" };
            out.command(&[b"SETBIT", key, offset, value])
        })
    }

    pub fn get(self, key: &[u8], offset: u64) -> C::Call<decode::Changed> {
        self.0.encode::<decode::Changed>(move |out| {
            let mut buffer = core::fmt::NumBuffer::new();
            let offset = offset.format_into(&mut buffer).as_bytes();
            out.command(&[b"GETBIT", key, offset])
        })
    }

    pub fn combine(
        self,
        op: Operation,
        dest: &[u8],
        sources: nonempty::Slice<'_, &[u8]>,
    ) -> C::Call<u64> {
        let sources = sources.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"BITOP", op.as_bytes(), dest], sources))
    }

    pub fn not(self, dest: &[u8], source: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"BITOP", b"NOT", dest, source]))
    }

    pub fn position(self, key: &[u8], value: bool) -> C::Call<i64> {
        self.0.encode::<i64>(move |out| {
            out.command(&[b"BITPOS", key, if value { b"1" } else { b"0" }])
        })
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
