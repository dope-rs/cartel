use crate::api::{Commands, decode};
use crate::client::nonempty;
use std::num;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Hashes<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Commands<'d, ID>, const ID: u8> Hashes<C, ID> {
    pub fn get(self, key: &[u8], field: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| out.command(&[b"HGET", key, field]))
    }

    pub fn set(self, key: &[u8], field: &[u8], value: &[u8]) -> C::Call<decode::Changed> {
        self.0
            .encode::<decode::Changed>(move |out| out.command(&[b"HSET", key, field, value]))
    }

    pub fn set_many(self, key: &[u8], fv: nonempty::Slice<'_, (&[u8], &[u8])>) -> C::Call<u64> {
        let fv = fv.as_slice();
        self.0
            .encode::<u64>(move |out| out.sequence(&[b"HSET", key], fv.len(), fv.iter().copied()))
    }

    /// Encodes a repeatable field projection directly into its admitted frame.
    ///
    /// The declared count and both iterator traversals are checked before the
    /// frame can be submitted. The returned call retains no iterator or input.
    pub fn set_iter<I, K, V>(self, key: &[u8], count: num::NonZero<usize>, pairs: I) -> C::Call<u64>
    where
        I: Iterator<Item = (K, V)> + Clone,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.0
            .encode::<u64>(move |out| out.sequence(&[b"HSET", key], count.get(), pairs))
    }

    pub fn values(
        self,
        key: &[u8],
        fields: nonempty::Slice<'_, &[u8]>,
    ) -> C::Call<decode::Optional> {
        let fields = fields.as_slice();
        self.0
            .encode::<decode::Optional>(move |out| out.slices(&[b"HMGET", key], fields))
    }

    pub fn remove(self, key: &[u8], fields: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let fields = fields.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"HDEL", key], fields))
    }

    pub fn entries(self, key: &[u8]) -> C::Call<decode::Pairs> {
        self.0
            .encode::<decode::Pairs>(move |out| out.command(&[b"HGETALL", key]))
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"HLEN", key]))
    }

    pub fn contains(self, key: &[u8], field: &[u8]) -> C::Call<bool> {
        self.0
            .encode::<bool>(move |out| out.command(&[b"HEXISTS", key, field]))
    }

    pub fn increment(self, key: &[u8], field: &[u8], by: i64) -> C::Call<i64> {
        self.0.encode::<i64>(move |out| {
            let mut buffer = core::fmt::NumBuffer::new();
            let by = by.format_into(&mut buffer).as_bytes();
            out.command(&[b"HINCRBY", key, field, by])
        })
    }
}
