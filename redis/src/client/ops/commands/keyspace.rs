use crate::api::{Client, decode};
use crate::client::nonempty;
use crate::request::encode::write::space;
use std::num::NonZero;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Keyspace<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Keyspace<C, ID> {
    pub fn swap(self, key: &[u8], value: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| space::Writer::new(out).replace(key, value))
    }

    pub fn take(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| space::Writer::new(out).take(key))
    }

    pub fn append(self, key: &[u8], value: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| space::Writer::new(out).append(key, value))
    }

    pub fn value_len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| space::Writer::new(out).len(key))
    }

    pub fn kind(self, key: &[u8]) -> C::Call<decode::Kind> {
        self.0
            .encode::<decode::Kind>(move |out| space::Writer::new(out).kind(key))
    }

    pub fn rename(self, src: &[u8], dst: &[u8]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| space::Writer::new(out).rename(src, dst))
    }

    pub fn persist(self, key: &[u8]) -> C::Call<bool> {
        self.0
            .encode::<bool>(move |out| space::Writer::new(out).persist(key))
    }

    pub fn unlink(self, keys: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let keys = keys.as_slice();
        self.0
            .encode::<u64>(move |out| space::Writer::new(out).unlink(keys))
    }

    pub fn db_size(self) -> C::Call<u64> {
        self.0.encode::<u64>(|out| space::Writer::new(out).db_len())
    }

    pub fn scan(
        self,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: Option<NonZero<u64>>,
    ) -> C::Call<decode::Scan> {
        self.0
            .encode::<decode::Scan>(move |out| space::Writer::new(out).scan(cursor, pattern, count))
    }
}
