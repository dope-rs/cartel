use crate::api::{Client, decode};
use crate::client::nonempty;
use crate::request::encode;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Lists<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Lists<C, ID> {
    pub fn push_front(self, key: &[u8], values: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let values = values.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"LPUSH", key], values))
    }

    pub fn push_back(self, key: &[u8], values: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let values = values.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"RPUSH", key], values))
    }

    pub fn pop_front(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| out.command(&[b"LPOP", key]))
    }

    pub fn pop_back(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| out.command(&[b"RPOP", key]))
    }

    pub fn range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Bulks> {
        self.0.encode::<decode::Bulks>(move |out| {
            let mut head = [0; 21];
            let start = encode::format_int_into(&mut head, start);
            let mut tail = [0; 21];
            let stop = encode::format_int_into(&mut tail, stop);
            out.command(&[b"LRANGE", key, start, stop])
        })
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"LLEN", key]))
    }
}
