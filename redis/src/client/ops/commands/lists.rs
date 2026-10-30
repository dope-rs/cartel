use crate::api::{Client, decode};
use crate::client::nonempty;
use crate::request::encode::write::value::list;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Lists<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Lists<C, ID> {
    pub fn push_front(self, key: &[u8], values: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let values = values.as_slice();
        self.0
            .encode::<u64>(move |out| list::Writer::new(out).push_front(key, values))
    }

    pub fn push_back(self, key: &[u8], values: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let values = values.as_slice();
        self.0
            .encode::<u64>(move |out| list::Writer::new(out).push_back(key, values))
    }

    pub fn pop_front(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| list::Writer::new(out).pop_front(key))
    }

    pub fn pop_back(self, key: &[u8]) -> C::Call<decode::MaybeBytes> {
        self.0
            .encode::<decode::MaybeBytes>(move |out| list::Writer::new(out).pop_back(key))
    }

    pub fn range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Bulks> {
        self.0
            .encode::<decode::Bulks>(move |out| list::Writer::new(out).range(key, start, stop))
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| list::Writer::new(out).len(key))
    }
}
