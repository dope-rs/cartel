use crate::api::{Client, decode};
use crate::client::nonempty;
use crate::request::encode::write::value::sorted;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Sorted<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Sorted<C, ID> {
    pub fn add<M: AsRef<[u8]>>(self, key: &[u8], score: f64, member: M) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| sorted::Writer::new(out).add(key, score, member.as_ref()))
    }

    pub fn remove(self, key: &[u8], members: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let members = members.as_slice();
        self.0
            .encode::<u64>(move |out| sorted::Writer::new(out).remove(key, members))
    }

    pub fn range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Bulks> {
        self.0
            .encode::<decode::Bulks>(move |out| sorted::Writer::new(out).range(key, start, stop))
    }

    pub fn rev_range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Scored> {
        self.0.encode::<decode::Scored>(move |out| {
            sorted::Writer::new(out).rev_range(key, start, stop)
        })
    }

    pub fn scored_range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Scored> {
        self.0.encode::<decode::Scored>(move |out| {
            sorted::Writer::new(out).scored_range(key, start, stop)
        })
    }

    pub fn score_range(self, key: &[u8], min: f64, max: f64) -> C::Call<decode::Bulks> {
        self.0
            .encode::<decode::Bulks>(move |out| sorted::Writer::new(out).score_range(key, min, max))
    }

    pub fn rank(self, key: &[u8], member: &[u8]) -> C::Call<Option<u64>> {
        self.0
            .encode::<Option<u64>>(move |out| sorted::Writer::new(out).rank(key, member))
    }

    pub fn reverse_rank<M: AsRef<[u8]>>(self, key: &[u8], member: M) -> C::Call<Option<u64>> {
        self.0.encode::<Option<u64>>(move |out| {
            sorted::Writer::new(out).rev_rank(key, member.as_ref())
        })
    }

    pub fn score(self, key: &[u8], member: &[u8]) -> C::Call<Option<f64>> {
        self.0
            .encode::<Option<f64>>(move |out| sorted::Writer::new(out).score(key, member))
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| sorted::Writer::new(out).len(key))
    }

    pub fn increment(self, key: &[u8], by: f64, member: &[u8]) -> C::Call<f64> {
        self.0
            .encode::<f64>(move |out| sorted::Writer::new(out).incr_by(key, by, member))
    }
}
