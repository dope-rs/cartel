use crate::api::{Commands, decode};
use crate::client::nonempty;
use crate::request::encode;
use std::num::NonZero;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Sorted<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Commands<'d, ID>, const ID: u8> Sorted<C, ID> {
    pub fn add<M: AsRef<[u8]>>(self, key: &[u8], score: f64, member: M) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.float::<false>(b"ZADD", key, score, member.as_ref()))
    }

    /// Updates an existing member's score without inserting a missing member.
    pub fn add_existing<M: AsRef<[u8]>>(self, key: &[u8], score: f64, member: M) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.float::<true>(b"ZADD", key, score, member.as_ref()))
    }

    pub fn remove(self, key: &[u8], members: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let members = members.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"ZREM", key], members))
    }

    pub fn range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Bulks> {
        self.0
            .encode::<decode::Bulks>(move |out| out.range(b"ZRANGE", &[], key, start, stop))
    }

    /// Returns the cursor and interleaved member/score bulk strings.
    /// `count` is a server work hint, not a response-size limit.
    pub fn scan(
        self,
        key: &[u8],
        cursor: u64,
        count: Option<NonZero<u64>>,
    ) -> C::Call<decode::Scan> {
        self.0
            .encode::<decode::Scan>(move |out| out.scan(&[b"ZSCAN", key], cursor, None, count))
    }

    pub fn rev_range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Scored> {
        self.0.encode::<decode::Scored>(move |out| {
            out.range(b"ZREVRANGE", &[b"WITHSCORES"], key, start, stop)
        })
    }

    pub fn scored_range(self, key: &[u8], start: i64, stop: i64) -> C::Call<decode::Scored> {
        self.0.encode::<decode::Scored>(move |out| {
            out.range(b"ZRANGE", &[b"WITHSCORES"], key, start, stop)
        })
    }

    pub fn score_range(self, key: &[u8], min: f64, max: f64) -> C::Call<decode::Bulks> {
        self.0.encode::<decode::Bulks>(move |out| {
            let mut lower = ryu::Buffer::new();
            let min = encode::format_float(&mut lower, min);
            let mut upper = ryu::Buffer::new();
            let max = encode::format_float(&mut upper, max);
            out.command(&[b"ZRANGEBYSCORE", key, min, max])
        })
    }

    pub fn rank(self, key: &[u8], member: &[u8]) -> C::Call<Option<u64>> {
        self.0
            .encode::<Option<u64>>(move |out| out.command(&[b"ZRANK", key, member]))
    }

    pub fn reverse_rank<M: AsRef<[u8]>>(self, key: &[u8], member: M) -> C::Call<Option<u64>> {
        self.0
            .encode::<Option<u64>>(move |out| out.command(&[b"ZREVRANK", key, member.as_ref()]))
    }

    pub fn score(self, key: &[u8], member: &[u8]) -> C::Call<Option<f64>> {
        self.0
            .encode::<Option<f64>>(move |out| out.command(&[b"ZSCORE", key, member]))
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"ZCARD", key]))
    }

    pub fn increment(self, key: &[u8], by: f64, member: &[u8]) -> C::Call<f64> {
        self.0
            .encode::<f64>(move |out| out.float::<false>(b"ZINCRBY", key, by, member))
    }
}
