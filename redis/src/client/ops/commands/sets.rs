use crate::api::{Commands, decode};
use crate::client::nonempty;
use std::num;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Sets<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Commands<'d, ID>, const ID: u8> Sets<C, ID> {
    pub fn add(self, key: &[u8], members: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let members = members.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"SADD", key], members))
    }

    /// Encodes members directly; the call retains no iterator or input.
    /// Both traversals must match the declared count before submission.
    pub fn add_iter<I, M>(self, key: &[u8], count: num::NonZero<usize>, members: I) -> C::Call<u64>
    where
        I: Iterator<Item = M> + Clone,
        M: AsRef<[u8]>,
    {
        self.0.encode::<u64>(move |out| {
            out.sequence(&[b"SADD", key], count.get(), members.map(|member| [member]))
        })
    }

    pub fn remove(self, key: &[u8], members: nonempty::Slice<'_, &[u8]>) -> C::Call<u64> {
        let members = members.as_slice();
        self.0
            .encode::<u64>(move |out| out.slices(&[b"SREM", key], members))
    }

    pub fn members(self, key: &[u8]) -> C::Call<decode::Bulks> {
        self.0
            .encode::<decode::Bulks>(move |out| out.command(&[b"SMEMBERS", key]))
    }

    pub fn contains(self, key: &[u8], member: &[u8]) -> C::Call<bool> {
        self.0
            .encode::<bool>(move |out| out.command(&[b"SISMEMBER", key, member]))
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"SCARD", key]))
    }
}
