use crate::api::{Client, decode};
use crate::client::nonempty;
use std::num::NonZero;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Streams<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Client<'d, ID>, const ID: u8> Streams<C, ID> {
    /// Encodes the field/value tail unchanged; Redis validates its arity.
    /// The returned call retains no input.
    pub fn add<K: AsRef<[u8]>>(
        self,
        key: &[u8],
        id: &[u8],
        fields: nonempty::Slice<'_, K>,
    ) -> C::Call<decode::Bytes> {
        let fields = fields.as_slice();
        self.0.encode::<decode::Bytes>(move |out| {
            out.sequence(
                &[b"XADD", key, id],
                fields.len(),
                fields.iter().map(|value| [value]),
            )
        })
    }

    pub fn range<D: decode::Decode<C>>(
        self,
        key: &[u8],
        start: &[u8],
        end: &[u8],
        count: Option<NonZero<u64>>,
    ) -> C::Call<D> {
        self.range_with::<D>(b"XRANGE", key, start, end, count)
    }

    pub fn rev_range<D: decode::Decode<C>>(
        self,
        key: &[u8],
        end: &[u8],
        start: &[u8],
        count: Option<NonZero<u64>>,
    ) -> C::Call<D> {
        self.range_with::<D>(b"XREVRANGE", key, end, start, count)
    }

    pub fn remove<K: AsRef<[u8]>>(self, key: &[u8], ids: nonempty::Slice<'_, K>) -> C::Call<u64> {
        let ids = ids.as_slice();
        self.0.encode::<u64>(move |out| {
            out.sequence(&[b"XDEL", key], ids.len(), ids.iter().map(|id| [id]))
        })
    }

    pub fn len(self, key: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| out.command(&[b"XLEN", key]))
    }

    pub fn create_group(self, key: &[u8], group: &[u8], id: &[u8]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| out.command(&[b"XGROUP", b"CREATE", key, group, id]))
    }

    pub fn set_group_id(self, key: &[u8], group: &[u8], id: &[u8]) -> C::Call<()> {
        self.0
            .encode::<()>(move |out| out.command(&[b"XGROUP", b"SETID", key, group, id]))
    }

    /// Reads new deliveries from one stream without blocking.
    pub fn read_group<D: decode::Decode<C>>(
        self,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
        count: NonZero<u64>,
    ) -> C::Call<D> {
        self.0.encode::<D>(move |out| {
            let mut buffer = core::fmt::NumBuffer::new();
            let count = count.get().format_into(&mut buffer).as_bytes();
            out.command(&[
                b"XREADGROUP",
                b"GROUP",
                group,
                consumer,
                b"COUNT",
                count,
                b"STREAMS",
                key,
                b">",
            ])
        })
    }

    pub fn ack<K: AsRef<[u8]>>(
        self,
        key: &[u8],
        group: &[u8],
        ids: nonempty::Slice<'_, K>,
    ) -> C::Call<u64> {
        let ids = ids.as_slice();
        self.0.encode::<u64>(move |out| {
            out.sequence(&[b"XACK", key, group], ids.len(), ids.iter().map(|id| [id]))
        })
    }

    pub fn pending<D: decode::Decode<C>>(self, key: &[u8], group: &[u8]) -> C::Call<D> {
        self.0
            .encode::<D>(move |out| out.command(&[b"XPENDING", key, group]))
    }

    fn range_with<D: decode::Decode<C>>(
        self,
        verb: &[u8],
        key: &[u8],
        first: &[u8],
        last: &[u8],
        count: Option<NonZero<u64>>,
    ) -> C::Call<D> {
        self.0.encode::<D>(move |out| match count {
            Some(count) => {
                let mut buffer = core::fmt::NumBuffer::new();
                let count = count.get().format_into(&mut buffer).as_bytes();
                out.command(&[verb, key, first, last, b"COUNT", count])
            }
            None => out.command(&[verb, key, first, last]),
        })
    }
}
