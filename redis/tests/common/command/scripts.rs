use cartel_redis::{
    api::{Storage, decode::Decode},
    client::{
        ops::scripts,
        settings::{self, Builder, Bulk},
    },
    request,
};
use o3::base::time::Positive;

pub struct Retain;

impl<S: Storage> Decode<S> for Retain {
    type Output = S::Reply;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        Ok(reply)
    }
}

pub struct Owned {
    pub(crate) keys: [Vec<u8>; 0],
    pub(crate) args: [Vec<u8>; 1],
}

pub struct Segmented<'a, const N: usize> {
    pub(crate) len: usize,
    pub(crate) chunks: [&'a [u8]; N],
}

impl<const N: usize> request::Bulk for Segmented<'_, N> {
    fn len(&self) -> usize {
        self.len
    }

    fn visit(&self, mut visit: impl FnMut(&[u8])) {
        for chunk in self.chunks {
            visit(chunk);
        }
    }
}

pub struct Split<'a> {
    pub(crate) args: [Segmented<'a, 4>; 1],
}

pub const SPLIT: Split<'static> = Split {
    args: [Segmented {
        len: 12,
        chunks: [b"first", b"", b"\0", b"second"],
    }],
};

impl<'a> scripts::Invocation for Split<'a> {
    type Key = &'a [u8];
    type Argument = Segmented<'a, 4>;

    fn script(&self) -> &[u8] {
        b"return 17"
    }

    fn keys(&self) -> &[Self::Key] {
        &[]
    }

    fn arguments(&self) -> &[Self::Argument] {
        &self.args
    }
}

impl scripts::Invocation for Owned {
    type Key = Vec<u8>;
    type Argument = Vec<u8>;

    fn script(&self) -> &[u8] {
        b"return 17"
    }

    fn keys(&self) -> &[Self::Key] {
        &self.keys
    }

    fn arguments(&self) -> &[Self::Argument] {
        &self.args
    }
}

pub fn try_config() -> Result<settings::Config, settings::Error> {
    Builder::new(settings::Capacities {
        connections: 1,
        admission_waiters: 1,
        inflight: 1,
        request_frame_bytes: 512,
        response_bytes: 64,
        response_values: 1,
        response_frame_bytes: 64,
        response_frame_values: 1,
    })
    .window(Positive::from_millis::<25>())
    .build()
}

pub fn try_bulk_config(bytes: usize) -> Result<settings::Config, settings::Error> {
    Builder::new(settings::Capacities {
        connections: 1,
        admission_waiters: 1,
        inflight: 1,
        request_frame_bytes: 256 * 1024,
        response_bytes: 64,
        response_values: 1,
        response_frame_bytes: 64,
        response_frame_values: 1,
    })
    .bulk(Bulk::new(1, bytes))
    .window(Positive::from_secs::<1>())
    .build()
}
