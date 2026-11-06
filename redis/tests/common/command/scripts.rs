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

const NESTED_PAIR: &[u8] = b"*2\r\n$1\r\n1\r\n*3\r\n$-1\r\n$-1\r\n$-1\r\n";
pub const PAIR_REPLIES: [&[u8]; 4] = [
    b"*2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n",
    b"*2\r\n$4\r\n1.25\r\n:-2\r\n",
    NESTED_PAIR,
    NESTED_PAIR,
];
pub const EMPTY_REPLIES: [&[u8]; 2] = [b"$0\r\n\r\n", b":17\r\n"];
pub const ACK: &[u8] =
    b"*5\r\n$4\r\nXACK\r\n$3\r\nkey\r\n$5\r\ngroup\r\n$3\r\n1-0\r\n$3\r\n2-0\r\n";

impl<S: Storage> Decode<S> for Retain {
    type Output = S::Reply;

    fn decode(reply: S::Reply) -> Result<Self::Output, S::Error> {
        Ok(reply)
    }
}

pub struct Owned {
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
    pub(crate) keys: &'a [&'a [u8]],
    pub(crate) args: [Segmented<'a, 4>; 1],
}

pub const SPLIT: Split<'static> = Split {
    keys: &[],
    args: [Segmented {
        len: 12,
        chunks: [b"first", b"", b"\0", b"second"],
    }],
};

pub const KEY_COUNTS: [(usize, &[u8]); 3] = [
    (0, b"*4\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n"),
    (9, b"*13\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n9\r\n"),
    (10, b"*14\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$2\r\n10\r\n"),
];
pub const KEY_BULK: &[u8] = b"$1\r\nk\r\n";
pub const SPLIT_BULK: &[u8] = b"$12\r\nfirst\0second\r\n";

const MAX_LENGTH: usize = i64::MAX as usize;
pub const PREFIX_ERRORS: [(usize, usize, request::Error); 3] = [
    (
        usize::MAX,
        1,
        request::Error::Add {
            left: usize::MAX,
            right: 1,
        },
    ),
    (
        usize::MAX,
        0,
        request::Error::Add {
            left: 3,
            right: usize::MAX,
        },
    ),
    (
        MAX_LENGTH,
        0,
        request::Error::Length {
            actual: MAX_LENGTH + 3,
            maximum: MAX_LENGTH,
        },
    ),
];
pub const KEY_LENGTH_ERRORS: [(usize, &[usize], request::Error); 4] = [
    (
        1,
        &[],
        request::Error::Items {
            declared: 1,
            actual: 0,
        },
    ),
    (
        0,
        &[usize::MAX],
        request::Error::Items {
            declared: 0,
            actual: 1,
        },
    ),
    (
        2,
        &[usize::MAX],
        request::Error::Length {
            actual: usize::MAX,
            maximum: MAX_LENGTH,
        },
    ),
    (
        2,
        &[MAX_LENGTH, MAX_LENGTH],
        request::Error::Add {
            left: MAX_LENGTH + 75,
            right: MAX_LENGTH,
        },
    ),
];

impl<'a> scripts::Invocation for Split<'a> {
    type Key = &'a [u8];
    type Argument = Segmented<'a, 4>;

    fn script(&self) -> &[u8] {
        b"return 17"
    }

    fn keys(&self) -> &[Self::Key] {
        self.keys
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
        &[]
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
