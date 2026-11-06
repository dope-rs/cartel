use cartel_redis::client::settings::{self, Builder};
use o3::base::time::Positive;

const MEMBERS: &[u8] = b"*2\r\n$8\r\nSMEMBERS\r\n$3\r\nset\r\n";

pub const ENTRIES: &[u8] = b"*2\r\n$7\r\nHGETALL\r\n$4\r\nhash\r\n";

#[derive(Clone, Copy)]
pub enum List {
    Array,
    ArrayProof,
    ArrayEmpty,
    Optional,
    ArrayLate,
    ArrayVisit,
    ArrayVisitAt,
    Pairs,
    PairsOdd,
    PairsLate,
    PairsVisit,
}

pub const DRAINS: [List; 3] = [List::Array, List::ArrayVisitAt, List::Pairs];

pub const REPLIES: [&[u8]; 2] = [
    List::Pairs.reply(),
    b"*4\r\n$1\r\nx\r\n$1\r\n3\r\n$1\r\ny\r\n$1\r\n4\r\n",
];

pub struct AfterDrain<F: FnMut()>(pub F);

impl<F: FnMut()> Drop for AfterDrain<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

impl List {
    pub const fn request<'a>(self) -> &'a [u8] {
        match self {
            Self::Optional => b"*4\r\n$5\r\nHMGET\r\n$4\r\nhash\r\n$1\r\na\r\n$1\r\nb\r\n",
            Self::Array
            | Self::ArrayProof
            | Self::ArrayEmpty
            | Self::ArrayLate
            | Self::ArrayVisit
            | Self::ArrayVisitAt => MEMBERS,
            Self::Pairs | Self::PairsOdd | Self::PairsLate | Self::PairsVisit => ENTRIES,
        }
    }

    pub const fn reply<'a>(self) -> &'a [u8] {
        match self {
            Self::Optional => b"*2\r\n$0\r\n\r\n$-1\r\n",
            Self::Array | Self::ArrayVisit | Self::ArrayVisitAt => {
                b"*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"
            }
            Self::ArrayProof => b"*6\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n$4\r\nfive\r\n$3\r\nsix\r\n",
            Self::ArrayEmpty => b"*0\r\n",
            Self::ArrayLate => b"*3\r\n$3\r\none\r\n$3\r\ntwo\r\n+last\r\n",
            Self::Pairs | Self::PairsVisit => b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n",
            Self::PairsOdd => b"*3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n",
            Self::PairsLate => b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n+last\r\n",
        }
    }
}

pub fn try_config(inflight: usize) -> Result<settings::Config, settings::Error> {
    Builder::new(settings::Capacities {
        connections: 1,
        admission_waiters: 1,
        inflight,
        request_frame_bytes: 128,
        response_bytes: 128,
        response_values: 8,
        response_frame_bytes: 128,
        response_frame_values: 8,
    })
    .window(Positive::from_millis::<1000>())
    .build()
}
