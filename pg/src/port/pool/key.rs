use zeroize::Zeroizing;

use crate::protocol;

pub(crate) struct Key {
    pid: i32,
    secret: Zeroizing<[u8; 4]>,
}

impl Key {
    pub(crate) fn empty() -> Self {
        Self {
            pid: 0,
            secret: Zeroizing::new([0; 4]),
        }
    }

    pub(crate) fn decode(payload: protocol::Wire<'_>) -> Result<Self, protocol::Reason> {
        let &[p0, p1, p2, p3, s0, s1, s2, s3] = payload.as_slice() else {
            return Err(protocol::Reason::BackendKeyWidth {
                actual: payload.length(),
            });
        };
        Ok(Self {
            pid: i32::from_be_bytes([p0, p1, p2, p3]),
            secret: Zeroizing::new([s0, s1, s2, s3]),
        })
    }

    pub(crate) const fn pid(&self) -> i32 {
        self.pid
    }

    pub(crate) fn secret(&self) -> &[u8; 4] {
        &self.secret
    }
}

const _: () = assert!(std::mem::size_of::<Key>() == 2 * std::mem::size_of::<i32>());
