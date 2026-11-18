use crate::protocol::frame;
use cartel_core::memory;
use o3::buffer;

use crate::port::config;
use crate::protocol;
use codec::{
    eof,
    staging::{Buffered, Policy},
};
use dope::manifold::client::session::codec;
use std::num;

use o3::buffer::write::consume::scope;

pub struct Codec {
    max_frame_bytes: u32,
}

impl Codec {
    pub fn new(config: &config::Config) -> Self {
        Self {
            max_frame_bytes: config.frame_limit(),
        }
    }

    #[inline]
    fn required(&self, bytes: &[u8]) -> Result<u32, (protocol::Reason, usize)> {
        let Some(header) = bytes.first_chunk::<5>() else {
            return Ok(5);
        };
        let declared = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        if declared < 4 {
            return Err((
                protocol::Reason::FrameLength {
                    declared,
                    minimum: 4,
                },
                bytes.len(),
            ));
        }
        if declared >= self.max_frame_bytes {
            return Err((
                protocol::Reason::FrameCapacity {
                    declared,
                    capacity: protocol::Length::from_u32(self.max_frame_bytes),
                },
                5,
            ));
        }
        Ok(declared + 1)
    }
}

impl codec::Codec for Codec {
    type Head<'input, 'd, R: buffer::Cursor>
        = frame::Head<'input, 'd, R>
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Buffered;

    fn staging(&self) -> <Self::Staging as Policy>::Spec {
        memory::try_staging(self.max_frame_bytes as usize)
    }
}

impl codec::Stateless for Codec {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        buffer: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        let bytes = buffer.as_slice();
        let total = match self.required(bytes) {
            Ok(total) => total as usize,
            Err((reason, discard)) => {
                let consumed =
                    buffer.try_consumed(num::NonZero::<usize>::MIN.saturating_add(discard - 1))?;
                return Ok(codec::Parse::Item {
                    head: frame::Head::Fatal(crate::Error::Protocol(reason)),
                    consumed,
                });
            }
        };
        if bytes.len() < total {
            return Ok(match buffer.need_to(total) {
                Some(need) => codec::Parse::More(need),
                None => codec::Parse::CapacityExhausted,
            });
        }
        let (data, used) =
            buffer.try_prefix(5, num::NonZero::<usize>::MIN.saturating_add(total - 1))?;
        Ok(codec::Parse::Item {
            head: frame::Head::Message(frame::Message {
                typ: bytes[0],
                payload: frame::Payload(data),
            }),
            consumed: used,
        })
    }
}

impl eof::Finish for Codec {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Finish<Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        let bytes = input.as_slice();
        if bytes.is_empty() {
            Ok(codec::Finish::Empty)
        } else {
            let actual = match protocol::Length::try_new(bytes.len()) {
                Ok(actual) => actual,
                Err(error) => return Ok(codec::Finish::Item(frame::Head::Fatal(error))),
            };
            let reason = match self.required(bytes) {
                Ok(required) => protocol::Reason::FrameTruncated {
                    actual,
                    required: protocol::Length::from_u32(required),
                },
                Err((reason, _)) => reason,
            };
            Ok(codec::Finish::Item(frame::Head::Fatal(
                crate::Error::Protocol(reason),
            )))
        }
    }
}
