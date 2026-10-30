use crate::protocol::frame;
use o3::buffer;
use o3::buffer::resident::staging;

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
        const WARM: usize = 16 * 1024;
        let max = self.max_frame_bytes as usize;
        staging::Plan::new(max.min(WARM), max)
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
        if bytes.len() < 5 {
            return Ok(match buffer.need_to(5) {
                Some(need) => codec::Parse::More(need),
                None => codec::Parse::CapacityExhausted,
            });
        }
        let typ = bytes[0];
        let wire = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if wire < 4 {
            let used =
                buffer.try_consumed(num::NonZero::<usize>::MIN.saturating_add(bytes.len() - 1))?;
            return Ok(codec::Parse::Item {
                head: frame::Head::Fatal(crate::Error::Protocol(protocol::Reason::FrameLength {
                    declared: wire,
                    minimum: 4,
                })),
                consumed: used,
            });
        }
        let len = wire as usize;
        if wire >= self.max_frame_bytes {
            let used = buffer.try_consumed(num::NonZero::<usize>::MIN.saturating_add(4))?;
            return Ok(codec::Parse::Item {
                head: frame::Head::Fatal(crate::Error::Protocol(protocol::Reason::FrameCapacity {
                    declared: wire,
                    capacity: protocol::Length::from_u32(self.max_frame_bytes),
                })),
                consumed: used,
            });
        }
        let total = len + 1;
        if bytes.len() < total {
            return Ok(match buffer.need_to(total) {
                Some(need) => codec::Parse::More(need),
                None => codec::Parse::CapacityExhausted,
            });
        }
        let data = frame::Payload::plan(&buffer, 5..wire + 1).map_err(scope::Error::Range)?;
        let used = buffer.try_consumed(num::NonZero::<usize>::MIN.saturating_add(total - 1))?;
        Ok(codec::Parse::Item {
            head: frame::Head::Message(frame::Message { typ, payload: data }),
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
            let reason = if bytes.len() < 5 {
                protocol::Reason::FrameTruncated {
                    actual,
                    required: protocol::Length::from_u32(5),
                }
            } else {
                let wire = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                if wire < 4 {
                    protocol::Reason::FrameLength {
                        declared: wire,
                        minimum: 4,
                    }
                } else if wire >= self.max_frame_bytes {
                    protocol::Reason::FrameCapacity {
                        declared: wire,
                        capacity: protocol::Length::from_u32(self.max_frame_bytes),
                    }
                } else {
                    let need = wire + 1;
                    protocol::Reason::FrameTruncated {
                        actual,
                        required: protocol::Length::from_u32(need),
                    }
                }
            };
            Ok(codec::Finish::Item(frame::Head::Fatal(
                crate::Error::Protocol(reason),
            )))
        }
    }
}
