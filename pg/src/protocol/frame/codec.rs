use crate::protocol::{self, frame, messages::Be};
use cartel_core::memory;
use o3::buffer;

use crate::port::config;
use codec::staging::{Buffered, Policy};
use dope::manifold::client::session::codec;
use std::num;

use o3::buffer::write::consume::scope;

pub struct Codec {
    max_frame_bytes: u32,
}

impl Codec {
    pub fn new<P: crate::port::ready::Policy>(config: &config::Configuration<P>) -> Self {
        Self {
            max_frame_bytes: config.frame_limit(),
        }
    }

    #[inline]
    fn required(max_frame_bytes: u32, bytes: &[u8]) -> Result<u32, (protocol::Reason, usize)> {
        let Some(header) = bytes.first_chunk::<5>() else {
            return Ok(5);
        };
        let declared = frame::declared(header);
        if declared < 4 {
            return Err((
                protocol::Reason::FrameLength {
                    declared,
                    minimum: 4,
                },
                bytes.len(),
            ));
        }
        if declared >= max_frame_bytes {
            return Err((
                protocol::Reason::FrameCapacity {
                    declared,
                    capacity: protocol::Length::from_u32(max_frame_bytes),
                },
                5,
            ));
        }
        Ok(declared + 1)
    }

    pub fn parse<'id, 'input, 'd, R: buffer::Cursor, C, T>(
        &self,
        input: codec::Input<'id, 'input, 'd, R>,
        context: C,
        reject: impl FnOnce(C, crate::Error) -> T,
        deliver: impl FnOnce(C, Complete<'id, 'input, 'd, R>) -> Item<'id, T>,
    ) -> Parsed<'id, T>
    where
        'd: 'input,
    {
        let bytes = input.as_slice();
        let len = match Self::required(self.max_frame_bytes, bytes) {
            Ok(len) => len as usize,
            Err((reason, discard)) => {
                let consumed =
                    input.try_consumed(num::NonZero::<usize>::MIN.saturating_add(discard - 1))?;
                return Ok(codec::Parse::Item {
                    head: reject(context, crate::Error::Protocol(reason)),
                    consumed,
                });
            }
        };
        if len > bytes.len() {
            return Ok(match input.need_to(len) {
                Some(need) => codec::Parse::More(need),
                None => codec::Parse::CapacityExhausted,
            });
        }
        let (head, consumed) = deliver(
            context,
            Complete {
                input,
                len,
                typ: bytes[0],
                max_frame_bytes: self.max_frame_bytes,
            },
        )?;
        Ok(codec::Parse::Item { head, consumed })
    }
}

pub type Parsed<'id, T> = Result<codec::Parse<'id, T>, scope::Error>;
pub type Item<'id, T> = Result<
    (
        T,
        scope::End<'id, { dope::core::io::transfer::MAX_BYTES as u32 }>,
    ),
    scope::Error,
>;

pub struct Complete<'id, 'input, 'd, R: buffer::Cursor> {
    input: codec::Input<'id, 'input, 'd, R>,
    len: usize,
    pub(crate) typ: u8,
    max_frame_bytes: u32,
}

impl<'id, 'input, 'd, R: buffer::Cursor> Complete<'id, 'input, 'd, R> {
    pub fn message<T>(
        self,
        deliver: impl FnOnce(frame::Message<'input, 'd, R>) -> T,
    ) -> Item<'id, T> {
        let (payload, consumed) = self.input.try_prefix(
            config::FRAME_HEADER_BYTES,
            num::NonZero::<usize>::MIN.saturating_add(self.len - 1),
        )?;
        Ok((
            deliver(frame::Message {
                typ: self.typ,
                payload: frame::Payload(payload),
            }),
            consumed,
        ))
    }

    pub fn collected<T>(
        self,
        deliver: impl FnOnce(frame::Payload<'input, 'd, R>, num::NonZero<u32>) -> T,
    ) -> Item<'id, T>
    where
        R: buffer::hold::raw::Retain<'d>,
    {
        let mut end = self.len;
        let mut rows = num::NonZero::<u32>::MIN;
        if self.typ == Be::DATA_ROW {
            let input = self.input.as_slice();
            let input = &input[..input.len().min(dope::core::io::transfer::MAX_BYTES)];
            let mut tail = &input[end..];
            while let Some([Be::DATA_ROW, ..]) = tail.first_chunk::<5>() {
                let Ok(len) = Codec::required(self.max_frame_bytes, tail) else {
                    break;
                };
                let Some(next) = tail.get(len as usize..) else {
                    break;
                };
                // SAFETY: each counted frame takes at least five of the input's MAX_BYTES bytes.
                rows = unsafe { num::NonZero::new_unchecked(rows.get() + 1) };
                tail = next;
            }
            end = input.len() - tail.len();
        }
        let (payload, consumed) = self
            .input
            .try_prefix(0, num::NonZero::<usize>::MIN.saturating_add(end - 1))?;
        Ok((deliver(frame::Payload(payload), rows), consumed))
    }
}

impl codec::Codec for Codec {
    type Head<'input, 'd, R: buffer::Cursor>
        = Result<frame::Message<'input, 'd, R>, crate::Error>
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Buffered;

    fn staging(&self) -> <Self::Staging as Policy>::Spec {
        memory::try_staging(self.max_frame_bytes as usize)
    }
}

impl codec::Stateful for Codec {
    type State<'d> = ();
    type Storage<S> = S;

    fn state<'d>(&self, _: buffer::resident::Budget<'d>) {}

    fn storage<'d, S>(&self, source: S, _: &()) -> S {
        source
    }

    fn source<S: buffer::raw::Source>(storage: &S) -> &S {
        storage
    }

    fn edit<S>(storage: &mut S) -> codec::storage::Edit<'_, S> {
        codec::storage::Edit::new(storage)
    }

    fn restart<'a, 'd, S>(&self, storage: &'a mut S, _: &()) -> codec::storage::Edit<'a, S> {
        Self::edit(storage)
    }
}

impl Codec {
    pub(crate) fn finish_with<T>(
        &self,
        bytes: &[u8],
        fail: impl FnOnce(crate::Error) -> T,
    ) -> codec::Finish<T> {
        if bytes.is_empty() {
            codec::Finish::Empty
        } else {
            let actual = match protocol::Length::try_new(bytes.len()) {
                Ok(actual) => actual,
                Err(error) => return codec::Finish::Item(fail(error)),
            };
            let reason = match Self::required(self.max_frame_bytes, bytes) {
                Ok(required) => protocol::Reason::FrameTruncated {
                    actual,
                    required: protocol::Length::from_u32(required),
                },
                Err((reason, _)) => reason,
            };
            codec::Finish::Item(fail(crate::Error::Protocol(reason)))
        }
    }
}
