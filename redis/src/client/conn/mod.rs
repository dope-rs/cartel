use crate::client::ReplyError;
use crate::port::schedule;
use crate::{api, client};
use crate::{request, request::encode, value};
use client::dispatch::command;
use dope_fiber::abi;

pub(super) mod acquire;

/// An exclusive lease of one exact Redis connection generation.
///
/// Deliberately neither `Copy` nor `Clone`; calls borrow it mutably until completion,
/// so connection-local observations cannot hop to another pool lane or overlap.
///
pub struct Connection<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8 = 0> {
    pub(super) lease: schedule::Lease<'app, 's, 'b, 'c, 'd, B, ID>,
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Connection<'app, 's, 'b, 'c, 'd, B, ID> {
    fn dispatch<'lease, R>(
        &'lease mut self,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error> + 'lease,
    ) -> impl abi::Fiber<'d, Output = Result<R, ReplyError<'b, 'c, 'd, ID>>> + 'lease
    where
        R: value::Decode<'s, 'b, 'c, 'd, ID, Output = R> + 'lease,
    {
        let frame = self.lease.redis().port.encode(encode);
        command::Command::<_, R, ID>::new(command::Leased::new(&mut self.lease), frame)
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> api::Connection<'d, ID>
    for Connection<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Error = ReplyError<'b, 'c, 'd, ID>;
    type Bytes = value::Bytes<'b, 'c, 'd>;

    fn ping(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Result<(), ReplyError<'b, 'c, 'd, ID>>> + '_ {
        self.dispatch::<()>(|out| out.command(&[b"PING"]))
    }

    fn client_id(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Result<i64, ReplyError<'b, 'c, 'd, ID>>> + '_ {
        self.dispatch::<i64>(|out| out.command(&[b"CLIENT".as_slice(), b"ID"]))
    }

    fn client_name(
        &mut self,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Option<value::Bytes<'b, 'c, 'd>>, ReplyError<'b, 'c, 'd, ID>>,
    > + '_ {
        self.dispatch::<Option<value::Bytes<'b, 'c, 'd>>>(|out| {
            out.command(&[b"CLIENT".as_slice(), b"GETNAME"])
        })
    }
}
