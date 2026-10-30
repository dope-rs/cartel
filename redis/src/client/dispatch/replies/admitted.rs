use crate::client::ReplyError;
use crate::protocol::parser::frame;
use crate::{client, port, value};
use cartel_core::reply::recv::outcome::Terminal;
use cartel_core::reply::{self, recv, retain};

pub(in crate::client) struct Admitted<'a, 'b, 'c, 's, 'd, const ID: u8> {
    pub(in crate::client) inner:
        recv::Response<'a, 'b, 'c, 'd, frame::Stored<'s, 'd>, Extract<ID>, client::Failure<'d>>,
    active: &'a port::Activity<'b, 'd, ID>,
}

impl<'a, 'b, 'c, 's, 'd, const ID: u8> Admitted<'a, 'b, 'c, 's, 'd, ID> {
    pub(in crate::client) fn new(active: &'a port::Activity<'b, 'd, ID>) -> Self {
        Self {
            inner: recv::Response::new(),
            active,
        }
    }
}

impl<'a, 'b, 'c, 's, 'd, const ID: u8> Drop for Admitted<'a, 'b, 'c, 's, 'd, ID> {
    fn drop(&mut self) {
        self.active.wake_all();
    }
}

pub(in crate::client) struct Extract<const ID: u8>;

impl<'a, 'b, 'c: 'b, 's, 'd: 'c, const ID: u8>
    recv::Extract<'a, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>> for Extract<ID>
{
    type Output = Result<value::Reply<'s, 'b, 'c, 'd>, ReplyError<'b, 'c, 'd, ID>>;

    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 'a, 'b, 'c, 'd, frame::Stored<'s, 'd>, client::Failure<'d>>,
    ) -> Option<Self::Output> {
        match slot.terminal()? {
            Terminal::Failed(failure) => Some(Err(failure.into_error().into_error().into())),
            Terminal::Complete(done) => {
                let retained = match done.try_retain() {
                    Ok(retained) => retained,
                    Err(error) => return Some(Err(crate::Error::ResponseRetain(error).into())),
                };
                Some(match retained.map(retain::Retained::into_parts) {
                    Some((frame, keep)) => Ok(value::Reply::new(frame, keep)),
                    None => Err(crate::Error::ReplySlotEmpty.into()),
                })
            }
        }
    }
}
