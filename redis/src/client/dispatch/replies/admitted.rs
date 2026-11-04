use crate::client::ReplyError;
use crate::protocol::parser::frame;
use crate::{client, value};
use cartel_core::reply::recv::outcome::Terminal;
use cartel_core::reply::{self, recv, retain};

pub(in crate::client) type Admitted<'a, 'b, 'c, 's, 'd, const ID: u8> =
    recv::Response<'a, 'b, 'c, 'd, frame::Stored<'s, 'd>, Extract<ID>, client::Failure<'d>>;

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
