use cartel_core::reply::order::Front;
use dope::manifold::client::session::response::work;

use crate::{
    api::results::group::Position,
    port::{replies, replies::Kind},
    protocol::{
        self,
        logic::{state, turn::Turn},
        runtime::tx::poll,
    },
};

pub(super) enum Progress {
    Complete,
    Blocked,
}

enum Action {
    Complete,
    Missing,
    Skip { server: bool },
}

impl<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: crate::port::Instance, const ID: u8>
    Turn<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) fn try_settle<A: work::Admission>(
        self,
        status: poll::Status,
        work: &mut A,
    ) -> Result<Progress, crate::Error> {
        use state::Progress::{Complete, Missing, Running, Skipping, Unknown};

        let entry = self
            .state
            .entry()
            .ok_or(crate::Error::Protocol(protocol::Reason::ReadyUnknown))?;
        let mut lane = replies::Lane::new(self.port, self.conn, entry, self.region);
        let depth = loop {
            let selected = lane.ordered();
            let action = match self.state.progress {
                // Command completion leaves this order at the front, or its paired
                // Sync after the last group command. Detachment preserves the order.
                Complete => work.admit(|| Action::Complete),
                Missing => work.admit(|| Action::Missing),
                progress @ (Running | Skipping | Unknown) => {
                    let Some(tag) = replies::Tag::current(selected.tag(), self.state.batch)? else {
                        break None;
                    };
                    match (progress, tag.kind()) {
                        (Running, _) => {
                            return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                        }
                        (_, Kind::Sync(group)) => work.admit(|| {
                            if matches!(progress, Unknown) {
                                group.resolve(Position::Sync);
                            }
                            Action::Complete
                        }),
                        (Unknown, _) => {
                            let (group, position) = tag
                                .diagnostic()
                                .ok_or(crate::Error::Protocol(protocol::Reason::GroupReply))?;
                            work.admit(|| Action::Skip {
                                server: group.resolve(position),
                            })
                        }
                        _ => work.admit(|| Action::Skip { server: false }),
                    }
                }
            };
            let work::Capacity::Available(action) = action else {
                return Ok(Progress::Blocked);
            };
            let selected = selected.into_push();
            match action {
                Action::Complete => break Some(selected.complete()),
                Action::Missing => {
                    break if matches!(selected.front(), Front::Detached) {
                        Some(selected.complete())
                    } else {
                        selected.fail_one(replies::Failure::from_error(crate::Error::NotFound))
                    };
                }
                Action::Skip { server } => {
                    if matches!(selected.front(), Front::Detached) {
                        let depth = selected.complete();
                        lane.reindex(depth);
                    } else {
                        let depth = if server {
                            selected.fail_one(replies::GroupServer)
                        } else {
                            selected.fail_one(replies::Failure::from_error(crate::Error::Skipped))
                        };
                        lane.after_failure(depth);
                    }
                }
            }
        };
        let depth = depth.unwrap_or_else(|| lane.depth());
        let settled = lane
            .settle(status, depth)
            .ok_or(crate::Error::Protocol(protocol::Reason::ReadyUnknown))?;
        self.state.progress = state::Progress::Running;
        self.state.batch = self.state.batch.next();
        if matches!(settled, poll::Readiness::Pending) {
            return Err(crate::Error::Protocol(protocol::Reason::TransactionOpen));
        }
        Ok(Progress::Complete)
    }
}
