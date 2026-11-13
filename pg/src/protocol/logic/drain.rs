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
    schema,
};

pub(super) enum Progress {
    Complete,
    Blocked,
}

enum Action {
    Done,
    Complete,
    Retire,
    FailGroup,
    FailOne,
    Missing,
}

impl<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Turn<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) fn try_settle<A: work::Admission>(
        self,
        status: poll::Status,
        work: &mut A,
    ) -> Result<Progress, crate::Error> {
        let entry = self
            .state
            .entry()
            .ok_or(crate::Error::Protocol(protocol::Reason::ReadyUnknown))?;
        let mut lane = replies::Lane::new(self.port, self.conn, entry, self.region);
        let depth = loop {
            let (action, selected) = lane.inspect(self.state.batch, |end, front| {
                if matches!(self.state.progress, state::Progress::Complete) {
                    if !matches!(
                        end,
                        Some(
                            Kind::Ready
                                | Kind::Required
                                | Kind::Begin
                                | Kind::Commit
                                | Kind::Sync(_)
                        )
                    ) {
                        return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                    }
                    return Ok(work.admit(|| Action::Complete));
                }
                if matches!(self.state.progress, state::Progress::Missing) {
                    if !matches!(end, Some(Kind::Required)) {
                        return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                    }
                    return match front {
                        Front::Open | Front::Denied => Ok(work.admit(|| Action::Missing)),
                        Front::Detached => Ok(work.admit(|| Action::Complete)),
                        Front::Empty => Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown)),
                    };
                }
                let Some(end) = end else {
                    return Ok(work::Capacity::Available(Action::Done));
                };
                if let Kind::Sync(group) = end {
                    if matches!(self.state.progress, state::Progress::Running) {
                        return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                    }
                    return Ok(work.admit(|| {
                        if matches!(self.state.progress, state::Progress::Unknown) {
                            group.resolve(Position::Sync);
                        }
                        Action::Complete
                    }));
                }
                let command = match self.state.progress {
                    state::Progress::Skipping => None,
                    state::Progress::Unknown => match end {
                        Kind::Group(index, group) | Kind::Last(index, group) => {
                            Some((group, Position::Command(index as usize)))
                        }
                        Kind::Command
                        | Kind::Ready
                        | Kind::Required
                        | Kind::Begin
                        | Kind::Commit
                        | Kind::Sync(_) => {
                            return Err(crate::Error::Protocol(protocol::Reason::GroupReply));
                        }
                    },
                    state::Progress::Running
                    | state::Progress::Complete
                    | state::Progress::Missing => {
                        return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                    }
                };
                Ok(work.admit(|| {
                    let server = command.is_some_and(|(group, position)| group.resolve(position));
                    if matches!(front, Front::Detached) {
                        Action::Retire
                    } else if server {
                        Action::FailGroup
                    } else {
                        Action::FailOne
                    }
                }))
            })?;
            match action? {
                work::Capacity::Available(Action::Done) => break lane.depth(),
                work::Capacity::Available(Action::Complete) => break selected.complete(),
                work::Capacity::Available(Action::Missing) => {
                    break selected
                        .fail_one(replies::Failure::from_error(crate::Error::NotFound))
                        .unwrap_or_else(|| unreachable!("reply"));
                }
                work::Capacity::Available(Action::Retire) => {
                    let depth = selected.complete();
                    lane.reindex(depth);
                }
                work::Capacity::Available(Action::FailGroup) => {
                    lane.fail_group();
                }
                work::Capacity::Available(Action::FailOne) => {
                    lane.fail_one(replies::Failure::from_error(crate::Error::Skipped))
                }
                work::Capacity::Exhausted(()) => return Ok(Progress::Blocked),
            }
        };
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
