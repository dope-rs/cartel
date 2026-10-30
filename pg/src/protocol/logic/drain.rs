use cartel_core::reply::order::Front;
use dope::{
    core::{driver, driver::route::KeyTag},
    manifold::client::{
        conn,
        session::{response::work, turns},
    },
};
use protocol::runtime::tx::poll;

use crate::{
    api::results::group::Position,
    port::replies::End,
    port::{self, pool::slot, replies, requests},
    protocol::{
        self,
        logic::{self, failure, state},
    },
    reply::group::Resolution,
    schema,
};

pub(super) struct Drain<
    'turn,
    'life,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'life,
    I: schema::QuerySet,
    const ID: u8,
> {
    port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    conn: conn::Id<'d, KeyTag<ID>>,
    state: &'turn mut logic::State<'life, 'd, I, ID>,
    driver: driver::Context<'turn, 'd>,
    pending: state::Drain,
}

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
}

impl<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Drain<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) fn new<'ctx>(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        ctx: &'turn mut turns::Context<
            'ctx,
            '_,
            'd,
            logic::State<'life, 'd, I, ID>,
            requests::Frame<'d>,
            KeyTag<ID>,
        >,
        pending: state::Drain,
    ) -> Self
    where
        'ctx: 'turn,
    {
        let parts = ctx.parts();
        Self {
            port,
            conn: parts.conn_id,
            state: parts.state,
            driver: parts.driver,
            pending,
        }
    }

    pub(super) fn settle<A: work::Admission>(mut self, work: &mut A) -> Progress {
        let pending = self.pending;
        let Some(lane) = self.port.responses(self.conn) else {
            failure::Failure::new(self.port, self.conn, self.state, self.driver.region_token())
                .apply(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
            return Progress::Complete;
        };
        loop {
            let action = lane
                .inspect(pending.batch, self.driver.region_token(), |end, front| {
                    if pending.completed {
                        if !matches!(pending.skip, state::Skip::None)
                            || !matches!(end, Some(End::Ready | End::Commit | End::Sync(_)))
                        {
                            return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                        }
                        return Ok(work.admit(|| Action::Complete));
                    }
                    let Some(end) = end else {
                        return Ok(work::Capacity::Available(Action::Done));
                    };
                    if let End::Sync(group) = end {
                        if matches!(pending.skip, state::Skip::None) {
                            return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                        }
                        return Ok(work.admit(|| {
                            if matches!(pending.skip, state::Skip::Unknown) {
                                group.resolve(Position::Sync);
                            }
                            Action::Complete
                        }));
                    }
                    let command = match pending.skip {
                        state::Skip::Pending => None,
                        state::Skip::Unknown => match end {
                            End::Group(index, group) | End::Last(index, group) => {
                                Some((group, Position::Command(index as usize)))
                            }
                            End::Command | End::Ready | End::Commit | End::Sync(_) => {
                                return Err(crate::Error::Protocol(protocol::Reason::GroupReply));
                            }
                        },
                        state::Skip::None => {
                            return Err(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                        }
                    };
                    Ok(work.admit(|| {
                        let server = command.is_some_and(|(group, position)| {
                            matches!(group.resolve(position), Resolution::Resolved)
                        });
                        if matches!(front, Front::Detached) {
                            Action::Retire
                        } else if server {
                            Action::FailGroup
                        } else {
                            Action::FailOne
                        }
                    }))
                })
                .and_then(|action| action);
            match action {
                Ok(work::Capacity::Available(Action::Done)) => break,
                Ok(work::Capacity::Available(Action::Complete)) => {
                    lane.complete(self.driver.region_token());
                    break;
                }
                Ok(work::Capacity::Available(Action::Retire)) => {
                    lane.complete(self.driver.region_token());
                }
                Ok(work::Capacity::Available(Action::FailGroup)) => {
                    lane.fail_group(self.driver.region_token());
                }
                Ok(work::Capacity::Available(Action::FailOne)) => lane.fail_one(
                    self.driver.region_token(),
                    replies::Failure::from_error(crate::Error::Skipped),
                ),
                Ok(work::Capacity::Exhausted(())) => return Progress::Blocked,
                Err(error) => {
                    failure::Failure::new(
                        self.port,
                        self.conn,
                        self.state,
                        self.driver.region_token(),
                    )
                    .apply(error);
                    return Progress::Complete;
                }
            }
        }
        self.complete();
        Progress::Complete
    }

    /// Retirement advances parsed batches without reindexing reset pending counts.
    fn complete(&mut self) {
        let Some(depth) = self.port.depth(self.conn, self.driver.region_token()) else {
            failure::Failure::new(self.port, self.conn, self.state, self.driver.region_token())
                .apply(crate::Error::Protocol(protocol::Reason::TransactionLane));
            return;
        };
        let settled = self.port.settle(self.conn);
        match settled {
            slot::Settle::Applied | slot::Settle::Closing => {}
            slot::Settle::Empty | slot::Settle::Stale => {
                failure::Failure::new(self.port, self.conn, self.state, self.driver.region_token())
                    .apply(crate::Error::Protocol(protocol::Reason::ReadyUnknown));
                return;
            }
        }
        self.state.batch = self.pending.batch.next();
        if matches!(settled, slot::Settle::Closing) {
            return;
        }
        self.port.shared.reindex(self.conn, depth);
        let Some(load) = self.port.load(self.conn, self.driver.region_token()) else {
            failure::Failure::new(self.port, self.conn, self.state, self.driver.region_token())
                .apply(crate::Error::Protocol(protocol::Reason::TransactionLane));
            return;
        };
        if matches!(
            self.port
                .shared
                .transaction()
                .poll_ready(self.conn, self.pending.status, load),
            poll::Readiness::Pending
        ) {
            failure::Failure::new(self.port, self.conn, self.state, self.driver.region_token())
                .apply(crate::Error::Protocol(protocol::Reason::TransactionOpen));
        }
    }
}
