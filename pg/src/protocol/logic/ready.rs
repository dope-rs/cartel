use cartel_core::reply::{input::Target, order::Front, usage};
use o3::buffer::{self, hold};

use crate::{
    port::{
        pool::access,
        replies::{Kind, Tag},
    },
    protocol::{
        Reason, frame,
        logic::{self, payloads, state, turn::Turn},
        messages,
        runtime::{ready::Entry, tx::poll},
    },
    reply,
};

impl<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: crate::port::Instance, const ID: u8>
    Turn<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    /// Command completion retains Ready replies until Sync resolves the transaction.
    pub(super) fn ready<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        typ: u8,
        data: frame::Payload<'input, 'd, R>,
        entry: Entry<'life, 'd, ID, I::Policy>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error>
    where
        'd: 'input,
    {
        if !matches!(self.state.progress, state::Progress::Running)
            && !matches!(
                typ,
                messages::Be::PARAMETER_STATUS
                    | messages::Be::NOTIFICATION_RESPONSE
                    | messages::Be::READY_FOR_QUERY
            )
        {
            return Err(crate::Error::UnexpectedReadyMessage(typ));
        }
        let copy = matches!(
            self.state.phase,
            state::Phase::CopyInActive(_) | state::Phase::CopyOutActive(_)
        );
        match typ {
            messages::Be::ROW_DESCRIPTION | messages::Be::PARAMETER_DESCRIPTION => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                Ok(None)
            }
            messages::Be::PARAMETER_STATUS => {
                payloads::parameter(data.wire())?;
                Ok(None)
            }
            messages::Be::PARSE_COMPLETE
            | messages::Be::BIND_COMPLETE
            | messages::Be::NO_DATA
            | messages::Be::PORTAL_SUSPENDED => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::empty(typ, data.wire())?;
                Ok(None)
            }
            messages::Be::COPY_DONE => {
                if !matches!(self.state.phase, state::Phase::CopyOutActive(_)) {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::empty(typ, data.wire())?;
                self.state.phase = state::Phase::Ready(entry);
                Ok(None)
            }
            messages::Be::COPY_IN_RESPONSE => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::copy_format(data.wire())?;
                self.state.phase = state::Phase::CopyInActive(entry);
                Ok(None)
            }
            messages::Be::COPY_OUT_RESPONSE => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::copy_format(data.wire())?;
                self.state.phase = state::Phase::CopyOutActive(entry);
                Ok(None)
            }
            messages::Be::COMMAND_COMPLETE | messages::Be::EMPTY_QUERY_RESPONSE => {
                let mut queue =
                    crate::port::replies::Lane::new(self.port, self.conn, entry, self.region);
                let selected = queue.ordered();
                let end =
                    Tag::current(selected.tag(), self.state.batch).map(|tag| tag.map(Tag::kind));
                let found = if typ == messages::Be::COMMAND_COMPLETE {
                    if matches!(end, Ok(Some(Kind::Required))) {
                        payloads::update(data.wire())?
                    } else {
                        payloads::command(typ, data.wire())?;
                        true
                    }
                } else {
                    payloads::empty(typ, data.wire())?;
                    true
                };
                let Some(end) = end? else {
                    return Err(crate::Error::Protocol(Reason::CommandEmpty));
                };
                match end {
                    Kind::Command | Kind::Group(..) => {
                        let depth = selected.into_push().complete();
                        queue.reindex(depth);
                    }
                    Kind::Last(..) => {
                        let depth = selected.into_push().complete();
                        queue.reindex(depth);
                        self.state.progress = state::Progress::Complete;
                    }
                    Kind::Ready | Kind::Begin => {
                        self.state.progress = state::Progress::Complete;
                    }
                    Kind::Required => {
                        if typ == messages::Be::EMPTY_QUERY_RESPONSE {
                            return Err(crate::Error::UnexpectedReadyMessage(typ));
                        }
                        self.state.progress = if found {
                            state::Progress::Complete
                        } else {
                            state::Progress::Missing
                        };
                    }
                    Kind::Commit => match data.wire().as_slice() {
                        b"COMMIT\0" => self.state.progress = state::Progress::Complete,
                        b"ROLLBACK\0" => {
                            let depth = selected.into_push().fail_one(
                                crate::port::replies::Failure::from_error(
                                    crate::Error::CommitRolledBack,
                                ),
                            );
                            queue.after_failure(depth);
                            self.state.progress = state::Progress::Skipping;
                        }
                        _ => return Err(crate::Error::Protocol(Reason::CommitTag)),
                    },
                    Kind::Sync(_) => {
                        return Err(crate::Error::UnexpectedReadyMessage(typ));
                    }
                }
                if copy {
                    self.state.phase = state::Phase::Ready(entry);
                }
                Ok(None)
            }
            messages::Be::NOTIFICATION_RESPONSE => {
                payloads::notification(data, &self.port.shared.notifications, self.region)?;
                Ok(None)
            }
            messages::Be::READY_FOR_QUERY => {
                let status = payloads::ready_status(data.wire())?;
                match (status, entry.slot().access(self.conn)) {
                    (poll::Status::Transaction, Some(access::State::Reserved(_))) => {
                        let queue = crate::port::replies::Lane::new(
                            self.port,
                            self.conn,
                            entry,
                            self.region,
                        );
                        if !matches!(
                            queue.current(self.state.batch)?.map(Tag::kind),
                            Some(Kind::Begin)
                        ) {
                            return Err(crate::Error::Protocol(Reason::TransactionLease));
                        }
                    }
                    (poll::Status::Transaction, Some(access::State::Held(_))) => {}
                    (poll::Status::Transaction, Some(access::State::Finalizing(_))) => {
                        return Err(crate::Error::Protocol(Reason::TransactionOpen));
                    }
                    (poll::Status::Transaction, _)
                    | (poll::Status::Idle, Some(access::State::Held(_))) => {
                        return Err(crate::Error::Protocol(Reason::TransactionLease));
                    }
                    (poll::Status::Idle, _) => {}
                }
                if copy {
                    self.state.phase = state::Phase::Ready(entry);
                }
                Ok(Some(logic::Pending::ready(status)))
            }
            other => Err(crate::Error::UnexpectedReadyMessage(other)),
        }
    }

    pub(super) fn push<'id, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        frame: frame::Complete<'id, 'input, 'd, R>,
        entry: Entry<'life, 'd, ID, I::Policy>,
    ) -> frame::Item<'id, Result<Option<logic::Pending<'d>>, crate::Error>>
    where
        'd: 'input,
    {
        use crate::port::replies;

        let mut queue = replies::Lane::new(self.port, self.conn, entry, self.region);
        let selected = queue.ordered();
        match Tag::current(selected.tag(), self.state.batch).map(|tag| tag.map(Tag::kind)) {
            Err(error) => return frame.message(|_| Err(error)),
            Ok(None) => {
                return frame.message(|_| Err(crate::Error::Protocol(Reason::RowEmpty)));
            }
            Ok(Some(Kind::Sync(_))) => {
                return frame.message(|_| match selected.into_push().front() {
                    Front::Detached => Ok(None),
                    Front::Open | Front::Denied => Err(crate::Error::Protocol(Reason::CommandData)),
                    Front::Empty => Err(crate::Error::Protocol(Reason::ReadyUnknown)),
                });
            }
            Ok(Some(
                Kind::Command
                | Kind::Group(..)
                | Kind::Last(..)
                | Kind::Ready
                | Kind::Required
                | Kind::Begin
                | Kind::Commit,
            )) => {}
        }
        let headroom = self.port.shared.config.retention_headroom();
        match selected.into_push().input() {
            Ok(Target::Collected(target)) => frame.collected(|data, rows| {
                Self::row_result(
                    target.try_build(replies::Run::new(data, rows, headroom)),
                    |_, cause| {
                        Err(crate::Error::ReplyCapacity(usage::Capacity::Unavailable(
                            cause,
                        )))
                    },
                )
            }),
            Ok(Target::Single(target)) => frame.message(|message| {
                Self::row_result(
                    target.try_build(replies::RowPlan::row(message.payload, headroom)),
                    |build, _| {
                        build
                            .into_pending()
                            .map(|row| Some(logic::Pending::row(row)))
                            .map_err(|(_, error)| crate::Error::ResponseRetention(error))
                    },
                )
            }),
            Ok(Target::Discard) => frame.message(|_| Ok(None)),
            Err(front) => frame.message(|_| Self::row_front(front)),
        }
    }

    fn row_front(front: Front) -> Result<Option<logic::Pending<'d>>, crate::Error> {
        match front {
            Front::Detached => Ok(None),
            Front::Denied => Err(crate::Error::Protocol(Reason::CommandData)),
            Front::Empty => Err(crate::Error::Protocol(Reason::ReadyUnknown)),
            Front::Open => Err(crate::Error::ReplyFront(front)),
        }
    }

    fn row_result<B, V>(
        result: Result<(), usage::PushError<B, hold::Error, V>>,
        pressure: impl FnOnce(
            B,
            usage::Backpressure,
        ) -> Result<Option<logic::Pending<'d>>, crate::Error>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error> {
        match result {
            Ok(()) => Ok(None),
            Err(usage::PushError::Backpressure { builder, cause }) => pressure(builder, cause),
            Err(usage::PushError::Capacity { cause, .. }) => {
                Err(crate::Error::ReplyCapacity(cause))
            }
            Err(usage::PushError::Front { front, .. }) => Self::row_front(front),
            Err(usage::PushError::Build { error, .. }) => {
                Err(crate::Error::ResponseRetention(error))
            }
            Err(usage::PushError::Footprint {
                declared: plan,
                actual,
                ..
            }) => Err(crate::Error::ReplyFootprint {
                declared: plan,
                actual,
            }),
        }
    }

    pub(super) fn reply_error<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        plan: reply::Plan<'input, 'd, R>,
        entry: Entry<'life, 'd, ID, I::Policy>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error>
    where
        'd: 'input,
    {
        use crate::port::replies;

        if matches!(
            self.state.phase,
            state::Phase::CopyInActive(_) | state::Phase::CopyOutActive(_)
        ) {
            self.state.phase = state::Phase::Ready(entry);
        }
        let mut queue = replies::Lane::new(self.port, self.conn, entry, self.region);
        let selected = queue.ordered();
        let plan = match Tag::current(selected.tag(), self.state.batch)? {
            None => {
                self.fail_server(plan.class(), plan.try_build()?);
                return Ok(None);
            }
            Some(tag) => {
                let skipping = matches!(
                    self.state.progress,
                    state::Progress::Skipping | state::Progress::Unknown
                );
                match tag.record_error(skipping, plan)? {
                    replies::ErrorRoute::Single(plan) => plan,
                    replies::ErrorRoute::Group {
                        scope,
                        class,
                        command,
                    } => {
                        match scope {
                            reply::Scope::Connection => {
                                self.fail_classified(class);
                            }
                            reply::Scope::Command | reply::Scope::Unknown if skipping => {}
                            reply::Scope::Unknown => self.state.progress = state::Progress::Unknown,
                            reply::Scope::Command => {
                                if command {
                                    let selected = selected.into_push();
                                    if matches!(selected.front(), Front::Detached) {
                                        let depth = selected.complete();
                                        queue.reindex(depth);
                                    } else {
                                        let depth = selected.fail_one(replies::GroupServer);
                                        queue.after_failure(depth);
                                    }
                                }
                                self.state.progress = state::Progress::Skipping;
                            }
                        }
                        return Ok(None);
                    }
                }
            }
        };
        let scope = plan.scope();
        let class = plan.class();
        let pending = match selected.into_push().try_failure() {
            Ok(failure) => match failure.try_fail(replies::ReplyFailure::Server(plan.try_build()?))
            {
                Ok(depth) => {
                    queue.reindex(depth);
                    None
                }
                Err((value, usage::Acquire::Backpressure(_))) => {
                    Some(logic::Pending::row(replies::Pending::Failure(value)))
                }
                Err((_, usage::Acquire::Capacity(cause))) => {
                    return Err(crate::Error::ReplyCapacity(cause));
                }
            },
            Err(Front::Detached) => {
                queue.complete();
                None
            }
            Err(Front::Empty) => {
                return Err(crate::Error::Protocol(Reason::ReadyUnknown));
            }
            Err(front) => {
                return Err(crate::Error::ReplyFront(front));
            }
        };
        match scope {
            reply::Scope::Connection => {
                self.fail_classified(class);
            }
            reply::Scope::Command | reply::Scope::Unknown => {
                self.state.progress = state::Progress::Skipping;
            }
        }
        Ok(pending)
    }
}
