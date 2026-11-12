use cartel_core::reply::{order::Front, usage};
use o3::buffer::{self, hold};

use crate::{
    port::{pool::access, replies::Completion},
    protocol::{
        Reason, frame,
        logic::{self, payloads, state, turn::Turn},
        messages,
        runtime::{ready::Entry, tx::poll},
    },
    reply, schema,
};

impl<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Turn<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    /// Command completion retains Ready replies until Sync resolves the transaction.
    pub(super) fn ready<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        typ: u8,
        data: frame::Payload<'input, 'd, R>,
        entry: Entry<'life, 'd, ID>,
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
                if typ == messages::Be::COMMAND_COMPLETE {
                    payloads::command(typ, data.wire())?;
                } else {
                    payloads::empty(typ, data.wire())?;
                }
                let mut queue =
                    crate::port::replies::Lane::new(self.port, self.conn, entry, self.region);
                let end = queue.front(self.state.batch)?.0;
                let Some(end) = end else {
                    return Err(crate::Error::Protocol(Reason::CommandEmpty));
                };
                match end {
                    Completion::Command | Completion::Group => queue.complete(),
                    Completion::Last => {
                        queue.complete();
                        self.state.progress = state::Progress::Complete;
                    }
                    Completion::Ready | Completion::Begin => {
                        self.state.progress = state::Progress::Complete;
                    }
                    Completion::Commit => match data.wire().as_slice() {
                        b"COMMIT\0" => self.state.progress = state::Progress::Complete,
                        b"ROLLBACK\0" => {
                            queue.fail_one(crate::port::replies::Failure::from_error(
                                crate::Error::CommitRolledBack,
                            ));
                            self.state.progress = state::Progress::Skipping;
                        }
                        _ => return Err(crate::Error::Protocol(Reason::CommitTag)),
                    },
                    Completion::Sync => {
                        return Err(crate::Error::UnexpectedReadyMessage(typ));
                    }
                }
                if copy {
                    self.state.phase = state::Phase::Ready(entry);
                }
                Ok(None)
            }
            messages::Be::COPY_DATA => {
                if !matches!(self.state.phase, state::Phase::CopyOutActive(_)) {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                self.push(data, entry)
            }
            messages::Be::DATA_ROW => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                self.push(data, entry)
            }
            messages::Be::NOTIFICATION_RESPONSE => {
                payloads::notification(data, &self.port.shared.notifications, self.region)?;
                Ok(None)
            }
            messages::Be::READY_FOR_QUERY => {
                let status = payloads::ready_status(data.wire())?;
                match status {
                    poll::Status::Transaction => {
                        match self.port.shared.transaction().current(self.conn) {
                            access::Current::Reserved(_) => {
                                let mut queue = crate::port::replies::Lane::new(
                                    self.port,
                                    self.conn,
                                    entry,
                                    self.region,
                                );
                                if !matches!(
                                    queue.front(self.state.batch)?.0,
                                    Some(Completion::Begin)
                                ) {
                                    return Err(crate::Error::Protocol(Reason::TransactionLease));
                                }
                            }
                            access::Current::Held(_) => {}
                            access::Current::Finalizing(_) => {
                                return Err(crate::Error::Protocol(Reason::TransactionOpen));
                            }
                            access::Current::Stale
                            | access::Current::Handshaking
                            | access::Current::Shared
                            | access::Current::Draining
                            | access::Current::Closing => {
                                return Err(crate::Error::Protocol(Reason::TransactionLease));
                            }
                        }
                    }
                    poll::Status::Idle => {
                        if matches!(
                            self.port.shared.transaction().current(self.conn),
                            access::Current::Held(_)
                        ) {
                            return Err(crate::Error::Protocol(Reason::TransactionLease));
                        }
                    }
                }
                if copy {
                    self.state.phase = state::Phase::Ready(entry);
                }
                Ok(Some(logic::Pending::ready(status)))
            }
            other => Err(crate::Error::UnexpectedReadyMessage(other)),
        }
    }

    fn push<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        data: frame::Payload<'input, 'd, R>,
        entry: Entry<'life, 'd, ID>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error>
    where
        'd: 'input,
    {
        use crate::port::replies;

        let mut queue = replies::Lane::new(self.port, self.conn, entry, self.region);
        let (end, selected) = queue.front(self.state.batch)?;
        match end {
            None => {
                return Err(crate::Error::Protocol(Reason::RowEmpty));
            }
            Some(Completion::Sync) => {
                return match selected.front() {
                    Front::Detached => Ok(None),
                    Front::Open | Front::Denied => Err(crate::Error::Protocol(Reason::CommandData)),
                    Front::Empty => Err(crate::Error::Protocol(Reason::ReadyUnknown)),
                };
            }
            Some(
                Completion::Command
                | Completion::Group
                | Completion::Last
                | Completion::Ready
                | Completion::Begin
                | Completion::Commit,
            ) => {}
        }
        match selected.try_build(replies::RowPlan::row(
            data,
            self.port.shared.config.retention_headroom(),
        )) {
            Ok(()) => Ok(None),
            Err(usage::PushError::Backpressure { builder: build, .. }) => {
                match build.into_pending() {
                    Ok(next) => Ok(Some(logic::Pending::row(next))),
                    Err((_, error)) => Err(crate::Error::ResponseRetention(error)),
                }
            }
            Err(usage::PushError::Capacity { cause, .. }) => {
                Err(crate::Error::ReplyCapacity(cause))
            }
            Err(usage::PushError::Front { front, .. }) => match front {
                Front::Detached => Ok(None),
                Front::Denied => Err(crate::Error::Protocol(Reason::CommandData)),
                Front::Empty => Err(crate::Error::Protocol(Reason::ReadyUnknown)),
                Front::Open => Err(crate::Error::ReplyFront(front)),
            },
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
        entry: Entry<'life, 'd, ID>,
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
        let (end, mut selected) = queue.front(self.state.batch)?;
        let plan = match end {
            None => plan,
            Some(_) => {
                let skipping = matches!(
                    self.state.progress,
                    state::Progress::Skipping | state::Progress::Unknown
                );
                match selected
                    .tag()
                    .ok_or(crate::Error::Protocol(Reason::ResponseUnknown))?
                    .record_error(skipping, plan)?
                {
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
                                    let detached = matches!(selected.front(), Front::Detached);
                                    if detached {
                                        queue.complete();
                                    } else {
                                        queue.fail_group();
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
        if end.is_none() {
            self.fail_server(class, plan.try_build()?);
            return Ok(None);
        }
        let pending = match selected.try_failure() {
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
