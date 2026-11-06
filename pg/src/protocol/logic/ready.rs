use std::mem;

use cartel_core::reply::{order::Front, usage};
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use o3::{
    base::cell::region,
    buffer::{self, hold},
};

use crate::{
    port::{self, pool::access, replies::Completion},
    protocol::{
        Reason, frame,
        logic::{self, failure, payloads, state},
        messages,
        runtime::tx::poll,
    },
    reply, schema,
};

pub(super) struct Ready<
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
    region: &'turn mut region::Token<region::Identity<'d>>,
}

impl<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Ready<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) const fn new(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        state: &'turn mut logic::State<'life, 'd, I, ID>,
        region: &'turn mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            port,
            conn,
            state,
            region,
        }
    }

    /// Command completion retains Ready replies until Sync resolves the transaction.
    pub(super) fn handle<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        typ: u8,
        data: frame::Payload<'input, 'd, R>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error>
    where
        'd: 'input,
    {
        if (self.state.completed || !matches!(self.state.skip, state::Skip::None))
            && !matches!(
                typ,
                messages::Be::NOTICE_RESPONSE
                    | messages::Be::PARAMETER_STATUS
                    | messages::Be::NOTIFICATION_RESPONSE
                    | messages::Be::READY_FOR_QUERY
            )
        {
            return Err(crate::Error::UnexpectedReadyMessage(typ));
        }
        let copy = matches!(
            self.state.phase,
            state::Phase::CopyInActive | state::Phase::CopyOutActive
        );
        match typ {
            messages::Be::ROW_DESCRIPTION | messages::Be::PARAMETER_DESCRIPTION => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                Ok(None)
            }
            messages::Be::NOTICE_RESPONSE => Ok(None),
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
                if !matches!(self.state.phase, state::Phase::CopyOutActive) {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::empty(typ, data.wire())?;
                Ok(None)
            }
            messages::Be::COPY_IN_RESPONSE => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::copy_format(data.wire())?;
                self.state.phase = state::Phase::CopyInActive;
                Ok(None)
            }
            messages::Be::COPY_OUT_RESPONSE => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                payloads::copy_format(data.wire())?;
                self.state.phase = state::Phase::CopyOutActive;
                Ok(None)
            }
            messages::Be::COMMAND_COMPLETE | messages::Be::EMPTY_QUERY_RESPONSE => {
                if typ == messages::Be::COMMAND_COMPLETE {
                    payloads::command(typ, data.wire())?;
                } else {
                    payloads::empty(typ, data.wire())?;
                }
                let queue = self
                    .port
                    .responses(self.conn)
                    .ok_or(crate::Error::Protocol(Reason::CommandUnknown))?;
                let end = queue.front(self.state.batch, self.region)?.0;
                let Some(end) = end else {
                    return Err(crate::Error::Protocol(Reason::CommandEmpty));
                };
                match end {
                    Completion::Command | Completion::Group => queue.complete(self.region),
                    Completion::Last => {
                        queue.complete(self.region);
                        self.state.completed = true;
                    }
                    Completion::Ready | Completion::Begin => self.state.completed = true,
                    Completion::Commit => match data.wire().as_slice() {
                        b"COMMIT\0" => self.state.completed = true,
                        b"ROLLBACK\0" => {
                            queue.fail_one(
                                self.region,
                                crate::port::replies::Failure::from_error(
                                    crate::Error::CommitRolledBack,
                                ),
                            );
                            self.state.skip = state::Skip::Pending;
                        }
                        _ => return Err(crate::Error::Protocol(Reason::CommitTag)),
                    },
                    Completion::Sync => {
                        return Err(crate::Error::UnexpectedReadyMessage(typ));
                    }
                }
                if copy {
                    self.state.phase = state::Phase::Ready;
                }
                Ok(None)
            }
            messages::Be::COPY_DATA => {
                if !matches!(self.state.phase, state::Phase::CopyOutActive) {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                self.push(data)
            }
            messages::Be::DATA_ROW => {
                if copy {
                    return Err(crate::Error::UnexpectedReadyMessage(typ));
                }
                self.push(data)
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
                                let queue = self
                                    .port
                                    .responses(self.conn)
                                    .ok_or(crate::Error::Protocol(Reason::ReadyUnknown))?;
                                if !matches!(
                                    queue.front(self.state.batch, self.region)?.0,
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
                    self.state.phase = state::Phase::Ready;
                }
                let next = state::Drain {
                    skip: mem::take(&mut self.state.skip),
                    completed: mem::take(&mut self.state.completed),
                    status,
                    batch: self.state.batch,
                };
                Ok(Some(logic::Pending::ready(next)))
            }
            other => Err(crate::Error::UnexpectedReadyMessage(other)),
        }
    }

    fn push<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        data: frame::Payload<'input, 'd, R>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error>
    where
        'd: 'input,
    {
        use crate::port::replies;

        let queue = self
            .port
            .responses(self.conn)
            .ok_or(crate::Error::Protocol(Reason::RowUnknown))?;
        let (end, selected) = queue.front(self.state.batch, self.region)?;
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
        match selected.try_build(replies::RowPlan::row(data)) {
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

    pub(super) fn error<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        plan: reply::Plan<'input, 'd, R>,
    ) -> Result<Option<logic::Pending<'d>>, crate::Error>
    where
        'd: 'input,
    {
        use crate::port::replies;

        self.state.completed = false;
        if matches!(
            self.state.phase,
            state::Phase::CopyInActive | state::Phase::CopyOutActive
        ) {
            self.state.phase = state::Phase::Ready;
        }
        let queue = self
            .port
            .responses(self.conn)
            .ok_or(crate::Error::Protocol(Reason::ResponseUnknown))?;
        let (end, mut selected) = queue.front(self.state.batch, self.region)?;
        let plan = match end {
            None => plan,
            Some(_) => {
                let skipping = !matches!(self.state.skip, state::Skip::None);
                match selected
                    .tag()
                    .ok_or(crate::Error::Protocol(Reason::ResponseUnknown))?
                    .record_error(skipping, plan)?
                {
                    replies::ErrorRoute::Single(plan) => plan,
                    replies::ErrorRoute::Group {
                        scope,
                        class,
                        completion: end,
                    } => {
                        match scope {
                            reply::Scope::Connection => {
                                failure::Failure::new(
                                    self.port,
                                    self.conn,
                                    self.state,
                                    self.region,
                                )
                                .apply_retained(class);
                            }
                            reply::Scope::Command | reply::Scope::Unknown if skipping => {}
                            reply::Scope::Unknown => self.state.skip = state::Skip::Unknown,
                            reply::Scope::Command => {
                                match end {
                                    Completion::Group | Completion::Last => {
                                        let detached = matches!(selected.front(), Front::Detached);
                                        if detached {
                                            queue.complete(self.region);
                                        } else {
                                            queue.fail_group(self.region);
                                        }
                                    }
                                    Completion::Sync => {}
                                    Completion::Command
                                    | Completion::Ready
                                    | Completion::Begin
                                    | Completion::Commit => {
                                        return Err(crate::Error::Protocol(Reason::GroupReply));
                                    }
                                }
                                self.state.skip = state::Skip::Pending;
                            }
                        }
                        return Ok(None);
                    }
                }
            }
        };
        let scope = plan.scope();
        let (server, class) = match plan.try_build() {
            Ok(built) => built,
            Err(error) => return Err(crate::Error::ResponseRetention(error)),
        };
        if end.is_none() {
            failure::Failure::new(self.port, self.conn, self.state, self.region)
                .apply_server(class, server);
            return Ok(None);
        }
        let pending = match selected.try_fail(replies::ReplyFailure::Server(server)) {
            Ok(depth) => {
                self.port.shared.reindex(self.conn, depth);
                None
            }
            Err(usage::InsertError::Backpressure { value, .. }) => {
                Some(logic::Pending::row(replies::Pending::Failure(value)))
            }
            Err(usage::InsertError::Capacity { cause, .. }) => {
                return Err(crate::Error::ReplyCapacity(cause));
            }
            Err(usage::InsertError::Front {
                front: Front::Detached,
                ..
            }) => {
                queue.complete(self.region);
                None
            }
            Err(usage::InsertError::Front {
                front: Front::Empty,
                ..
            }) => {
                return Err(crate::Error::Protocol(Reason::ReadyUnknown));
            }
            Err(usage::InsertError::Front { front, .. }) => {
                return Err(crate::Error::ReplyFront(front));
            }
        };
        match scope {
            reply::Scope::Connection => {
                failure::Failure::new(self.port, self.conn, self.state, self.region)
                    .apply_retained(class);
            }
            reply::Scope::Command | reply::Scope::Unknown => {
                self.state.skip = state::Skip::Pending;
            }
        }
        Ok(pending)
    }
}
