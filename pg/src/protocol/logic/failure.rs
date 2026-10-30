use dope::{
    core::driver::route::KeyTag,
    manifold::client::{conn, session::turns},
};
use o3::base::cell::region;
use o3::buffer::{hold, resident, storage::provided::account};
use resident::copy;

use crate::{
    port::{self, Disposition, replies, requests::Frame},
    protocol::logic::{State, state},
    reply::{self, server},
    schema, scram,
};

pub(super) struct Failure<
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
    state: &'turn mut State<'life, 'd, I, ID>,
    region: &'turn mut region::Token<region::Identity<'d>>,
}

impl<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Failure<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) const fn new(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        state: &'turn mut State<'life, 'd, I, ID>,
        region: &'turn mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            port,
            conn,
            state,
            region,
        }
    }

    pub(super) fn in_context<'ctx, 'e>(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        ctx: &'turn mut turns::Context<
            'ctx,
            'e,
            'd,
            State<'life, 'd, I, ID>,
            Frame<'d>,
            KeyTag<ID>,
        >,
    ) -> Self
    where
        'ctx: 'turn,
    {
        let parts = ctx.parts();
        Self::new(
            port,
            parts.conn_id,
            parts.state,
            &mut parts.driver.into_regions().0,
        )
    }

    pub(super) fn apply(self, error: crate::Error) {
        let class = match &error {
            crate::Error::Input(_)
            | crate::Error::Retirement { .. }
            | crate::Error::Scram(
                scram::Error::InvalidEncoding
                | scram::Error::ExtensionsNotSupported
                | scram::Error::InvalidProof
                | scram::Error::ChannelBindingsDontMatch
                | scram::Error::ServerDoesSupportChannelBinding
                | scram::Error::ChannelBindingNotSupported
                | scram::Error::UnsupportedChannelBindingType
                | scram::Error::UnknownUser
                | scram::Error::InvalidUsernameEncoding
                | scram::Error::Other,
            )
            | crate::Error::Auth(_)
            | crate::Error::UnsupportedAuthMethod(_)
            | crate::Error::ServerFirst(_)
            | crate::Error::ServerFirstBase64(_)
            | crate::Error::ServerFinal(_)
            | crate::Error::ServerFinalBase64(_)
            | crate::Error::TransactionSlot(_)
            | crate::Error::EgressSlot(_)
            | crate::Error::ScramSlot(_)
            | crate::Error::ScramIterations { .. }
            | crate::Error::Protocol(_)
            | crate::Error::DescriptionLtreeOidZero
            | crate::Error::DescriptionRowCountTruncated { .. }
            | crate::Error::DescriptionRowCountNegative { .. }
            | crate::Error::DescriptionColumnCount { .. }
            | crate::Error::DescriptionDecoderOverflow { .. }
            | crate::Error::DescriptionFieldName { .. }
            | crate::Error::DescriptionFieldMetadata { .. }
            | crate::Error::DescriptionFieldFormat { .. }
            | crate::Error::DescriptionOid { .. }
            | crate::Error::DescriptionStringOid { .. }
            | crate::Error::DescriptionStringArrayOid { .. }
            | crate::Error::DescriptionLtreeOid { .. }
            | crate::Error::DescriptionUnvalidated { .. }
            | crate::Error::DescriptionRowTrailing { .. }
            | crate::Error::DescriptionParameterCountTruncated { .. }
            | crate::Error::DescriptionParameterCountNegative { .. }
            | crate::Error::DescriptionParameterCount { .. }
            | crate::Error::DescriptionParameterOidTruncated { .. }
            | crate::Error::DescriptionParameterOid { .. }
            | crate::Error::DescriptionParameterTrailing { .. }
            | crate::Error::DescriptionUnexpectedRows
            | crate::Error::QueryMetadata
            | crate::Error::UnexpectedStartupMessage(_)
            | crate::Error::UnexpectedSaslMessage(_)
            | crate::Error::UnexpectedEagerPrepareMessage(_)
            | crate::Error::UnexpectedReadyMessage(_)
            | crate::Error::Request(_)
            | crate::Error::CopyFinishWithoutStart
            | crate::Error::CopyFinishDuringPipeline { .. }
            | crate::Error::ReplyCapacity(_)
            | crate::Error::GroupDiagnosticRecords { .. }
            | crate::Error::GroupDiagnosticBytes(_)
            | crate::Error::ReplyFront(_)
            | crate::Error::ReplyFootprint { .. }
            | crate::Error::ResponseRetention(
                hold::Error::Range(_)
                | hold::Error::Resident(
                    resident::AllocError::Capacity(_) | resident::AllocError::Layout(_),
                )
                | hold::Error::Account(account::Cause::Foreign),
            )
            | crate::Error::StorageCapacity(_) => reply::Class::Permanent,
            crate::Error::Scram(scram::Error::NoResources)
            | crate::Error::Closed
            | crate::Error::Connection(_)
            | crate::Error::Receive(_)
            | crate::Error::Timeout(_)
            | crate::Error::RequestTimeout(_)
            | crate::Error::Random(_)
            | crate::Error::NotFound
            | crate::Error::Deadline(_)
            | crate::Error::RequestEgress(_)
            | crate::Error::RequestLoad(_)
            | crate::Error::RequestReserve(_)
            | crate::Error::RequestCapacity
            | crate::Error::RequestDepthOverflow { .. }
            | crate::Error::GroupOpen { .. }
            | crate::Error::GroupDisabled
            | crate::Error::GroupOwnerCapacity
            | crate::Error::GroupRecordCapacity
            | crate::Error::GroupBytesCapacity
            | crate::Error::RequestCloseDuringCopy { .. }
            | crate::Error::RequestOpenDuringCopy { .. }
            | crate::Error::CopyDuringPipeline { .. }
            | crate::Error::CopyAlreadyOpen { .. }
            | crate::Error::RequestQueue { .. }
            | crate::Error::Reply(_)
            | crate::Error::ResponseRetention(
                hold::Error::Resident(
                    resident::AllocError::Budget(_)
                    | resident::AllocError::Retain(_)
                    | resident::AllocError::Exhausted(_),
                )
                | hold::Error::Copy(copy::Error::Budget(_) | copy::Error::Allocation(_))
                | hold::Error::Account(
                    account::Cause::Exhausted { .. }
                    | account::Cause::Budget(_)
                    | account::Cause::Retain(_),
                ),
            )
            | crate::Error::ResponseRetain(_)
            | crate::Error::Allocation(_)
            | crate::Error::StorageExhausted(_)
            | crate::Error::NotificationDropOverflow
            | crate::Error::Skipped
            | crate::Error::CommitRolledBack
            | crate::Error::Backpressure { .. } => reply::Class::Transient,
        };
        let cause = replies::Failure::from_error(error);
        self.finish(class, cause, None);
    }

    pub(super) fn apply_server(self, class: reply::Class, error: server::Diagnostic<'d>) {
        self.finish(
            class,
            replies::Failure::from_error(crate::Error::Closed),
            Some(error),
        );
    }

    /// The diagnostic is retained by a reply, group ledger, or pending delivery.
    pub(super) fn apply_retained(self, class: reply::Class) {
        self.finish(
            class,
            replies::Failure::from_error(crate::Error::Closed),
            None,
        );
    }

    fn finish(
        self,
        class: reply::Class,
        cause: replies::Failure,
        server: Option<server::Diagnostic<'d>>,
    ) {
        let wake = self.state.cancel(self.region);
        if let Some(wake) = wake {
            self.port.connection().wake(wake);
        }
        let mode = match (&self.state.phase, class) {
            (
                state::Phase::StartupSent
                | state::Phase::Sasl { .. }
                | state::Phase::AwaitingReady
                | state::Phase::Preparing { .. },
                reply::Class::Permanent,
            ) => Disposition::Permanent,
            (
                state::Phase::NeedsStartup
                | state::Phase::Ready
                | state::Phase::CopyInActive
                | state::Phase::CopyOutActive
                | state::Phase::Failed,
                reply::Class::Permanent,
            )
            | (
                state::Phase::NeedsStartup
                | state::Phase::StartupSent
                | state::Phase::Sasl { .. }
                | state::Phase::AwaitingReady
                | state::Phase::Preparing { .. }
                | state::Phase::Ready
                | state::Phase::CopyInActive
                | state::Phase::CopyOutActive
                | state::Phase::Failed,
                reply::Class::Transient,
            ) => Disposition::Reconnect,
        };
        self.state.phase = state::Phase::Failed;
        self.state.skip = state::Skip::None;
        self.state.completed = false;
        let shared = &self.port.shared;
        match mode {
            Disposition::Permanent => {
                let cause = shared.wake_close(self.conn, cause, Disposition::Permanent);
                match server {
                    Some(error) => shared.signals.record_reply(error),
                    None => shared.signals.record_fatal(cause.into_error()),
                }
            }
            Disposition::Reconnect => {
                shared.wake_close(self.conn, cause, Disposition::Reconnect);
            }
        }
    }
}
