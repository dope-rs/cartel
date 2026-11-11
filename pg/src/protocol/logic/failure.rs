use dope::{core::driver::result::SubmitCause, manifold::client::conn};
use o3::buffer::{hold, resident, storage::provided::account};
use resident::copy;

use crate::{
    port::{Disposition, replies},
    protocol::logic::{state, turn::Turn},
    reply::{self, server},
    schema, scram,
};

impl<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Turn<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) fn fail(&mut self, error: crate::Error) {
        let class = match &error {
            crate::Error::Transmit(conn::Transmit::Rejected(failure))
                if failure.into_error().cause() == SubmitCause::IdentityExhausted =>
            {
                reply::Class::Permanent
            }
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
            | crate::Error::Transmit(_)
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
        self.finish_failure(class, cause, None);
    }

    pub(super) fn fail_server(&mut self, class: reply::Class, error: server::Diagnostic<'d>) {
        self.finish_failure(
            class,
            replies::Failure::from_error(crate::Error::Closed),
            Some(error),
        );
    }

    pub(super) fn fail_classified(&mut self, class: reply::Class) {
        self.finish_failure(
            class,
            replies::Failure::from_error(crate::Error::Closed),
            None,
        );
    }

    fn finish_failure(
        &mut self,
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
        self.state.progress = state::Progress::Running;
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
