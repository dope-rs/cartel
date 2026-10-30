use crate::api::work::guard::Guard;

pub(crate) enum Call {}

impl<'work> Guard<'work, Call> {
    /// Consumes the sole healthy call authority while retaining its meter cleanup.
    pub(crate) fn defer(self, error: crate::Error) -> impl Sized + use<'work> {
        self.work.health.defer(error);
        self
    }
}
