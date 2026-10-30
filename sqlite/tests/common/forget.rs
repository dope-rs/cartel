use dope_core::driver::schedule;
use dope_runtime::executor::boot;
use std::mem;

pub const COMMANDS: &[&str] =
    &["CREATE TABLE IF NOT EXISTS payloads (data BLOB NOT NULL)"; schedule::LIMIT];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Pending,
    Installed,
}

pub struct Forgotten<R> {
    pub root: R,
    pub stage: Stage,
}

#[derive(Debug)]
pub enum Error<E> {
    Forgotten(Stage),
    Open(E),
}

impl<'d, R, A, E> boot::Root<'d> for Forgotten<R>
where
    R: boot::Root<'d, Output = Result<A, E>>,
{
    type Output = Result<A, Error<E>>;

    fn poll(self, context: boot::Context<'_, 'd>) -> boot::Poll<Self, Self::Output> {
        match self.root.poll(context) {
            boot::Poll::Pending(root) => match self.stage {
                Stage::Pending => {
                    mem::forget(root);
                    boot::Poll::Ready(Err(Error::Forgotten(Stage::Pending)))
                }
                Stage::Installed => boot::Poll::Pending(Self {
                    root,
                    stage: self.stage,
                }),
            },
            boot::Poll::Ready(Err(error)) => boot::Poll::Ready(Err(Error::Open(error))),
            boot::Poll::Ready(Ok(app)) => {
                mem::forget(app);
                boot::Poll::Ready(Err(Error::Forgotten(Stage::Installed)))
            }
        }
    }

    fn cancel(self) {
        self.root.cancel();
    }
}
