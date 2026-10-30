use crate::{api, config};
use api::{stmt, work};
use std::{cell, mem, num};
use work::cleanup::Status;

/// The shallowest admitted depth still owed rollback and release, or the whole transaction.
pub(crate) struct Pending(cell::Cell<State>);

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
struct State(usize);

#[derive(Clone, Copy)]
enum Phase {
    Rollback,
    Release,
}

const NONE: State = State(0);
const TX: State = State(1);
const FAILED: State = State(usize::MAX);
const TAG: usize = 1;

#[repr(transparent)]
pub(in crate::engine::api::work) struct Cleanup(num::NonZero<usize>);

impl Pending {
    pub(crate) const fn new() -> Self {
        Self(cell::Cell::new(NONE))
    }

    pub(in crate::engine::api::work) fn control(&self) -> Option<Cleanup> {
        match self.0.get() {
            FAILED => None,
            state => num::NonZero::<usize>::new(state.0).map(Cleanup),
        }
    }

    pub(crate) fn transaction(&self) {
        if self.0.get() != FAILED {
            self.0.set(TX);
        }
    }

    pub(crate) fn savepoint(&self, depth: work::Count) {
        let state = self.0.get();
        let next = match state {
            NONE => Self::encode(depth.get(), Phase::Rollback),
            TX | FAILED => return,
            state => {
                let phase = match state.0 & TAG {
                    0 => Phase::Rollback,
                    _ => Phase::Release,
                };
                Self::encode((state.0 >> 1).min(depth.get()), phase)
            }
        };
        self.0.set(next);
    }

    pub(in crate::engine::api::work) fn complete(&self, cleanup: Cleanup, depth: &work::Depth) {
        let state = State(cleanup.0.get());
        let next = if state == TX {
            depth.reset();
            NONE
        } else {
            match state.0 & TAG {
                0 => Self::encode(state.0 >> 1, Phase::Release),
                _ => {
                    // SAFETY: Call retains this exact source's affine debt. Native
                    // cleanup cannot mutate depth; failure terminalizes its source
                    // before resetting, and no receipt survives a yield or retry.
                    let release = unsafe { work::raw::Release::new(depth) };
                    let target = state.0 >> 1;
                    let done = release.depth().get() == target;
                    release.close();
                    if done {
                        NONE
                    } else {
                        Self::encode(target, Phase::Rollback)
                    }
                }
            }
        };
        self.0.set(next);
    }

    pub(crate) fn status(&self) -> Status {
        match self.0.get() {
            NONE | FAILED => Status::Idle,
            _ => Status::Pending,
        }
    }

    pub(crate) fn reset(&self) {
        self.0.set(NONE);
    }

    /// Existing scopes cannot schedule more work after cleanup has lost its native depth.
    pub(in crate::engine::api::work) fn fail(&self) {
        self.0.set(FAILED);
    }

    const fn encode(target: usize, phase: Phase) -> State {
        let tag = match phase {
            Phase::Rollback => 0,
            Phase::Release => TAG,
        };
        State((target << 1) | tag)
    }
}

impl Cleanup {
    pub(in crate::engine::api::work) fn control(&self) -> stmt::Control {
        let state = State(self.0.get());
        if state == TX {
            stmt::Control::Rollback
        } else if state.0 & TAG == 0 {
            stmt::Control::RollbackTo
        } else {
            stmt::Control::Release
        }
    }
}

const _: () = {
    assert!(config::MAX_SAVEPOINTS < usize::MAX >> 1);
    assert!(mem::size_of::<State>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Pending>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Cleanup>>() == mem::size_of::<usize>());
};
