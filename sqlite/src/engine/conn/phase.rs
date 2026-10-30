use crate::{api, catalogs, runtime};
use api::stmt;
use dope_core::driver::schedule;
use runtime::slot;
use std::{cell, mem};

pub(super) enum Phase<'local, 'd: 'local, I: catalogs::Catalog> {
    Live(Live<'local, 'd, I>),
    Closing(api::State<'local, 'd, I>),
    Retiring(slot::Retirement<'local, 'd, I>),
    Empty,
}

pub(super) struct Live<'local, 'd: 'local, I: catalogs::Catalog> {
    pub(super) state: cell::UnsafeCell<api::State<'local, 'd, I>>,
    pub(super) borrow: cell::Cell<Borrow>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum Borrow {
    Vacant,
    Borrowed,
    Closed,
    Closing,
}

impl Borrow {
    pub(super) const fn is_borrowed(self) -> bool {
        matches!(self, Self::Borrowed | Self::Closing)
    }

    pub(super) const fn returned(self) -> Self {
        match self {
            Self::Vacant | Self::Borrowed => Self::Vacant,
            Self::Closed | Self::Closing => Self::Closed,
        }
    }

    const fn stopped(self) -> Self {
        match self {
            Self::Vacant | Self::Closed => Self::Closed,
            Self::Borrowed | Self::Closing => Self::Closing,
        }
    }
}

impl<'local, 'd: 'local, I: catalogs::Catalog> Live<'local, 'd, I> {
    pub(super) fn new(state: api::State<'local, 'd, I>) -> Self {
        Self {
            state: cell::UnsafeCell::new(state),
            borrow: cell::Cell::new(Borrow::Vacant),
        }
    }

    fn idle(&self) -> Option<&api::State<'local, 'd, I>> {
        if self.borrow.get().is_borrowed() {
            return None;
        }
        // SAFETY: the vacant phase excludes every mutable client state access.
        Some(unsafe { &*self.state.get() })
    }
}

impl<'local, 'd: 'local, I: catalogs::Catalog> Phase<'local, 'd, I> {
    pub(super) fn progress<const STOPPING: bool>(&self) -> schedule::Progress<'d> {
        let state = match self {
            Self::Live(live) => match live.idle() {
                Some(state) => return state.progress(),
                None => return schedule::Progress::Quiescent,
            },
            Self::Closing(state) => state,
            Self::Retiring(retiring) => {
                return match retiring.inventory() {
                    stmt::Inventory::Populated if STOPPING => schedule::Progress::Runnable,
                    stmt::Inventory::Empty | stmt::Inventory::Populated => {
                        schedule::Progress::Quiescent
                    }
                };
            }
            Self::Empty => return schedule::Progress::Quiescent,
        };
        if STOPPING {
            state.stopping()
        } else {
            state.progress()
        }
    }

    pub(super) fn stop(&self) {
        if let Self::Live(live) = self {
            live.borrow.set(live.borrow.get().stopped());
        }
    }

    pub(super) fn released(&mut self) {
        *self = match mem::replace(self, Self::Empty) {
            Self::Live(live) => {
                let state = live.state.into_inner();
                state.shutdown();
                Self::Closing(state)
            }
            phase => phase,
        };
    }

    pub(super) fn retire(&mut self) {
        *self = match mem::replace(self, Self::Empty) {
            Self::Closing(state) => Self::Retiring(state.retire()),
            phase => phase,
        };
    }

    pub(super) fn force(&mut self) {
        let mut retiring = match mem::replace(self, Self::Empty) {
            Self::Live(live) => live.state.into_inner().retire(),
            Self::Closing(state) => state.retire(),
            Self::Retiring(retiring) => retiring,
            Self::Empty => return,
        };
        retiring.force();
        *self = Self::Retiring(retiring);
    }
}

const _: () = assert!(mem::size_of::<Borrow>() == 1);
