use std::{cell, mem, pin, task};

use dope::core::driver::schedule::clock::timer;
use dope_fiber::context;
use o3::base::time::Positive;

/// The bounded operation phase in which an absolute request deadline elapsed.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum RequestPhase {
    Admission,
    Response,
    Settlement,
    Lease,
    Contention,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Elapsed {
    phase: RequestPhase,
}

impl Elapsed {
    pub const fn new(phase: RequestPhase) -> Self {
        Self { phase }
    }

    pub const fn phase(self) -> RequestPhase {
        self.phase
    }
}

/// Copyable authority to create deadlines tied to one exact driver lifetime.
#[derive(Clone, Copy)]
pub struct Source<'d> {
    timer: timer::Timer<'d>,
    window: Positive,
}

impl<'d> Source<'d> {
    pub const fn new(timer: timer::Timer<'d>, window: Positive) -> Self {
        Self { timer, window }
    }

    pub fn guard(self) -> Result<Guard<'d>, timer::Exhausted> {
        Ok(Guard {
            alarm: self.timer.try_alarm()?,
            window: self.window,
            state: cell::Cell::new(GuardState::Fresh),
        })
    }

    pub const fn into_parts(self) -> (timer::Timer<'d>, Positive) {
        (self.timer, self.window)
    }
}

/// One first-poll-anchored absolute deadline. Phase changes never rearm it.
pub struct Guard<'d> {
    alarm: timer::Alarm<'d>,
    window: Positive,
    state: cell::Cell<GuardState>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum GuardState {
    Fresh,
    Armed,
    Elapsed,
    Complete,
}

impl<'d> Guard<'d> {
    pub fn poll_expired(
        self: pin::Pin<&mut Self>,
        mut ctx: pin::Pin<&mut context::Context<'_, 'd>>,
        phase: RequestPhase,
    ) -> task::Poll<Elapsed> {
        let this = self.get_mut();
        match this.state.get() {
            GuardState::Elapsed => return task::Poll::Ready(Elapsed::new(phase)),
            GuardState::Complete => return task::Poll::Pending,
            GuardState::Fresh => {
                let wake = ctx.as_ref().completion();
                let now = ctx.as_mut().driver().driver_ref().clock().now();
                let due = now.clamp_add(this.window);
                this.state.set(GuardState::Armed);
                this.alarm.arm(due, wake);
                return task::Poll::Pending;
            }
            GuardState::Armed => {}
        }
        let wake = ctx.as_ref().completion();
        let now = ctx.as_mut().driver().driver_ref().clock().now();
        if this.alarm.poll(now, wake).is_ready() {
            this.state.set(GuardState::Elapsed);
            task::Poll::Ready(Elapsed::new(phase))
        } else {
            task::Poll::Pending
        }
    }

    pub fn complete(&self) {
        self.alarm.clear();
        self.state.set(GuardState::Complete);
    }
}

const _: () = {
    assert!(mem::size_of::<GuardState>() == 1);
    assert!(mem::size_of::<cell::Cell<GuardState>>() == mem::size_of::<GuardState>());
    assert!(mem::align_of::<cell::Cell<GuardState>>() == mem::align_of::<GuardState>());
};
