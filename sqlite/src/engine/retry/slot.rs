use crate::{config, sys};
use dope_core::driver::{
    schedule::clock::{self, timer},
    schedule::ready::wake,
};
use std::{mem, task};

pub(crate) struct Slot<'d> {
    alarm: Option<timer::Alarm<'d>>,
    state: State<'d>,
}

#[derive(Clone, Copy)]
enum State<'d> {
    Idle,
    Ready(Budget<'d>),
    PendingYield(Budget<'d>),
    PendingSleep(Budget<'d>),
}

#[derive(Clone, Copy)]
struct Budget<'d> {
    deadline: clock::Deadline<'d>,
    fault: sys::Fault,
    remaining: u32,
}

pub(crate) enum Wait {
    Ready,
    Pending,
}

impl<'d> Slot<'d> {
    pub(crate) const fn yielding() -> Self {
        Self {
            alarm: None,
            state: State::Idle,
        }
    }

    pub(crate) const fn timed(alarm: timer::Alarm<'d>) -> Self {
        Self {
            alarm: Some(alarm),
            state: State::Idle,
        }
    }

    pub(super) fn defer(
        &mut self,
        limits: &config::Contention,
        now: clock::Moment<'d>,
        fault: sys::Fault,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        let mut budget = match self.state {
            State::Idle => Budget {
                deadline: now.clamp_add(limits.window),
                fault,
                remaining: limits.retries.get(),
            },
            State::Ready(mut budget) => {
                self.state = State::Idle;
                budget.fault = fault;
                budget
            }
            State::PendingYield(_) | State::PendingSleep(_) => return self.poll(now, wake),
        };
        let Some(left) = budget.remaining.checked_sub(1) else {
            return task::Poll::Ready(Err(fault.busy()));
        };
        budget.remaining = left;
        if budget.deadline.is_due(now) {
            return task::Poll::Ready(Err(fault.timeout()));
        }
        if let Some(alarm) = &self.alarm
            && let config::Delay::Sleep(interval) = limits.delay()
        {
            let wake_at = now.clamp_add(interval).min(budget.deadline);
            alarm.arm(wake_at, wake);
            self.state = State::PendingSleep(budget);
        } else {
            self.state = State::PendingYield(budget);
            wake.wake();
        }
        task::Poll::Pending
    }

    pub(super) const fn wait(&self) -> Wait {
        match &self.state {
            State::Idle | State::Ready(_) => Wait::Ready,
            State::PendingYield(_) | State::PendingSleep(_) => Wait::Pending,
        }
    }

    pub(super) fn poll(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        let (budget, alarm) = match &self.state {
            State::PendingYield(budget) => (budget, None),
            State::PendingSleep(budget) => (budget, self.alarm.as_ref()),
            State::Idle | State::Ready(_) => return task::Poll::Ready(Ok(())),
        };
        if budget.deadline.is_due(now) {
            if let Some(alarm) = alarm {
                alarm.clear();
            }
            let fault = budget.fault;
            self.state = State::Idle;
            return task::Poll::Ready(Err(fault.timeout()));
        }
        if let Some(alarm) = alarm
            && alarm.poll(now, wake).is_pending()
        {
            return task::Poll::Pending;
        }
        self.state = State::Ready(*budget);
        task::Poll::Ready(Ok(()))
    }

    pub(crate) fn finish(&mut self) {
        if let Some(alarm) = &self.alarm {
            alarm.clear();
        }
        self.state = State::Idle;
    }
}

const _: () = {
    assert!(
        mem::size_of::<Option<timer::Alarm<'static>>>() == mem::size_of::<timer::Alarm<'static>>()
    );
    assert!(
        mem::size_of::<State<'static>>()
            <= mem::size_of::<Budget<'static>>() + mem::align_of::<Budget<'static>>()
    );
    assert!(
        mem::size_of::<Slot<'static>>()
            == mem::size_of::<timer::Alarm<'static>>() + mem::size_of::<State<'static>>()
    );
    assert!(mem::align_of::<Slot<'static>>() == mem::align_of::<timer::Alarm<'static>>());
};
