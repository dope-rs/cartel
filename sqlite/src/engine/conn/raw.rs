use crate::{
    api, catalogs,
    conn::{
        Installed,
        phase::{Live, Phase},
    },
    runtime,
};
use api::stmt;
use dope_core::driver::{schedule, settings};
use dope_runtime::{executor, topology::lending};
use std::{alloc::Layout, marker, pin, ptr};

pub(crate) struct Vacant<'local, 'd: 'local, I: catalogs::Catalog> {
    phase: ptr::NonNull<Phase<'local, 'd, I>>,
    backing: marker::PhantomData<&'local mut Phase<'local, 'd, I>>,
}

impl<'local, 'd: 'local, I: catalogs::Catalog> Vacant<'local, 'd, I> {
    pub(crate) const LAYOUT: Layout = Layout::new::<Phase<'local, 'd, I>>();

    /// # Safety
    /// Storage is an uninitialized, aligned LAYOUT partition retained for 'local.
    /// The consumed database admission grants its only initialization authority.
    pub(crate) unsafe fn new(storage: &'local runtime::raw::Span) -> Self {
        Self {
            phase: storage.base().cast(),
            backing: marker::PhantomData,
        }
    }

    pub(crate) fn insert(self, state: api::State<'local, 'd, I>) -> Installed<'local, 'd, I> {
        // SAFETY: this affine vacancy owns the admitted storage's sole initialization.
        unsafe { self.phase.as_ptr().write(Phase::Live(Live::new(state))) };
        Installed {
            phase: self.phase,
            ownership: marker::PhantomData,
        }
    }
}

impl<I: catalogs::Catalog> Drop for Installed<'_, '_, I> {
    fn drop(&mut self) {
        // SAFETY: client loans ended before this unique owner is destroyed. Local
        // retains the backing allocation, including when this owner is forgotten.
        unsafe { self.phase.as_ptr().drop_in_place() };
    }
}

// SAFETY: SQLite owns no driver routes or asynchronous native requests. Its SQL
// and VFS calls finish synchronously. Retry alarms retain resident timer leases;
// cleanup checks their absolute deadlines and clears them without native dispatch.
// The pinned connection and allocator remain owned through release and finish.
unsafe impl<'local, 'd: 'local, I: catalogs::Catalog> lending::raw::Resume<'d>
    for Installed<'local, 'd, I>
{
}

impl<'local, 'd: 'local, I: catalogs::Catalog> executor::raw::Layout<'d>
    for Installed<'local, 'd, I>
{
    type ParkPlan = ();
}

unsafe impl<'local, 'd: 'local, I: catalogs::Catalog> executor::raw::App<'d>
    for Installed<'local, 'd, I>
{
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 1;

    fn select_park() -> Option<Self::ParkPlan> {
        Some(())
    }

    fn released(
        mut released: executor::raw::Released<'_, Self>,
    ) -> executor::raw::Released<'_, Self> {
        let app = released.owner();
        let conn = app.as_ref().get_ref();
        // SAFETY: Released ends all client access to this exact pinned owner,
        // including loans whose guards were forgotten without running Drop.
        unsafe { &mut *conn.phase.as_ptr() }.released();
        released
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        shutdown: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        let (app, turn, driver) = shutdown.into_parts();
        let conn = app.as_ref().get_ref();
        // SAFETY: shared phase access preserves any dependent client's state borrow.
        unsafe { &*conn.phase.as_ptr() }.stop();
        executor::raw::Shutdown::new(app, turn, driver)
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (app, install, finish) = finish.into_parts();
        if finish.is_forced() {
            let conn = app.as_ref().get_ref();
            // SAFETY: all client borrows ended before this final pinned application access.
            unsafe { &mut *conn.phase.as_ptr() }.force();
        }
        executor::raw::Finish::new(app, install, finish)
    }

    fn progress(
        app: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        // SAFETY: progress only reads the phase; Live excludes active client state borrows.
        unsafe { &*app.get_ref().phase.as_ptr() }.progress::<false>()
    }

    fn stopping(
        app: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        // SAFETY: stopping only reads the phase; Live excludes active client state borrows.
        unsafe { &*app.get_ref().phase.as_ptr() }.progress::<true>()
    }

    /// # Safety
    /// The root, prepared plan, and park authority belong to this installed owner.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut Self>,
        (): &mut Self::ParkPlan,
        mut park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        let conn = app.as_ref().get_ref();
        // SAFETY: the phase remains pinned, and shared access preserves client state borrows.
        let phase = unsafe { &*conn.phase.as_ptr() };
        if let Phase::Live(live) = phase
            && live.borrow.get().is_borrowed()
        {
            return;
        }
        // SAFETY: phase admission excludes client borrows, and park excludes reentry.
        let phase = unsafe { &mut *conn.phase.as_ptr() };
        loop {
            let (state, plan) = match phase {
                Phase::Live(live) => {
                    // SAFETY: the vacant live phase excludes all mutable client state borrows.
                    let state = unsafe { &mut *live.state.get() };
                    let plan = state.maintenance(park.now());
                    (state, plan)
                }
                Phase::Closing(state) => {
                    let plan = state.maintenance(park.now());
                    if matches!(plan, api::Plan::Idle) {
                        phase.retire();
                        continue;
                    }
                    (state, plan)
                }
                Phase::Retiring(retiring) => {
                    if matches!(retiring.inventory(), stmt::Inventory::Empty) {
                        break;
                    }
                    if park.next_step().is_none() {
                        break;
                    }
                    retiring.retire_one();
                    continue;
                }
                Phase::Empty => break,
            };
            match plan {
                api::Plan::Idle | api::Plan::Delayed => break,
                api::Plan::Ready => {}
            }
            let Some(step) = park.next_step() else {
                break;
            };
            if state.maintain(step.now()).is_pending() {
                break;
            }
        }
    }
}
