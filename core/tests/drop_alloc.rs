use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cartel_core::{Arena, Extract, Limits, Registrable, Reply, Slot};
use dope::runtime::profile::Throughput;
use dope::runtime::{Executor, StorageFactory};
use dope::{DriverContext, driver};

struct CountingAllocator;

static TRACKING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if TRACKING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACKING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

struct Counted;

impl Drop for Counted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}

struct Never;

impl Extract<Counted> for Never {
    type Output = ();

    fn extract(_slot: &mut Slot<'_, Counted>) -> Option<Self::Output> {
        None
    }
}

struct Factory;

impl StorageFactory for Factory {
    type Output<'d> = Arena<'d, Counted>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Arena::with_limits(4, Limits::new(4, 1, 4))
    }
}

#[test]
fn reply_registration_does_not_allocate_after_arena_construction() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    ALLOCATIONS.store(0, Ordering::Relaxed);
    let cfg = driver::Config::for_tcp_profile::<Throughput>(1);
    Executor::new(cfg)
        .expect("executor")
        .with_storage_factory(Factory)
        .enter(|mut session| {
            let arena = session.storage();
            let lane = arena.lane(0);
            let mut first = Reply::<_, Never>::new();
            let mut second = Reply::<_, Never>::new();
            let mut third = Reply::<_, Never>::new();
            let mut fourth = Reply::<_, Never>::new();
            TRACKING.store(true, Ordering::Relaxed);
            assert!(first.try_attach(session.driver_access().region_token(), lane));
            assert!(second.try_attach(session.driver_access().region_token(), lane));
            assert!(third.try_attach(session.driver_access().region_token(), lane));
            assert!(fourth.try_attach(session.driver_access().region_token(), lane));
            drop((first, second, third, fourth));
            TRACKING.store(false, Ordering::Relaxed);
        });

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
}

#[test]
fn arena_drop_does_not_allocate() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    ALLOCATIONS.store(0, Ordering::Relaxed);
    DROPS.store(0, Ordering::Relaxed);
    let cfg = driver::Config::for_tcp_profile::<Throughput>(1);
    Executor::new(cfg)
        .expect("executor")
        .with_storage_factory(Factory)
        .enter(|mut session| {
            let arena = session.storage();
            let lane = arena.lane(0);
            let mut reply = Reply::<_, Never>::new();
            assert!(reply.try_attach(session.driver_access().region_token(), lane));
            for _ in 0..4 {
                assert!(lane.try_push(session.driver_access().region_token(), Counted, 0, 1));
            }
            std::mem::forget(reply);
            TRACKING.store(true, Ordering::Relaxed);
        });
    TRACKING.store(false, Ordering::Relaxed);

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(DROPS.load(Ordering::Relaxed), 4);
}
