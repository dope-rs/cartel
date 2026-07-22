use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cartel_core::{Arena, Extract, Limits, Registrable, Reply, Slot};

struct CountingAllocator;

static TRACKING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);

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

unsafe impl Extract<Counted> for Never {
    type Output = ();

    fn extract(_slot: &mut Slot<'_, Counted>) -> Option<Self::Output> {
        None
    }
}

#[test]
fn arena_drop_does_not_allocate() {
    let arena = Arena::with_limits(1, Limits::new(4, 1, 4));
    let arena_ref = unsafe { &*(&arena as *const Arena<'_, Counted>) };
    let mut reply = Reply::<_, Never>::new();
    assert!(reply.try_attach(arena_ref));
    for _ in 0..4 {
        assert!(arena.try_push(Counted, 0, 1));
    }
    std::mem::forget(reply);

    ALLOCATIONS.store(0, Ordering::Relaxed);
    DROPS.store(0, Ordering::Relaxed);
    TRACKING.store(true, Ordering::Relaxed);
    drop(arena);
    TRACKING.store(false, Ordering::Relaxed);

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(DROPS.load(Ordering::Relaxed), 4);
}
