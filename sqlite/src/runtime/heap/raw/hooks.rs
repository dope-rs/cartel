use std::{alloc, cell, num::NonZero, ptr};

use o3::base::marker::ThreadBound;

use crate::runtime::heap;
use heap::scopes::Scope;

pub(in crate::runtime) fn methods() -> libsqlite3_sys::sqlite3_mem_methods {
    libsqlite3_sys::sqlite3_mem_methods {
        xMalloc: Some(malloc),
        xFree: Some(free),
        xRealloc: Some(realloc),
        xSize: Some(size),
        xRoundup: Some(roundup),
        xInit: Some(initialize),
        xShutdown: None,
        pAppData: ptr::null_mut(),
    }
}

unsafe extern "C" fn malloc(bytes: std::ffi::c_int) -> *mut std::ffi::c_void {
    let Some(bytes) = Len::new(bytes) else {
        return ptr::null_mut();
    };
    Origin::allocate(bytes).cast()
}

unsafe extern "C" fn free(pointer: *mut std::ffi::c_void) {
    // SAFETY: sqlite3_free rejects NULL before dispatching this live allocation.
    unsafe { Block::read(ptr::NonNull::new_unchecked(pointer.cast())) }.release();
}

/// Native zero-size requests are already freed; rejected rounding preserves the old allocation.
unsafe extern "C" fn realloc(
    pointer: *mut std::ffi::c_void,
    bytes: std::ffi::c_int,
) -> *mut std::ffi::c_void {
    let Some(bytes) = Len::new(bytes) else {
        return ptr::null_mut();
    };
    // SAFETY: sqlite3Realloc routes NULL to sqlite3Malloc before dispatching this live allocation.
    unsafe { Block::read(ptr::NonNull::new_unchecked(pointer.cast())) }
        .resize(bytes)
        .cast()
}

unsafe extern "C" fn size(pointer: *mut std::ffi::c_void) -> std::ffi::c_int {
    // SAFETY: sqlite3MallocSize and sqlite3DbMallocSize pass live original allocations;
    // sqlite3_msize handles NULL before dispatch. These three native entrances preserve
    // the nonnull payload and its initialized hidden header.
    unsafe { Block::read(ptr::NonNull::new_unchecked(pointer.cast())) }.size()
}

unsafe extern "C" fn roundup(bytes: std::ffi::c_int) -> std::ffi::c_int {
    let Some(order) = Len::new(bytes).and_then(Len::order) else {
        return 0;
    };
    heap::raw::payload_integer(order)
}

unsafe extern "C" fn initialize(_: *mut std::ffi::c_void) -> std::ffi::c_int {
    libsqlite3_sys::SQLITE_OK
}

thread_local! {
    static CURRENT: cell::Cell<Origin> = const { cell::Cell::new(Origin::Core) };
}

#[derive(Clone, Copy)]
enum Origin {
    Core,
    Foreign,
}

struct Allocation {
    pointer: ptr::NonNull<heap::raw::Header>,
    bytes: Len,
}

enum Block {
    Core(heap::raw::Decoded),
    Foreign(Allocation),
}

/// Native initialization and VFS allocations have lifetimes independent of connection arenas.
pub(in crate::runtime) struct Foreign {
    previous: Origin,
    _thread: ThreadBound,
}

impl Foreign {
    pub(in crate::runtime) fn enter() -> Self {
        let previous = CURRENT.replace(Origin::Foreign);
        Self {
            previous,
            _thread: ThreadBound::NEW,
        }
    }
}

impl Drop for Foreign {
    fn drop(&mut self) {
        CURRENT.set(self.previous);
    }
}

impl Origin {
    pub(super) fn allocate(bytes: Len) -> *mut u8 {
        match CURRENT.get() {
            Self::Core => {
                let Some(order) = bytes.order() else {
                    return ptr::null_mut();
                };
                Scope::allocate(order)
            }
            Self::Foreign => Allocation::allocate(bytes),
        }
    }
}

impl Allocation {
    fn allocate(bytes: Len) -> *mut u8 {
        use heap::raw::Header;

        let layout = bytes.layout();
        let pointer = unsafe { alloc::alloc(layout) };
        if pointer.is_null() {
            return pointer;
        }
        unsafe {
            pointer.cast::<Header>().write(Header::foreign(bytes));
            pointer.add(Header::BYTES)
        }
    }

    fn release(self) {
        let layout = self.bytes.layout();
        // SAFETY: this native receipt retains the original Foreign allocation and its layout.
        unsafe { alloc::dealloc(self.pointer.as_ptr().cast(), layout) };
    }

    fn resize(self, bytes: Len) -> *mut u8 {
        use heap::raw::Header;

        let layout = self.bytes.layout();
        let next = bytes.layout();
        // SAFETY: both layouts retain the same alignment and the complete native header.
        let next = unsafe { alloc::realloc(self.pointer.as_ptr().cast(), layout, next.size()) };
        if next.is_null() {
            return next;
        }
        // SAFETY: successful resize preserves the complete Foreign header and its absent owner.
        // Only its initialized length member changes, within the retained header.
        unsafe {
            (*next.cast::<Header>()).metadata.bytes = bytes;
            next.add(Header::BYTES)
        }
    }
}

impl Block {
    /// # Safety
    /// This is these hooks' original live payload, with its initialized header unchanged until release or replacement.
    /// Installed exclusive SQLite custody keeps Core blocks on their original worker with exclusive access and counted backing retention.
    pub(super) unsafe fn read(pointer: ptr::NonNull<u8>) -> Self {
        use heap::raw::{Decoded, Header};

        let pointer = unsafe { pointer.sub(Header::BYTES).cast::<Header>() };
        let header = unsafe { pointer.as_ptr().read() };
        match header.owner {
            Some(owner) => {
                // SAFETY: Core publication initializes this member with the actual arena owner.
                Self::Core(unsafe { Decoded::new(pointer, owner) })
            }
            None => {
                // SAFETY: Foreign publication initializes this member without an arena owner.
                let bytes = unsafe { header.metadata.bytes };
                Self::Foreign(Allocation { pointer, bytes })
            }
        }
    }

    pub(super) fn release(self) {
        use heap::raw::Arena;

        match self {
            Self::Core(header) => Arena::release(header),
            Self::Foreign(block) => block.release(),
        }
    }

    pub(super) fn resize(self, bytes: Len) -> *mut u8 {
        use heap::raw::Arena;

        match self {
            Self::Core(header) => {
                if bytes.into_usize().get() <= heap::raw::payload_bytes(header.order()) {
                    return header.payload();
                }
                let Some(order) = bytes.order() else {
                    return ptr::null_mut();
                };
                Arena::grow(header, order)
            }
            Self::Foreign(block) => block.resize(bytes),
        }
    }

    pub(super) fn size(self) -> std::ffi::c_int {
        use heap::raw::Arena;

        match self {
            Self::Core(header) => Arena::size(header),
            Self::Foreign(block) => block.bytes.0.get() as std::ffi::c_int,
        }
    }
}

/// A positive SQLite allocator request bounded by its signed 32-bit ABI.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Len(NonZero<u32>);

impl Len {
    fn new(bytes: std::ffi::c_int) -> Option<Self> {
        if bytes <= 0 {
            return None;
        }
        // SAFETY: a positive c_int preserves its nonzero value in u32.
        Some(Self(unsafe { NonZero::new_unchecked(bytes as u32) }))
    }

    /// The signed ABI bound plus Header fits u32; the unit floor keeps the order nonnegative.
    fn order(self) -> Option<heap::raw::Order> {
        use heap::raw::{Header, Order, UNIT_BYTES};

        let last = (self.0.get() + Header::BYTES as u32 - 1).max(UNIT_BYTES as u32 - 1);
        Order::new((u32::BITS - last.leading_zeros() - UNIT_BYTES.trailing_zeros()) as usize)
    }

    fn into_usize(self) -> NonZero<usize> {
        const {
            assert!(usize::BITS >= 64);
        }
        NonZero::<usize>::MIN.saturating_add(self.0.get() as usize - 1)
    }

    fn layout(self) -> alloc::Layout {
        let bytes = self.into_usize().get() + heap::raw::Header::BYTES;
        // SAFETY: the signed 32-bit upper bound plus this header fits isize; 8 is a power of two.
        unsafe { alloc::Layout::from_size_align_unchecked(bytes, 8) }
    }
}
