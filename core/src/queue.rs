use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::credits::FairCredits;
use crate::reply::NodePool;

const NONE: u32 = u32::MAX;

struct QueuePoolState<T> {
    nodes: NodePool<(T, usize)>,
    credits: FairCredits,
}

pub struct QueuePool<T> {
    state: Box<UnsafeCell<QueuePoolState<T>>>,
}

pub struct QueuePoolRef<'d, T> {
    state: NonNull<UnsafeCell<QueuePoolState<T>>>,
    lane: usize,
    lifetime: PhantomData<&'d QueuePool<T>>,
}

impl<T> Clone for QueuePoolRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for QueuePoolRef<'_, T> {}

impl<T> QueuePool<T> {
    fn with_state<R>(&self, operation: impl for<'a> FnOnce(&'a mut QueuePoolState<T>) -> R) -> R {
        // SAFETY: the pool is single-threaded, and `R` cannot borrow the scoped state reference.
        unsafe { operation(&mut *self.state.get()) }
    }

    pub fn with_capacity(capacity: usize, lanes: usize) -> Self {
        Self {
            state: Box::new(UnsafeCell::new(QueuePoolState {
                nodes: NodePool::with_capacity(capacity),
                credits: FairCredits::with_reserve(capacity, lanes, 1),
            })),
        }
    }

    pub fn handle(&self, lane: usize) -> QueuePoolRef<'_, T> {
        assert!(lane < self.with_state(|state| state.credits.lanes()));
        QueuePoolRef {
            state: NonNull::from(self.state.as_ref()),
            lane,
            lifetime: PhantomData,
        }
    }
}

impl<T> QueuePoolRef<'_, T> {
    /// # Safety
    /// The pool must outlive `'a`.
    pub unsafe fn assume_lifetime<'a>(self) -> QueuePoolRef<'a, T> {
        QueuePoolRef {
            state: self.state,
            lane: self.lane,
            lifetime: PhantomData,
        }
    }

    fn with_state<R>(self, operation: impl for<'a> FnOnce(&'a mut QueuePoolState<T>) -> R) -> R {
        // SAFETY: the handle keeps the pool alive; the higher-ranked borrow cannot escape.
        unsafe { operation(&mut *self.state.as_ref().get()) }
    }
}

pub struct BoundedQueue<'d, T> {
    pool: QueuePoolRef<'d, T>,
    head: Cell<u32>,
    tail: Cell<u32>,
    len: Cell<usize>,
    weight: Cell<usize>,
}

impl<'d, T> BoundedQueue<'d, T> {
    pub fn new(pool: QueuePoolRef<'d, T>) -> Self {
        Self {
            pool,
            head: Cell::new(NONE),
            tail: Cell::new(NONE),
            len: Cell::new(0),
            weight: Cell::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    pub fn weight(&self) -> usize {
        self.weight.get()
    }

    pub fn has_capacity(&self) -> bool {
        self.pool.with_state(|state| {
            !state.nodes.is_full() && state.credits.can_acquire(self.pool.lane, 1)
        })
    }

    pub fn try_push(&self, item: T, weight: usize) -> Result<(), T> {
        if !self.has_capacity() {
            return Err(item);
        }
        self.push_reserved(item, weight);
        Ok(())
    }

    pub fn push_reserved(&self, item: T, weight: usize) {
        self.pool.with_state(|state| {
            assert!(
                !state.nodes.is_full() && state.credits.can_acquire(self.pool.lane, 1),
                "queue reservation must be checked before insertion"
            );
            state.credits.acquire_reserved(self.pool.lane, 1);
            let index = state.nodes.insert_reserved((item, weight));
            let tail = self.tail.replace(index);
            if tail == NONE {
                self.head.set(index);
            } else {
                state.nodes.set_next(tail, index);
            }
        });
        self.len.set(self.len.get() + 1);
        self.weight.set(self.weight.get().saturating_add(weight));
    }

    pub fn pop_front(&self) -> Option<T> {
        let (index, _, item, weight) = self.take_front()?;
        self.release(index);
        self.weight.set(self.weight.get().saturating_sub(weight));
        Some(item)
    }

    pub fn drain(&self, mut push: impl FnMut(T) -> Result<(), T>) {
        while let Some((index, next, item, weight)) = self.take_front() {
            match push(item) {
                Ok(()) => {
                    self.release(index);
                    self.weight.set(self.weight.get().saturating_sub(weight));
                }
                Err(item) => {
                    self.restore_front(index, next, item, weight);
                    break;
                }
            }
        }
    }

    pub fn clear(&self) {
        while let Some(item) = self.pop_front() {
            drop(item);
        }
    }

    fn take_front(&self) -> Option<(u32, u32, T, usize)> {
        let index = self.head.get();
        if index == NONE {
            return None;
        }
        let (next, (item, weight)) = self.pool.with_state(|state| state.nodes.take(index));
        self.head.set(next);
        if next == NONE {
            self.tail.set(NONE);
        }
        self.len.set(self.len.get() - 1);
        Some((index, next, item, weight))
    }

    fn restore_front(&self, index: u32, _next: u32, item: T, weight: usize) {
        let head = self.head.replace(index);
        self.pool
            .with_state(|state| state.nodes.restore(index, head, (item, weight)));
        if head == NONE {
            self.tail.set(index);
        }
        self.len.set(self.len.get() + 1);
    }

    fn release(&self, index: u32) {
        self.pool.with_state(|state| {
            state.nodes.release(index);
            state.credits.release(self.pool.lane, 1);
        });
    }
}

impl<T> Drop for BoundedQueue<'_, T> {
    fn drop(&mut self) {
        self.clear();
    }
}
