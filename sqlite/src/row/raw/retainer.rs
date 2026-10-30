use crate::runtime;
use std::{marker, mem, ptr, result, slice, str};

pub(crate) struct Retainer<'arena, T: Copy> {
    base: ptr::NonNull<u8>,
    front: usize,
    back: usize,
    items: usize,
    owner: marker::PhantomData<(&'arena mut runtime::raw::Span, T)>,
}

impl<'arena, T: Copy> Retainer<'arena, T> {
    pub(crate) fn new(
        storage: &'arena mut runtime::raw::Span,
        items: usize,
    ) -> result::Result<Self, crate::Error> {
        let required = mem::align_of::<T>();
        let limit = runtime::raw::Span::ALIGNMENT;
        if required > limit {
            return Err(crate::Error::RowLayout {
                required,
                available: limit,
            });
        }
        Ok(Self {
            base: storage.base(),
            front: 0,
            back: storage.len(),
            items,
            owner: marker::PhantomData,
        })
    }

    pub(crate) fn text(&mut self, value: &str) -> result::Result<&'arena str, crate::Error> {
        let bytes = self.bytes(value.as_bytes())?;
        // SAFETY: bytes retains an exact copy of the valid UTF-8 source in this arena.
        Ok(unsafe { str::from_utf8_unchecked(bytes) })
    }

    /// Construction and checked writes preserve front <= back for nonzero rows.
    /// Zero-sized rows use offset zero.
    pub(crate) fn bytes(&mut self, value: &[u8]) -> result::Result<&'arena [u8], crate::Error> {
        let required = value.len();
        let offset = if mem::size_of::<T>() == 0 {
            0
        } else {
            self.front
        };
        let left = self.back - offset;
        if required > left {
            return Err(crate::Error::CollectValueBytes {
                required,
                available: left,
            });
        }
        let start = self.back - required;
        self.back = start;
        let target: &mut [mem::MaybeUninit<u8>] =
            unsafe { slice::from_raw_parts_mut(self.base.as_ptr().add(start).cast(), required) };
        Ok(target.write_copy_of_slice(value))
    }

    /// A checked scan credit admits each push, so the next row count fits usize.
    /// Byte reservations preserve front <= back; row writes fit before advancing it.
    /// Zero-sized rows use offset zero.
    pub(crate) fn push(&mut self, value: T) -> result::Result<(), crate::Error> {
        let size = mem::size_of::<T>();
        let used = self.front.checked_div(size).unwrap_or(self.front);
        let required = used + 1;
        if required > self.items {
            return Err(crate::Error::CollectRows {
                required,
                available: self.items,
            });
        }
        let offset = if size == 0 { 0 } else { self.front };
        let left = self.back - offset;
        if size > left {
            return Err(crate::Error::CollectRowBytes {
                required: size,
                available: left,
            });
        }
        unsafe { self.base.as_ptr().add(offset).cast::<T>().write(value) };
        self.front = if size == 0 {
            required
        } else {
            self.front + size
        };
        Ok(())
    }

    pub(crate) fn finish(self) -> &'arena [T] {
        let size = mem::size_of::<T>();
        let len = self.front.checked_div(size).unwrap_or(self.front);
        unsafe { slice::from_raw_parts(self.base.cast::<T>().as_ptr(), len) }
    }
}

const _: () = assert!(mem::size_of::<Retainer<'static, u64>>() == 4 * mem::size_of::<usize>());
