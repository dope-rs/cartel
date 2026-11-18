use super::{EXECUTE, Error, SYNC, Writer};
use crate::protocol::{
    messages,
    value::encode::{self as value, WriteArguments as _},
};
use crate::schema::{Admission, TypedQuery};
use o3::buffer::write::Init as _;
use o3::buffer::{
    pool,
    raw::{Initializer, Target},
    write,
    write::consume,
};
use std::{convert::Infallible, mem::MaybeUninit, ptr};

pub(super) struct Prefix<'a> {
    pub(super) name: &'a str,
    pub(super) formats: &'a [u16],
    pub(super) params: i16,
}

impl Prefix<'_> {
    fn visit<E>(&self, mut write: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        write(&[0])?;
        write(self.name.as_bytes())?;
        write(&[0])?;
        formats(self.formats, &mut write)?;
        write(&self.params.to_be_bytes())
    }

    #[inline]
    pub(super) fn write(&self, out: &mut write::Spare<'_>) -> Result<(), Error> {
        let mut target = out.bounded();
        if let Err(error) = target.try_init(self) {
            return self
                .visit(remaining(error.into_parts().1))
                .map_err(Error::capacity);
        }
        target.commit();
        Ok(())
    }
}

fn remaining(mut room: usize) -> impl FnMut(&[u8]) -> Result<(), consume::Error> {
    move |bytes| {
        consume::Error::check(bytes.len(), room)?;
        room -= bytes.len();
        Ok(())
    }
}

pub(super) fn formats<E>(
    formats: &[u16],
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    write(&(formats.len() as i16).to_be_bytes())?;
    for format in formats {
        write(&format.to_be_bytes())?;
    }
    Ok(())
}

// SAFETY: immutable sources retain the exact fragment sum. Callers retain query
// admission or use the unnamed singleton, so the format count fits i16.
unsafe impl Initializer for Prefix<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.name.len() + 6 + self.formats.len() * 2
    }

    #[inline]
    unsafe fn initialize(&self, target: *mut MaybeUninit<u8>) {
        // SAFETY: the admitted prefix is the sum of these disjoint fragments.
        let Ok(()) = self.visit(unsafe { destination(target) });
    }
}

// The caller retains this exclusive destination for the sum of all visited fragments.
unsafe fn destination(
    mut target: *mut MaybeUninit<u8>,
) -> impl FnMut(&[u8]) -> Result<(), Infallible> {
    move |bytes| {
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), target.cast(), bytes.len());
            target = target.add(bytes.len());
        }
        Ok(())
    }
}

impl<Q: TypedQuery> Admission<Q> {
    fn fixed<'p, const CLOSE: bool, E>(
        prefix: &Prefix<'_>,
        length: i32,
        params: impl Iterator<Item = Q::Params<'p>>,
        mut put: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        for params in params {
            put(&[messages::Fe::BIND])?;
            put(&length.to_be_bytes())?;
            prefix.visit(&mut put)?;
            let _ = value::bind::<Q>(&params);
            drop(params);
            formats(Q::RESULT_FORMAT_CODES, &mut put)?;
            put(EXECUTE)?;
        }
        if CLOSE {
            put(&SYNC[..1])?;
            put(&SYNC[1..])?;
        }
        Ok(())
    }

    pub(crate) fn write<const CLOSE: bool, const N: usize>(
        &self,
        span: &mut pool::Span,
        params: [Q::Params<'_>; N],
    ) -> Result<(), Error> {
        let prefix = Prefix {
            name: Q::STATEMENT_NAME,
            formats: Q::PARAM_FORMAT_CODES,
            params: Q::N_PARAMS,
        };
        let results = Q::RESULT_FORMAT_CODES;
        if prefix.params == 0
            && let Ok(length) = i32::try_from(4 + prefix.len() + 2 + 2 * results.len())
        {
            let record = 1 + length as usize + EXECUTE.len();
            let total = record
                .saturating_mul(N)
                .saturating_add(if CLOSE { SYNC.len() } else { 0 });
            // SAFETY: this physical span exclusively owns its unpublished suffix.
            let spare = unsafe { Target::spare(span) };
            let mut params = params.into_iter();
            let Some(frame) = spare.get_mut(..total) else {
                let complete = N.min(spare.len() / record);
                for param in params.by_ref().take(complete) {
                    let _ = value::bind::<Q>(&param);
                }
                let room = spare.len() - complete * record;
                return Self::fixed::<CLOSE, _>(&prefix, length, params, remaining(room))
                    .map_err(Error::capacity);
            };
            // SAFETY: total covers every fragment and the zero-argument projection writes none.
            let put = unsafe { destination(frame.as_mut_ptr()) };
            let Ok(()) = Self::fixed::<CLOSE, _>(&prefix, length, params, put);
            // SAFETY: the cursor initialized the entire admitted request.
            unsafe { Target::complete(span, total) };
            return Ok(());
        }
        Writer::with(span, |out| {
            for params in params {
                out.run(&prefix, results, move |out| {
                    value::bind::<Q>(&params).write::<Q>(out)
                })?;
            }
            if CLOSE {
                out.sync()?;
            }
            Ok(())
        })
    }
}
