use o3::buffer;
use o3::buffer::resident::copy;

use crate::reply;

/// One PostgreSQL notification retained in one exact domain allocation.
#[repr(C)]
pub struct Notice<'d> {
    text: buffer::raw::Text<'d>,
    cut: u32,
    pid: u32,
}

impl<'d> Notice<'d> {
    pub(crate) fn try_new<'input, R: buffer::Source>(
        fields: reply::raw::Fields<'input, 'd, R>,
        pid: u32,
    ) -> Result<Self, copy::Error> {
        let cut = fields.get::<0>().len() as u32;
        let text = fields.try_compact()?;
        Ok(Self { text, cut, pid })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn channel(&self) -> &str {
        // SAFETY: construction stores the first complete UTF-8 field's length.
        unsafe { self.text.as_str().get_unchecked(..self.cut as usize) }
    }

    pub fn payload(&self) -> &str {
        // SAFETY: construction concatenates the validated fields at this boundary.
        unsafe { self.text.as_str().get_unchecked(self.cut as usize..) }
    }

    pub(crate) fn bytes(&self) -> usize {
        self.text.as_str().len()
    }
}

const _: () = {
    assert!(size_of::<Notice<'static>>() == 2 * size_of::<usize>());
    assert!(align_of::<Notice<'static>>() == align_of::<usize>());
};
