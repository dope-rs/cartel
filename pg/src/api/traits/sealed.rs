use crate::port::replies;
use cartel_core::reply;

pub trait Sealed {}

impl Sealed for reply::Batch<'_, '_, '_, '_, replies::Row<'_>> {}
