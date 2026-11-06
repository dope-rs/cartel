pub(crate) enum Disposition {
    Reusable,
    Lost,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::protocol) enum Status {
    Idle,
    Transaction,
}

pub(in crate::protocol) enum Readiness {
    Ready,
    Pending,
}

const _: () = assert!(std::mem::size_of::<Status>() == std::mem::size_of::<bool>());
const _: () = assert!(std::mem::align_of::<Status>() == std::mem::align_of::<bool>());
