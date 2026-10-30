#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Action {
    Open,
    Extended,
    ForeignKeys,
    Defensive,
    TrustedSchema,
    Lookaside,
    Prepare,
    BindNull,
    BindInteger,
    BindReal,
    BindText,
    BindBlob,
    Step,
    ColumnText,
    ColumnBlob,
    Reset,
    Finalize,
}

const _: () = assert!(size_of::<Action>() == 1);
