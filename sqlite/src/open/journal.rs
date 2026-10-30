#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

const _: () = assert!(size_of::<Mode>() == 1);
