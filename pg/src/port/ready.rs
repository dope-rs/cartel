use std::alloc::LayoutError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Part {
    Values,
    Nodes,
    Heads,
    Header,
    Words,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Array {
        part: Part,
        len: usize,
        source: LayoutError,
    },
    Join {
        part: Part,
        source: LayoutError,
    },
}
