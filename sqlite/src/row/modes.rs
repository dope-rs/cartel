use crate::{api::batches, row};

#[doc(hidden)]
pub struct Plain;

#[doc(hidden)]
pub struct Arena;

#[doc(hidden)]
pub struct ArenaState<'arena, T: Copy + 'arena> {
    retainer: row::raw::Retainer<'arena, T>,
    plain: Plain,
}

#[doc(hidden)]
pub struct Input<'query, 'owner: 'query> {
    arena: &'query mut batches::Arena<'owner>,
}

/// A static retention layout selected by one `Decode` implementation.
#[doc(hidden)]
pub trait Kind: Sized {
    type With<K: Kind>: Kind;
    type Init<'query, 'owner: 'query>;
    type State<'arena, T: Copy + 'arena>;

    fn init<'query, 'owner: 'query>(input: Input<'query, 'owner>) -> Self::Init<'query, 'owner>;

    fn start<'query, 'owner: 'query, T: Copy + 'query>(
        init: Self::Init<'query, 'owner>,
        rows: usize,
    ) -> Result<Self::State<'query, T>, crate::Error>;

    fn from_arena<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut ArenaState<'arena, T>,
    ) -> &'borrow mut Self::State<'arena, T>;
}

#[doc(hidden)]
pub trait Contains<K: Kind>: Kind {
    fn get<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut Self::State<'arena, T>,
    ) -> &'borrow mut K::State<'arena, T>;
}

impl<'query, 'owner: 'query> Input<'query, 'owner> {
    pub(crate) const fn new(arena: &'query mut batches::Arena<'owner>) -> Self {
        Self { arena }
    }
}

impl Kind for Plain {
    type With<K: Kind> = K;
    type Init<'query, 'owner: 'query> = Self;
    type State<'arena, T: Copy + 'arena> = Self;

    fn init<'query, 'owner: 'query>(_input: Input<'query, 'owner>) -> Self::Init<'query, 'owner> {
        Self
    }

    fn start<'query, 'owner: 'query, T: Copy + 'query>(
        init: Self::Init<'query, 'owner>,
        _rows: usize,
    ) -> Result<Self::State<'query, T>, crate::Error> {
        Ok(init)
    }

    fn from_arena<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut ArenaState<'arena, T>,
    ) -> &'borrow mut Self::State<'arena, T> {
        &mut state.plain
    }
}

impl Kind for Arena {
    type With<K: Kind> = Self;
    type Init<'query, 'owner: 'query> = Input<'query, 'owner>;
    type State<'arena, T: Copy + 'arena> = ArenaState<'arena, T>;

    fn init<'query, 'owner: 'query>(input: Input<'query, 'owner>) -> Self::Init<'query, 'owner> {
        input
    }

    fn start<'query, 'owner: 'query, T: Copy + 'query>(
        init: Self::Init<'query, 'owner>,
        rows: usize,
    ) -> Result<Self::State<'query, T>, crate::Error> {
        Ok(ArenaState {
            retainer: init.arena.retain(rows)?,
            plain: Plain,
        })
    }

    fn from_arena<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut ArenaState<'arena, T>,
    ) -> &'borrow mut Self::State<'arena, T> {
        state
    }
}

impl Contains<Plain> for Plain {
    fn get<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut Self::State<'arena, T>,
    ) -> &'borrow mut <Plain as Kind>::State<'arena, T> {
        state
    }
}

impl Contains<Plain> for Arena {
    fn get<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut Self::State<'arena, T>,
    ) -> &'borrow mut <Plain as Kind>::State<'arena, T> {
        &mut state.plain
    }
}

impl Contains<Arena> for Arena {
    fn get<'borrow, 'arena, T: Copy + 'arena>(
        state: &'borrow mut Self::State<'arena, T>,
    ) -> &'borrow mut <Arena as Kind>::State<'arena, T> {
        state
    }
}

impl<'arena, T: Copy> ArenaState<'arena, T> {
    pub(crate) fn retainer(&mut self) -> &mut row::raw::Retainer<'arena, T> {
        &mut self.retainer
    }

    pub(crate) fn push(&mut self, value: T) -> Result<(), crate::Error> {
        self.retainer.push(value)
    }

    pub(crate) fn finish(self) -> &'arena [T] {
        self.retainer.finish()
    }
}
