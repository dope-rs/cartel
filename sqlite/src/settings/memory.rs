use crate::{
    api::stmt,
    catalogs, config, conn,
    runtime::{Error, heap::layout::Layout, local, raw},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Memory {
    pub sqlite_bytes: usize,
    pub results: Results,
    pub statement_bytes: usize,
    pub cache: Cache,
    pub lookaside: Lookaside,
}

impl Memory {
    /// Computes the exact worker backing reservation for a catalog.
    pub fn reservation<I: crate::Catalog>(self) -> Option<Reservation> {
        self.admit::<I>().ok().map(|(plan, _)| plan)
    }

    pub(crate) fn admit<I: crate::Catalog>(self) -> Result<(Reservation, Layout), Error> {
        let sqlite = Layout::new(self.sqlite_bytes).ok_or(Error::SqliteBytes)?;
        let shape = catalogs::Shape::of::<I>();
        let stmts = Statements::new(shape).ok_or(Error::CapacityOverflow)?;
        let results = round(self.results.bytes).ok_or(Error::CapacityOverflow)?;
        let aside = round(self.lookaside.bytes()).ok_or(Error::CapacityOverflow)?;
        let phase = conn::raw::Vacant::<'_, '_, I>::LAYOUT;
        if phase.align() > raw::Span::ALIGNMENT {
            return Err(Error::Layout);
        }
        let phase = round(phase.size()).ok_or(Error::CapacityOverflow)?;
        let arena = sqlite.backing().ok_or(Error::CapacityOverflow)?;
        let retain = local::State::<I>::RETENTION_BYTES
            .checked_add(phase)
            .and_then(|bytes| bytes.checked_add(arena))
            .ok_or(Error::CapacityOverflow)?;
        let backing = retain
            .checked_add(results)
            .and_then(|bytes| bytes.checked_add(stmts.slots_bytes))
            .and_then(|bytes| bytes.checked_add(aside))
            .ok_or(Error::CapacityOverflow)?;
        let targets =
            Targets::new(self.cache, self.statement_bytes).ok_or(Error::CapacityOverflow)?;
        Ok((
            Reservation {
                backing_bytes: backing,
                control_bytes: retain - self.sqlite_bytes,
                sqlite_bytes: self.sqlite_bytes,
                sqlite_capacity: sqlite.capacity(),
                result_bytes: results,
                statements: stmts,
                lookaside_bytes: aside,
                targets,
            },
            sqlite,
        ))
    }
}

fn round(bytes: usize) -> Option<usize> {
    Some(bytes.checked_add(63)? / 64 * 64)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Results {
    pub rows: usize,
    pub bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Cache {
    pub main_bytes: usize,
    pub temporary_bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Lookaside {
    slot_bytes: u16,
    slots: u16,
}

impl Lookaside {
    pub const DISABLED: Self = Self {
        slot_bytes: 0,
        slots: 0,
    };

    pub const fn try_new(bytes: u16, slots: u16) -> Option<Self> {
        if bytes as usize <= size_of::<usize>()
            || !bytes.is_multiple_of(8)
            || slots == 0
            || bytes as u32 * slots as u32 > 0x7fff0000
        {
            return None;
        }
        Some(Self {
            slot_bytes: bytes,
            slots,
        })
    }

    pub const fn slot_bytes(self) -> usize {
        self.slot_bytes as usize
    }

    pub const fn slots(self) -> usize {
        self.slots as usize
    }

    pub const fn bytes(self) -> usize {
        const { assert!(usize::BITS >= 2 * u16::BITS) };
        self.slot_bytes() * self.slots()
    }

    pub(crate) const fn sqlite_parts(self) -> (i32, i32) {
        (self.slot_bytes as i32, self.slots as i32)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Reservation {
    /// Total bytes in the worker's single aligned backing allocation.
    pub backing_bytes: usize,
    /// Connection state, intrusive ownership and allocator control in the same allocation.
    pub control_bytes: usize,
    /// Physical partition reserved for SQLite's allocator.
    pub sqlite_bytes: usize,
    /// Usable SQLite allocation capacity after allocator metadata.
    pub sqlite_capacity: usize,
    /// Aligned retained-result arena bytes.
    pub result_bytes: usize,
    /// Validated prepared-statement layout.
    pub statements: Statements,
    /// Aligned per-connection lookaside bytes.
    pub lookaside_bytes: usize,
    /// Logical targets charged to SQLite's allocator partition.
    pub targets: Targets,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Statements {
    /// Validated compile-time catalog shape.
    pub catalog: crate::Shape,
    /// Prepared catalog and internal control statement slots.
    pub items: usize,
    /// Maximum SQL bytes across catalog and internal control statements.
    pub sql_bytes: usize,
    /// Aligned prepared-statement registry bytes.
    pub slots_bytes: usize,
}

impl Statements {
    fn new(catalog: crate::Shape) -> Option<Self> {
        let items = catalog
            .statements
            .checked_add(config::CONTROL_STATEMENT_ITEMS)?;
        let sql = catalog.max_sql_bytes.max(stmt::Control::MAX_SQL_BYTES);
        let bytes = stmt::bytes(items)?;
        Some(Self {
            catalog,
            items,
            sql_bytes: sql,
            slots_bytes: bytes,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Targets {
    /// SQLite-reported prepared-program bytes.
    pub statements: usize,
    /// Main plus temporary page-cache bytes.
    pub pager: usize,
    /// Statements plus page-cache bytes.
    pub retained: usize,
}

impl Targets {
    fn new(cache: Cache, stmts: usize) -> Option<Self> {
        let pager = cache.main_bytes.checked_add(cache.temporary_bytes)?;
        let retained = pager.checked_add(stmts)?;
        Some(Self {
            statements: stmts,
            pager,
            retained,
        })
    }
}
