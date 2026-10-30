mod observer;
pub use observer::Observer;

pub mod own;

pub mod row;

mod seed;
pub use seed::Seed;

pub mod user;

cartel_pg::pg_instance! {
    pub Db:
        user::Lookup,
        user::Write,
        user::Compare,
        search::Pattern,
        search::Order,
        reports::Aggregate,
        reports::Combine,
        row::Post,
        row::Widget,
        row::WidgetSlim,
        row::Score,
        row::Item,
        row::Copy,
        row::Reports,
        row::Doc,
        row::AllTypes,
        row::LtreeNode,
}

pub mod reports;

pub mod search;

pub mod runtime;
