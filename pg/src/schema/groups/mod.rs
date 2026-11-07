use std::{any::TypeId, marker::PhantomData};

use crate::schema::{self, desc};

mod proof;
pub(super) use proof::Proof;

#[doc(hidden)]
pub struct End;

#[doc(hidden)]
pub struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

#[doc(hidden)]
pub struct Empty;

#[doc(hidden)]
pub struct Item<Head, Tail>(PhantomData<(Head, Tail)>);

impl Proof for End {}
impl Proof for Empty {}

impl<Head, Tail> Proof for Item<Head, Tail> {}
impl<Head, Tail> Proof for Cons<Head, Tail> {}

#[doc(hidden)]
pub trait List: Proof {
    fn matches(groups: &[&[schema::QueryMeta<'_>]]) -> bool;

    fn contains<Q: schema::TypedQuery>() -> bool;

    fn validate(group: u32, query: u32, input: desc::Input<'_>) -> Result<(), crate::Error>;
}

#[doc(hidden)]
pub trait Items: Proof {
    fn matches(queries: &[schema::QueryMeta<'_>]) -> bool;

    fn contains<Q: schema::TypedQuery>() -> bool;

    fn validate(query: u32, input: desc::Input<'_>) -> Result<(), crate::Error>;
}

impl List for End {
    fn matches(groups: &[&[schema::QueryMeta<'_>]]) -> bool {
        matches!(groups, [])
    }

    fn contains<Q: schema::TypedQuery>() -> bool {
        false
    }

    fn validate(_group: u32, _query: u32, _input: desc::Input<'_>) -> Result<(), crate::Error> {
        Err(crate::Error::QueryMetadata)
    }
}

impl Items for Empty {
    fn matches(queries: &[schema::QueryMeta<'_>]) -> bool {
        matches!(queries, [])
    }

    fn contains<Q: schema::TypedQuery>() -> bool {
        false
    }

    fn validate(_query: u32, _input: desc::Input<'_>) -> Result<(), crate::Error> {
        Err(crate::Error::QueryMetadata)
    }
}

impl<Head, Tail> Items for Item<Head, Tail>
where
    Head: schema::TypedQuery,
    Tail: Items,
{
    fn matches(queries: &[schema::QueryMeta<'_>]) -> bool {
        let Some((head, tail)) = queries.split_first() else {
            return false;
        };
        head.matches::<Head>() && Tail::matches(tail)
    }

    fn contains<Q: schema::TypedQuery>() -> bool {
        TypeId::of::<Head>() == TypeId::of::<Q>() || Tail::contains::<Q>()
    }

    fn validate(query: u32, input: desc::Input<'_>) -> Result<(), crate::Error> {
        if query == 0 {
            desc::Types::query::<Head>(input.payload, input.registry)
        } else {
            Tail::validate(query - 1, input)
        }
    }
}

impl<Head, Tail> List for Cons<Head, Tail>
where
    Head: schema::QueryGroup,
    Tail: List,
{
    fn matches(groups: &[&[schema::QueryMeta<'_>]]) -> bool {
        let Some((head, tail)) = groups.split_first() else {
            return false;
        };
        *head == Head::QUERIES && Head::Items::matches(head) && Tail::matches(tail)
    }

    fn contains<Q: schema::TypedQuery>() -> bool {
        Head::Items::contains::<Q>() || Tail::contains::<Q>()
    }

    fn validate(group: u32, query: u32, input: desc::Input<'_>) -> Result<(), crate::Error> {
        if group == 0 {
            Head::Items::validate(query, input)
        } else {
            Tail::validate(group - 1, query, input)
        }
    }
}
