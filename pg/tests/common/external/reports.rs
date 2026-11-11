use crate::common::{external::user::User, types::UserId};
use cartel_gen::query_group;
use cartel_pg::{
    protocol::{Reason, value::decode::RowReader},
    schema::{Row, desc},
};

pub struct Reject;

impl Reject {
    pub const __CARTEL_RESULT_FORMAT_CODES: &'static [u16] = &[0];
}

impl desc::Validate for Reject {
    const FORMAT: u16 = 0;

    fn validate(types: &mut desc::Types<'_>) -> Result<(), cartel_pg::Error> {
        <i64 as desc::Validate>::validate(types)
    }
}

impl Row for Reject {
    type View<'row> = Self;

    fn decode<'row>(_: &mut RowReader<'row>) -> Result<Self, cartel_pg::Error> {
        Err(cartel_pg::Error::Protocol(Reason::ColumnNull { column: 0 }))
    }

    fn decode_owned(reader: &mut RowReader<'_>) -> Result<Self, cartel_pg::Error> {
        Self::decode(reader)
    }
}

pub enum Aggregate {}

#[query_group]
impl Aggregate {
    pub fn count_all() -> i64 {
        User::filter(|_u| true).count()
    }

    pub fn count_active(active: bool) -> i64 {
        User::filter(|u| u.active == active).count()
    }

    pub fn sum_ages() -> Option<i64> {
        User::filter(|_u| true).sum(|u| u.age)
    }

    pub fn avg_score() -> Option<f64> {
        User::filter(|_u| true).avg(|u| u.score)
    }

    pub fn min_age() -> Option<i32> {
        User::filter(|_u| true).min(|u| u.age)
    }

    pub fn max_score() -> Option<f64> {
        User::filter(|_u| true).max(|u| u.score)
    }

    pub fn sum_doubled(min: i32, limit: i64, offset: i64) -> Option<UserId> {
        User::filter(|u| u.age >= min)
            .limit(limit)
            .offset(offset)
            .sum(|u| u.age * 2)
    }

    pub fn reject_id(min: i64) -> Option<Reject> {
        User::filter(|u| u.id >= min).min(|u| u.id)
    }

    pub fn row_numbers() -> Vec<(i64, i64)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| (u.id, Dialect::row_number().over(|w| w.order_by(u.id))))
            .all()
    }

    pub fn lag_lead_ids() -> Vec<(i64, Option<i64>, Option<i64>)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| {
                (
                    u.id,
                    Dialect::lag::<_, i64>(u.id, 1).over(|w| w.order_by(u.id)),
                    Dialect::lead::<_, i64>(u.id, 1).over(|w| w.order_by(u.id)),
                )
            })
            .all()
    }

    pub fn array_int4(ids: &[i32]) -> i64 {
        User::filter(|_u| Dialect::array_length(ids, 1) > 0).count()
    }

    pub fn cardinality(ids: &[i32]) -> i64 {
        User::filter(|_u| Dialect::cardinality(ids) > 0).count()
    }
}

pub enum Combine {}

#[query_group]
impl Combine {
    pub fn union() -> Vec<User> {
        User::filter(|u| u.id <= 1)
            .union(User::filter(|u| u.id >= 4))
            .order_by(|u| u.id)
            .all()
    }

    pub fn union_all() -> Vec<User> {
        User::filter(|u| u.id <= 1)
            .union_all(User::filter(|u| u.id == 1))
            .order_by(|u| u.id)
            .all()
    }

    pub fn intersect() -> Vec<User> {
        User::filter(|u| u.age >= 25)
            .intersect(User::filter(|u| u.active))
            .order_by(|u| u.id)
            .all()
    }

    pub fn overlap_all() -> Vec<User> {
        User::filter(|u| u.age >= 25)
            .intersect_all(User::filter(|u| u.active))
            .order_by(|u| u.id)
            .all()
    }

    pub fn except() -> Vec<User> {
        User::filter(|_u| true)
            .except(User::filter(|u| !u.active))
            .order_by(|u| u.id)
            .all()
    }

    pub fn except_all() -> Vec<User> {
        User::filter(|_u| true)
            .except_all(User::filter(|u| !u.active))
            .order_by(|u| u.id)
            .all()
    }

    pub fn cte_active() -> Vec<User> {
        let active = User::filter(|u| u.active).cte();
        active.filter(|u| u.age >= 25).order_by(|u| u.id).all()
    }

    pub fn cte_union(min_age: i32) -> Vec<User> {
        let young = User::filter(|u| u.age < 30).cte();
        let actives = User::filter(|u| u.active).cte();
        young
            .filter(|u| u.age >= min_age)
            .union(actives.filter(|u| u.age >= min_age))
            .order_by(|u| u.id)
            .all()
    }

    pub fn nested_cte() -> Vec<User> {
        let active = User::filter(|u| u.active).cte();
        let older = active.filter(|u| u.age >= 25).cte();
        older.filter(|_u| true).order_by(|u| u.id).all()
    }
}
