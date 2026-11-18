use crate::{common::schema::User, common::schema::Widget};

#[cartel_sqlite::query]
pub fn count_widgets_arith(min: i64) -> Option<i64> {
    Widget::filter(|w| w.value >= min).sum(|w| w.value * 2)
}

#[cartel_sqlite::query]
pub fn user_row_numbers() -> Vec<(i64, i64)> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .select(|u| (u.id, Dialect::row_number().over(|w| w.order_by(u.age))))
        .all()
}

#[cartel_sqlite::query]
pub fn widget_positions(sign: i64, group: i32) -> Vec<i64> {
    Widget::filter(|_w| true)
        .order_by(|w| w.id)
        .select(|w| {
            Dialect::row_number()
                .over(|v| v.order_by(w.value * sign).partition_by(w.bucket * group))
        })
        .all()
}
