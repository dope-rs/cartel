cartel_sqlite::catalog! {
    pub Commands;




    #[derive(cartel_sqlite::Table)]
    #[table_name("items")]
    pub struct Item {
        id: i64,
        r#select: i64,
    }

    #[query]
    pub fn insert(id: i64, value: i64) {
        Item::insert(|item| {
            item.id = id;
            item.r#select = value;
        })
    }

    #[query]
    pub fn update(id: i64, value: i64) {
        Item::filter(|item| item.id == id).update(|item| item.r#select = value)
    }

    #[query]
    pub fn delete(id: i64) {
        Item::filter(|item| item.id == id).delete()
    }

    #[query]
    pub fn retain(id: i64, value: i64) -> Item {
        Item::insert(|item| {
            item.id = id;
            item.r#select = value;
        }).returning_one()
    }
    #[query]
    pub fn legacy(__cartel_row: i64) -> Vec<(i64, i64)> {
        Item::filter(|row| row.id >= __cartel_row)
            .rows()
            .order_by(|row| row.id)
            .select(|row| (row.id + __cartel_row, row.r#select))
            .all()
    }

    #[query]
    pub fn shorthand(__cartel_row: i64) -> Vec<(i64, i64)> {
        Item::filter(|row| row.id >= __cartel_row)
            .rows()
            .order_by(Item::id)
            .select((Item::id + __cartel_row, Item::r#select))
            .all()
    }
}
