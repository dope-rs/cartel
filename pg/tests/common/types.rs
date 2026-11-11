use cartel_pg::{PgRow, PgTable};

#[derive(PgRow, PgTable)]
pub struct User {
    pub id: i64,
    pub name: String,
    pub age: i32,
}

#[derive(PgRow, PgTable)]
pub struct Post {
    pub id: i64,
    pub author_id: i64,
}

#[derive(PgRow, PgTable)]
#[table_name("users")]
pub struct UserId {
    pub id: i64,
}

#[derive(PgTable)]
#[table(batch)]
#[table_name("한\"글.select")]
pub struct BatchKeyword {
    pub r#select: i64,
}

type UserAlias = User;

#[derive(PgRow, PgTable)]
pub struct Formats {
    pub id: i64,
    pub path: cartel_pg::Ltree,
    pub optional_path: Option<cartel_pg::Ltree>,
    pub timestamp: cartel_pg::Timestamp,
    pub date: cartel_pg::Date,
    pub uuid: cartel_pg::Uuid,
    pub text: cartel_pg::Text,
    pub json: cartel_pg::Jsonb,
}

pub enum Projection {}

#[cartel_gen::query_group]
impl Projection {
    pub fn insert_batch(values: &[i64]) {
        BatchKeyword::insert_each(|row| row.r#select = values)
    }

    pub fn rename_owned(id: i64, name: String) {
        User::filter(User::id == id).update(User::name = name)
    }

    pub fn rename_ref(id: i64, name: &str) {
        User::filter(|scope| scope.id == id).update(|scope| scope.name = name)
    }

    pub fn mixed_rows() -> Vec<(User, Formats)> {
        User::join::<Formats>(|user, formats| user.id == formats.id)
            .filter(|_user, _formats| true)
            .all()
    }

    pub fn mixed_fields() -> Vec<(
        i64,
        cartel_pg::Ltree,
        Option<cartel_pg::Ltree>,
        cartel_pg::Timestamp,
    )> {
        Formats::filter(|_row| true)
            .select(|row| (row.id, row.path, row.optional_path, row.timestamp))
            .all()
    }

    pub fn binary() -> Vec<(
        cartel_pg::Date,
        cartel_pg::Uuid,
        cartel_pg::Text,
        cartel_pg::Jsonb,
    )> {
        Formats::filter(|_row| true)
            .select(|row| (row.date, row.uuid, row.text, row.json))
            .all()
    }

    pub fn scalar_path() -> Vec<cartel_pg::Ltree> {
        Formats::filter(|_row| true).select(|row| row.path).all()
    }

    pub fn maybe_path() -> Vec<Option<cartel_pg::Ltree>> {
        Formats::filter(|_row| true)
            .select(|row| row.optional_path)
            .all()
    }

    pub fn timestamp() -> Vec<cartel_pg::Timestamp> {
        Formats::filter(|_row| true)
            .select(|row| row.timestamp)
            .all()
    }

    pub fn aliased() -> Vec<UserAlias> {
        User::join::<Post>(|user, post| user.id == post.author_id)
            .filter(|_user, _post| true)
            .all()
    }

    pub fn qualified() -> Vec<(self::Post, UserAlias)> {
        User::join::<Post>(|user, post| user.id == post.author_id)
            .filter(|_user, _post| true)
            .all()
    }

    pub fn same_table() -> Vec<(User, UserId)> {
        User::join::<UserId>(|user, id| user.id == id.id)
            .filter(|_user, _id| true)
            .all()
    }

    pub fn self_join() -> Vec<(User, User)> {
        User::join::<User>(|left, right| left.id < right.id)
            .filter(|_left, _right| true)
            .all()
    }

    pub fn subset() -> Vec<User> {
        User::join::<Post>(|user, post| user.id == post.author_id)
            .filter(|_user, _post| true)
            .all()
    }

    pub fn reordered() -> Vec<(Post, User)> {
        User::join::<Post>(|user, post| user.id == post.author_id)
            .filter(|_user, _post| true)
            .all()
    }

    pub fn nested() -> Vec<User> {
        User::join::<Post>(|user, post| user.id == post.author_id)
            .filter(|user, _post| {
                Dialect::exists(User::filter(|inner| {
                    inner.id > user.id && Dialect::exists(User::filter(|deep| deep.id > inner.id))
                }))
            })
            .all()
    }

    pub fn lateral() -> Vec<(User, User)> {
        User::lateral_join::<User>(|outer| User::filter(|inner| inner.id > outer.id).for_share())
            .filter(|_outer, _inner| true)
            .all()
    }

    pub fn lateral_distinct(shift: i32) -> Vec<(User, User)> {
        User::lateral_join::<User>(|outer| {
            User::filter(|inner| inner.id > outer.id)
                .distinct_on(|inner| inner.age + outer.age + shift)
                .order_by(|inner| inner.age + outer.age + shift)
        })
        .filter(|_outer, _inner| true)
        .all()
    }

    pub fn legacy(r#row10: i64) -> Vec<(i64, String)> {
        User::filter(|r#row| row10 <= row.id)
            .order_by(|row| row.id)
            .select(|row| (row.id + r#row10, row.name))
            .all()
    }

    pub fn shorthand(row10: i64) -> Vec<(i64, String)> {
        User::filter(row10 <= User::id)
            .order_by(User::id)
            .select((User::id + row10, User::name))
            .all()
    }

    pub fn group_legacy(id: i64) -> Vec<(i32, Option<i64>)> {
        User::filter(|row| row.id >= id)
            .group_by(|row| row.age)
            .order_by(|row| row.age)
            .max(|row| row.id + id)
    }

    pub fn group_short(id: i64) -> Vec<(i32, Option<i64>)> {
        User::filter(User::id >= id)
            .group_by(User::age)
            .order_by(User::age)
            .max(User::id + id)
    }

    pub fn cte_legacy(id: i64) -> Vec<User> {
        let selected = User::filter(|row| row.id >= id).for_update().cte();
        selected.filter(|row| row.age > 0).all()
    }

    pub fn cte_short(r#id: i64) -> Vec<User> {
        let r#selected = User::filter(User::id >= id).for_update().cte();
        selected.filter(|r#row| row.age > 0).all()
    }

    pub fn cte_distinct(id: i64) -> Vec<User> {
        let selected = User::filter(|row| row.id >= id).distinct().cte();
        selected.all()
    }

    pub fn mixed_set() -> Vec<User> {
        User::filter(|row| row.id == 1)
            .union(User::filter(|row| row.id == 2))
            .intersect(User::filter(|row| row.id == 2))
            .except(User::filter(|row| row.id == 4))
            .order_by(|row| row.id)
            .all()
    }
}
