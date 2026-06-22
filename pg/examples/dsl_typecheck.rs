use cartel_pg::dsl::*;
use cartel_pg::{PgTable, Uuid};

#[derive(PgTable)]
#[allow(dead_code)]
struct User {
    #[pk]
    id: i64,
    name: String,
    age: i32,
}

#[derive(PgTable)]
#[allow(dead_code)]
struct Post {
    #[pk]
    id: i64,
    author_id: i64,
    title: String,
}

#[derive(PgTable)]
#[allow(dead_code)]
struct Doc {
    #[pk]
    id: i64,
    payload: Vec<u8>,
}

#[allow(dead_code, unreachable_code, clippy::let_unit_value)]
fn typecheck_basic() {
    let _: User = User::filter(|u| u.id == 1).one();
    let _: Option<User> = User::filter(|u| u.id == 1).first();
    let _: Vec<User> = User::filter(|u| u.id >= 1).all();
    let _: i64 = User::filter(|u| u.age >= 18).count();
    let _: Option<i32> = User::filter(|u| u.age >= 18).max(|u| u.age);
    let _: Option<f64> = User::filter(|u| u.age >= 18).avg(|u| u.age);

    let _: Vec<User> = User::filter(|u| u.id >= 1)
        .order_by_desc(|u| u.id)
        .limit(10)
        .offset(5)
        .all();
    let _: Vec<User> = User::filter(|u| u.id >= 1).distinct().all();
    let _: User = User::filter(|u| u.id == 1).for_update().one();

    let pat = String::from("a%");
    let _: Vec<User> = User::filter(|u| u.name.like(&pat)).all();

    let ids = vec![1i64, 2, 3];
    let _: Vec<User> = User::filter(|u| u.id.in_(&ids)).all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_join() {
    let _: Vec<Post> = Post::join::<User>(|p, u| p.author_id == u.id)
        .filter(|_p, u| u.id >= 1)
        .all();
    let _: Vec<(Post, User)> = Post::join::<User>(|p, u| p.author_id == u.id)
        .filter(|p, u| p.id >= 1 && u.id >= 1)
        .all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_aggregate() {
    let _: Vec<(i32, i64)> = User::filter(|u| u.age >= 18).group_by(|u| u.age).count();
    let _: Vec<(i32, i64)> = User::filter(|u| u.age >= 18)
        .group_by(|u| u.age)
        .having(|_u, agg| agg.count() > 10)
        .count();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_mutations() {
    User::filter(|u| u.id == 1).update(|u| {
        u.name = String::new();
    });
    let _: User = User::filter(|u| u.id == 1)
        .update(|u| {
            u.name = String::new();
        })
        .returning_one();
    User::filter(|u| u.id == 1).delete();

    User::insert(|u| {
        u.id = 1;
    });
    User::insert(|u| {
        u.id = 1;
    })
    .on_conflict_do_nothing();
    User::insert(|u| {
        u.id = 1;
    })
    .on_conflict(|u| u.id)
    .do_nothing();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_setops() {
    let _: Vec<User> = User::filter(|u| u.age < 30)
        .union(User::filter(|u| u.age > 60))
        .all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_subquery() {
    let _: Vec<User> = User::filter(|u| exists(Post::filter(|p| p.author_id == u.id))).all();
    let _: Vec<User> = User::filter(|u| not_exists(Post::filter(|p| p.author_id == u.id))).all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_cte() {
    let young = User::filter(|u| u.age < 30).cte();
    let old = User::filter(|u| u.age > 60).cte();
    let _: Vec<User> = young.union(old).all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_window() {
    let _: Vec<i64> = User::filter(|u| u.age >= 18).select(|u| u.id).all();
    let _: Vec<(i64, i64)> = User::filter(|u| u.age >= 18)
        .select(|u| {
            (
                u.id,
                row_number().over(|w| w.partition_by(u.age).order_by(u.id)),
            )
        })
        .all();
}

fn main() {
    let _ = Uuid::NIL;
}
