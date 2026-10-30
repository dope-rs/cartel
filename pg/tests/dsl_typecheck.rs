use cartel_pg::{
    dsl, dsl::expr::In as _, dsl::expr::Text as _, dsl::query::ops::Arrange as _,
    dsl::query::ops::Change as _, dsl::query::ops::Fetch as _, dsl::query::ops::Project as _,
    dsl::query::ops::Reduce as _, dsl::query::ops::Set as _,
};
use common::types::*;

pub mod common;

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let _: User = User::filter(scope, |user| user.id == 1).one();
        let _: Option<User> = User::filter(scope, |user| user.id == 1).first();
        let _: Vec<User> = User::filter(scope, |user| user.id >= 1).all();
        let _: i64 = User::filter(scope, |user| user.age >= 18).count();
        let _: Option<i32> = User::filter(scope, |user| user.age >= 18).max(|user| user.age);
        let _: Option<f64> = User::filter(scope, |user| user.age >= 18).avg(|user| user.age);

        let _: Vec<User> = User::filter(scope, |user| user.id >= 1)
            .order_desc(|user| user.id)
            .limit(10)
            .offset(5)
            .all();
        let _: Vec<User> = User::filter(scope, |user| user.id >= 1).distinct().all();
        let _: User = User::filter(scope, |user| user.id == 1).for_update().one();

        let pattern = String::from("a%");
        let _: Vec<User> = User::filter(scope, |user| user.name.is_like(scope, &pattern)).all();

        let ids = vec![1_i64, 2, 3];
        let _: Vec<User> = User::filter(scope, |user| user.id.is_in(scope, &ids)).all();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let _: Vec<Post> = Post::join::<User>(scope, |post, user| post.author_id == user.id)
            .filter(|post, user| post.id >= 1 && user.id >= 1)
            .all();
        let _: Vec<(Post, User)> =
            Post::join::<User>(scope, |post, user| post.author_id == user.id)
                .filter(|post, user| post.id >= 1 && user.id >= 1)
                .all();
        let _: Vec<(T, User)> = T::join::<User>(scope, |left, right| left.id == right.id)
            .filter(|left: T, right: User| left.id >= 1 && right.id >= 1)
            .all();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let _: Vec<(i32, i64)> = User::filter(scope, |user| user.age >= 18)
            .group_by(|user| user.age)
            .count();
        let _: Vec<(i32, i64)> = User::filter(scope, |user| user.age >= 18)
            .group_by(|user| user.age)
            .having(|user, group| user.age >= 18 && group.count() > 10)
            .count();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        User::filter(scope, |user| user.id == 1).update(|user| {
            user.name = String::new();
        });
        let _: User = User::filter(scope, |user| user.id == 1)
            .update(|user| {
                user.name = String::new();
            })
            .return_one();
        User::filter(scope, |user| user.id == 1).delete();

        User::insert(scope, |user| {
            user.id = 1;
        });
        User::insert(scope, |user| {
            user.id = 1;
        })
        .ignore();
        User::insert(scope, |user| {
            user.id = 1;
        })
        .on_conflict(|user| user.id)
        .ignore();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let _: Vec<User> = User::filter(scope, |user| user.age < 30)
            .union(User::filter(scope, |user| user.age > 60))
            .all();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let _: Vec<User> = User::filter(scope, |user| {
            scope.has_rows(Post::filter(scope, |post| post.author_id == user.id))
        })
        .all();
        let _: Vec<User> = User::filter(scope, |user| {
            scope.has_no_rows(Post::filter(scope, |post| post.author_id == user.id))
        })
        .all();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let young = User::filter(scope, |user| user.age < 30).cte();
        let older = User::filter(scope, |user| user.age > 60).cte();
        let _: Vec<User> = young.union(older).all();
    };
};

const _: () = {
    let _ = |scope: &dsl::Scope| {
        let _: Vec<i64> = User::filter(scope, |user| user.age >= 18)
            .select(|user| user.id)
            .all();
        let _: Vec<(i64, i64)> = User::filter(scope, |user| user.age >= 18)
            .select(|user| {
                (
                    user.id,
                    scope
                        .row_number()
                        .over(|window| window.partition_by(user.age).order_by(user.id)),
                )
            })
            .all();
    };
};
