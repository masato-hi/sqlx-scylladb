use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::macros::UserDefinedType;
use sqlx_scylladb::{
    ScyllaDBPool,
    ext::scylla::{DeserializeValue, SerializeValue},
};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, SerializeValue, DeserializeValue, UserDefinedType)]
struct MyUserDefinedType {
    my_bigint: i64,
    my_text: String,
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_user_defined_type(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO user_defined_type_tests(my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(MyUserDefinedType{
        my_bigint: 1,
        my_text: String::from("Hello!")
    })
    .bind(MyUserDefinedTypeVec(
        vec![
            MyUserDefinedType{
                my_bigint: 1,
                my_text: String::from("Hello!")
            },
            MyUserDefinedType{
                my_bigint: 2,
                my_text: String::from("Good morning!")
            },
            MyUserDefinedType{
                my_bigint: 3,
                my_text: String::from("Bye.")
            },
            MyUserDefinedType{
                my_bigint: 4,
                my_text: String::from("Good night.")
            }
        ]
    ))
    .bind(MyUserDefinedTypeVec(
        vec![
            MyUserDefinedType{
                my_bigint: 1,
                my_text: String::from("Hello!")
            },
            MyUserDefinedType{
                my_bigint: 2,
                my_text: String::from("Good morning!")
            },
            MyUserDefinedType{
                my_bigint: 3,
                my_text: String::from("Bye.")
            },
            MyUserDefinedType{
                my_bigint: 1,
                my_text: String::from("Hello!")
            },
        ]
    ))
    .execute(&pool)
    .await?;

    let (my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set): (Uuid, MyUserDefinedType, MyUserDefinedTypeVec, MyUserDefinedTypeVec) =
        sqlx::query_as(
            "SELECT my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set FROM user_defined_type_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);

    // assert my_user_defined_type
    assert_eq!(1, my_user_defined_type.my_bigint);
    assert_eq!("Hello!", my_user_defined_type.my_text);

    // assert my_user_defined_type_list
    assert_eq!(4, my_user_defined_type_list.len());
    assert_eq!(1, (&my_user_defined_type_list)[0].my_bigint);
    assert_eq!("Hello!", (&my_user_defined_type_list)[0].my_text);
    assert_eq!(2, (&my_user_defined_type_list)[1].my_bigint);
    assert_eq!("Good morning!", (&my_user_defined_type_list)[1].my_text);
    assert_eq!(3, (&my_user_defined_type_list)[2].my_bigint);
    assert_eq!("Bye.", (&my_user_defined_type_list)[2].my_text);
    assert_eq!(4, (&my_user_defined_type_list)[3].my_bigint);
    assert_eq!("Good night.", (&my_user_defined_type_list)[3].my_text);

    // assert my_user_defined_type_set
    assert_eq!(3, my_user_defined_type_set.len());
    assert_eq!(1, (&my_user_defined_type_set)[0].my_bigint);
    assert_eq!("Hello!", (&my_user_defined_type_set)[0].my_text);
    assert_eq!(2, (&my_user_defined_type_set)[1].my_bigint);
    assert_eq!("Good morning!", (&my_user_defined_type_set)[1].my_text);
    assert_eq!(3, (&my_user_defined_type_set)[2].my_bigint);
    assert_eq!("Bye.", (&my_user_defined_type_set)[2].my_text);

    #[derive(FromRow)]
    struct UserDefinedTypeTestRow {
        my_id: Uuid,
        my_user_defined_type: MyUserDefinedType,
        #[sqlx(try_from = "MyUserDefinedTypeVec")]
        my_user_defined_type_list: Vec<MyUserDefinedType>,
        #[sqlx(try_from = "MyUserDefinedTypeVec")]
        my_user_defined_type_set: Vec<MyUserDefinedType>,
    }

    let row: UserDefinedTypeTestRow = sqlx::query_as(
        "SELECT my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set FROM user_defined_type_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);

    // assert my_user_defined_type
    assert_eq!(1, row.my_user_defined_type.my_bigint);
    assert_eq!("Hello!", row.my_user_defined_type.my_text);

    // assert my_user_defined_type_list
    assert_eq!(4, row.my_user_defined_type_list.len());
    assert_eq!(1, row.my_user_defined_type_list[0].my_bigint);
    assert_eq!("Hello!", row.my_user_defined_type_list[0].my_text);
    assert_eq!(2, row.my_user_defined_type_list[1].my_bigint);
    assert_eq!("Good morning!", row.my_user_defined_type_list[1].my_text);
    assert_eq!(3, row.my_user_defined_type_list[2].my_bigint);
    assert_eq!("Bye.", row.my_user_defined_type_list[2].my_text);
    assert_eq!(4, row.my_user_defined_type_list[3].my_bigint);
    assert_eq!("Good night.", row.my_user_defined_type_list[3].my_text);

    // assert my_user_defined_type_set
    assert_eq!(3, row.my_user_defined_type_set.len());
    assert_eq!(1, row.my_user_defined_type_set[0].my_bigint);
    assert_eq!("Hello!", row.my_user_defined_type_set[0].my_text);
    assert_eq!(2, row.my_user_defined_type_set[1].my_bigint);
    assert_eq!("Good morning!", row.my_user_defined_type_set[1].my_text);
    assert_eq!(3, row.my_user_defined_type_set[2].my_bigint);
    assert_eq!("Bye.", row.my_user_defined_type_set[2].my_text);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_user_defined_type_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO user_defined_type_tests(my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<MyUserDefinedType>)
    .bind(None::<MyUserDefinedTypeVec>)
    .bind(None::<MyUserDefinedTypeVec>)
    .execute(&pool)
    .await?;

    let (my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set): (
        Uuid,
        Option<MyUserDefinedType>,
        Option<MyUserDefinedTypeVec>,
        Option<MyUserDefinedTypeVec>,
    ) = sqlx::query_as(
        "SELECT my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set FROM user_defined_type_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_user_defined_type.is_none());
    assert!(my_user_defined_type_list.is_none());
    assert!(my_user_defined_type_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO user_defined_type_tests(my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(MyUserDefinedType{
        my_bigint: 1,
        my_text: String::from("Hello!")
    })
    .bind(MyUserDefinedTypeVec(
        vec![
            MyUserDefinedType{
                my_bigint: 1,
                my_text: String::from("Hello!")
            },
            MyUserDefinedType{
                my_bigint: 2,
                my_text: String::from("Good morning!")
            },
            MyUserDefinedType{
                my_bigint: 3,
                my_text: String::from("Bye.")
            },
            MyUserDefinedType{
                my_bigint: 4,
                my_text: String::from("Good night.")
            }
        ]
    ))
    .bind(MyUserDefinedTypeVec(
        vec![
            MyUserDefinedType{
                my_bigint: 1,
                my_text: String::from("Hello!")
            },
            MyUserDefinedType{
                my_bigint: 2,
                my_text: String::from("Good morning!")
            },
            MyUserDefinedType{
                my_bigint: 3,
                my_text: String::from("Bye.")
            },
            MyUserDefinedType{
                my_bigint: 1,
                my_text: String::from("Hello!")
            },
        ]
    ))
    .execute(&pool)
    .await?;

    let (my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set): (
        Uuid,
        Option<MyUserDefinedType>,
        Option<MyUserDefinedTypeVec>,
        Option<MyUserDefinedTypeVec>,
    ) = sqlx::query_as(
        "SELECT my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set FROM user_defined_type_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);

    // assert my_user_defined_type
    let my_user_defined_type = my_user_defined_type.unwrap();
    assert_eq!(1, my_user_defined_type.my_bigint);
    assert_eq!("Hello!", my_user_defined_type.my_text);

    // assert my_user_defined_type_list
    let my_user_defined_type_list = my_user_defined_type_list.unwrap();
    assert_eq!(4, my_user_defined_type_list.len());
    assert_eq!(1, (&my_user_defined_type_list)[0].my_bigint);
    assert_eq!("Hello!", (&my_user_defined_type_list)[0].my_text);
    assert_eq!(2, (&my_user_defined_type_list)[1].my_bigint);
    assert_eq!("Good morning!", (&my_user_defined_type_list)[1].my_text);
    assert_eq!(3, (&my_user_defined_type_list)[2].my_bigint);
    assert_eq!("Bye.", (&my_user_defined_type_list)[2].my_text);
    assert_eq!(4, (&my_user_defined_type_list)[3].my_bigint);
    assert_eq!("Good night.", (&my_user_defined_type_list)[3].my_text);

    // assert my_user_defined_type_set
    let my_user_defined_type_set = my_user_defined_type_set.unwrap();
    assert_eq!(3, my_user_defined_type_set.len());
    assert_eq!(1, (&my_user_defined_type_set)[0].my_bigint);
    assert_eq!("Hello!", (&my_user_defined_type_set)[0].my_text);
    assert_eq!(2, (&my_user_defined_type_set)[1].my_bigint);
    assert_eq!("Good morning!", (&my_user_defined_type_set)[1].my_text);
    assert_eq!(3, (&my_user_defined_type_set)[2].my_bigint);
    assert_eq!("Bye.", (&my_user_defined_type_set)[2].my_text);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_user_defined_type(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_user_defined_type, my_user_defined_type_list, my_user_defined_type_set FROM user_defined_type_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_user_defined_type", describe.columns()[1].name());
    assert_eq!("my_user_defined_type_list", describe.columns()[2].name());
    assert_eq!("my_user_defined_type_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!(
        "my_user_defined_type",
        describe.columns()[1].type_info().name()
    );
    assert_eq!(
        "my_user_defined_type[]",
        describe.columns()[2].type_info().name()
    );
    assert_eq!(
        "my_user_defined_type[]",
        describe.columns()[3].type_info().name()
    );

    Ok(())
}
