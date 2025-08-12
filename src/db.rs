use sqlx::Pool;

pub async fn check_db_status(pool: &Pool<sqlx::Postgres>) {
    //This function checks if the ITINBUILDER schema exists in the database
    //and creates it if it does not exist.
    let result = sqlx::query(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'ITINBUILDER'
        )"
    ).execute(pool).await.expect("Failed to check ITINBUILDER schema status");
    if result.rows_affected() == 0 {
        sqlx::query("CREATE SCHEMA ITINBUILDER")
            .execute(pool)
            .await
            .expect("Failed to create ITINBUILDER schema");
        println!("ITINBUILDER schema created");
    } else {
        println!("ITINBUILDER schema already exists");
    }
}