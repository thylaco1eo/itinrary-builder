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

pub async fn init_table(pool: &Pool<sqlx::Postgres>) {
    // This function initializes the necessary tables in the ITINBUILDER schema.
    // You can add your table creation logic here.
    // For example:
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ITINBUILDER.flights (
            id SERIAL PRIMARY KEY,
            carrier VARCHAR(2) NOT NULL,
            flight_id VARCHAR(10) NOT NULL,
            departure_time TIMESTAMP WITH TIME ZONE NOT NULL,
            arrival_time TIMESTAMP WITH TIME ZONE NOT NULL,
            departure_station VARCHAR(3) NOT NULL,
            arrival_station VARCHAR(3) NOT NULL,
            frequency INT[] NOT NULL,
            flight_time INT NOT NULL
        )"
    )
    .execute(pool)
    .await
    .expect("Failed to create flights table");
}

pub async fn import_ssim(pool: &Pool<sqlx::Postgres>) {
    // This function imports SSIM data into the database, It drop existing data and re-imports it.
    sqlx::query("TRUNCATE TABLE ITINBUILDER.flights")
        .execute(pool)
        .await
        .expect("Failed to truncate flights table");
}
