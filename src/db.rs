use crate::structure;
use sqlx::Pool;
use neo4rs::Graph;

pub async fn check_db_status(pool: &Pool<sqlx::Postgres>) {
    //This function checks if the ITINBUILDER schema exists in the database
    //and creates it if it does not exist.
    let result = sqlx::query(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'ITINBUILDER'
        )",
    )
    .execute(pool)
    .await
    .expect("Failed to check ITINBUILDER schema status");
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
            frequency VARCHAR(7) NOT NULL,
            flight_time INT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .expect("Failed to create flights table");
}

pub async fn import_ssim(pool: &Pool<sqlx::Postgres>, flights: &Vec<structure::FlightInfo>) {
    // This function imports SSIM data into the database, It drop existing data and re-imports it.
    sqlx::query("TRUNCATE TABLE ITINBUILDER.flights")
        .execute(pool)
        .await
        .expect("Failed to truncate flights table");
    for flight in flights {
        sqlx::query(
            "INSERT INTO ITINBUILDER.flights (carrier, flight_id, departure_time, arrival_time, departure_station, arrival_station, frequency, flight_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        )
            .bind(&flight.carrier())
            .bind(&flight.flt_id())
            .bind(&flight.dpt_start_utc())
            .bind(&flight.dpt_end_utc())
            .bind(&flight.dpt_station())
            .bind(&flight.arr_station())
            .bind(&flight.frequency())
            .bind(flight.flight_time())
            .execute(pool).await
            .expect("Failed to insert flight data");
    }
}

pub async fn import_ssim_neo4j(graph:Graph,flights: &Vec<structure::FlightInfo>){
    let mut txn = graph.start_txn().await.unwrap();
}

