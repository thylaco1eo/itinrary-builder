mod engine;

use crate::structure::{self, Airport};
use sqlx::Pool;
use surrealdb::{Surreal, engine::any};

pub async fn check_db_status(pool: &Surreal<any::Any>) {
    //This function checks if the ITINBUILDER schema exists in the database
    //and creates it if it does not exist.

}

pub async fn init_table(pool: &Surreal<any::Any>) {
    // This function initializes the necessary tables in the ITINBUILDER schema.
    // You can add your table creation logic here.
    // For example:

}

pub async fn import_ssim(pool: &Surreal<any::Any>, flights: &Vec<structure::FlightInfo>) {
    // This function imports SSIM data into the database, It drop existing data and re-imports it.
    // sqlx::query("TRUNCATE TABLE ITINBUILDER.flights")
    //     .execute(pool)
    //     .await
    //     .expect("Failed to truncate flights table");
    // for flight in flights {
    //     sqlx::query(
    //         "INSERT INTO ITINBUILDER.flights (carrier, flight_id, departure_time, arrival_time, departure_station, arrival_station, frequency, flight_time)
    //         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
    //     )
    //         .bind(&flight.carrier())
    //         .bind(&flight.flt_id())
    //         .bind(&flight.dpt_start_utc())
    //         .bind(&flight.dpt_end_utc())
    //         .bind(&flight.dpt_station())
    //         .bind(&flight.arr_station())
    //         .bind(&flight.frequency())
    //         .bind(flight.flight_time())
    //         .execute(pool).await
    //         .expect("Failed to insert flight data");
    // }
}

pub async fn create_airport(pool: &Surreal<any::Any>, airport: Airport) -> Result<bool, surrealdb::Error> {
    // This function creates a new airport in the database.
    // You can add your airport creation logic here.
    // For example:
    Ok(true)
}