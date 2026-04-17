use std::time::{Duration, Instant};

use crate::domain::route::Route;
use crate::Infrastructure::db::model::flight_row::{FlightCacheRow, FlightRow};
use actix_web::rt::time::timeout;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

const PRODUCTION_FLIGHT_TABLE: &str = "flight";
const PRODUCTION_ROUTE_TABLE: &str = "route";
const TEMP_FLIGHT_TABLE: &str = "flight_tmp";
const TEMP_ROUTE_TABLE: &str = "route_tmp";
const FLIGHT_PRELOAD_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_FLIGHT_BATCH_SIZE: usize = 2_000;
const MIN_FLIGHT_BATCH_SIZE: usize = 250;
const INITIAL_FLIGHT_IMPORT_CHUNK_SIZE: usize = 2_000;
const MIN_FLIGHT_IMPORT_CHUNK_SIZE: usize = 125;
const ROUTE_IMPORT_CHUNK_SIZE: usize = 2_000;

pub fn temp_flight_table() -> &'static str {
    TEMP_FLIGHT_TABLE
}

pub fn temp_route_table() -> &'static str {
    TEMP_ROUTE_TABLE
}

pub async fn load_schedule_tmp(
    db: &Surreal<Any>,
    flight_rows: &[FlightRow],
    route_rows: &[Route],
) -> surrealdb::Result<()> {
    clear_schedule_tables(db, TEMP_FLIGHT_TABLE, TEMP_ROUTE_TABLE).await?;
    if !flight_rows.is_empty() {
        insert_flight_rows(db, TEMP_FLIGHT_TABLE, flight_rows).await?;
    }
    if !route_rows.is_empty() {
        insert_route_rows(db, TEMP_ROUTE_TABLE, route_rows).await?;
    }
    Ok(())
}

pub async fn promote_tmp_to_production(db: &Surreal<Any>) -> surrealdb::Result<()> {
    let switch_started = Instant::now();
    let transaction = db.clone().begin().await?;
    let promote_sql = format!(
        "DELETE {prod_flight};\
DELETE {prod_route};\
INSERT INTO {prod_flight} (SELECT * FROM {tmp_flight});\
INSERT RELATION INTO {prod_route} (SELECT * FROM {tmp_route});",
        prod_flight = PRODUCTION_FLIGHT_TABLE,
        prod_route = PRODUCTION_ROUTE_TABLE,
        tmp_flight = TEMP_FLIGHT_TABLE,
        tmp_route = TEMP_ROUTE_TABLE,
    );

    let response = match transaction.query(promote_sql).await {
        Ok(response) => response,
        Err(error) => {
            let _ = transaction.cancel().await;
            return Err(error);
        }
    };

    if let Err(error) = response.check() {
        let _ = transaction.cancel().await;
        return Err(error.into());
    }

    transaction.commit().await?;
    println!(
        "Production schedule switch completed in {:?}.",
        switch_started.elapsed()
    );
    Ok(())
}

async fn insert_flight_rows(
    db: &Surreal<Any>,
    flight_table: &str,
    flight_rows: &[FlightRow],
) -> surrealdb::Result<()> {
    let started = Instant::now();
    let mut inserted = 0usize;
    let mut chunk_size = INITIAL_FLIGHT_IMPORT_CHUNK_SIZE.min(flight_rows.len());

    while inserted < flight_rows.len() {
        let end = (inserted + chunk_size).min(flight_rows.len());
        let chunk = &flight_rows[inserted..end];
        let insert_result: surrealdb::Result<Vec<FlightRow>> =
            db.insert(flight_table).content(chunk.to_vec()).await;

        match insert_result {
            Ok(_) => {
                inserted = end;
                if inserted % 10_000 == 0 || inserted == flight_rows.len() {
                    println!(
                        "Flight import progress for {}: {}/{} rows inserted in {:?} (chunk_size={}).",
                        flight_table,
                        inserted,
                        flight_rows.len(),
                        started.elapsed(),
                        chunk_size
                    );
                }
            }
            Err(error) if is_message_too_long(&error) && chunk_size > MIN_FLIGHT_IMPORT_CHUNK_SIZE => {
                let next_chunk_size = (chunk_size / 2).max(MIN_FLIGHT_IMPORT_CHUNK_SIZE);
                eprintln!(
                    "Flight import chunk was too large for table {} at offset {}. Reducing chunk size {} -> {}.",
                    flight_table,
                    inserted,
                    chunk_size,
                    next_chunk_size
                );
                chunk_size = next_chunk_size;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

async fn insert_route_rows(
    db: &Surreal<Any>,
    route_table: &str,
    route_rows: &[Route],
) -> surrealdb::Result<()> {
    let started = Instant::now();

    for (chunk_index, chunk) in route_rows.chunks(ROUTE_IMPORT_CHUNK_SIZE).enumerate() {
        let _created: Vec<Route> = db.insert(route_table).relation(chunk.to_vec()).await?;
        let inserted = ((chunk_index + 1) * ROUTE_IMPORT_CHUNK_SIZE).min(route_rows.len());
        if inserted % 5_000 == 0 || inserted == route_rows.len() {
            println!(
                "Route import progress for {}: {}/{} rows inserted in {:?}.",
                route_table,
                inserted,
                route_rows.len(),
                started.elapsed()
            );
        }
    }

    Ok(())
}

async fn clear_schedule_tables(
    db: &Surreal<Any>,
    flight_table: &str,
    route_table: &str,
) -> surrealdb::Result<()> {
    let response = db
        .query(format!("DELETE {flight_table}; DELETE {route_table};"))
        .await?;
    response.check()?;
    Ok(())
}

fn is_message_too_long(error: &surrealdb::Error) -> bool {
    error.to_string().contains("Message too long")
}

pub async fn get_flights(db: &Surreal<Any>) -> Vec<FlightCacheRow> {
    println!("Querying flights from SurrealDB...");
    let probe_started = Instant::now();
    match timeout(
        Duration::from_secs(10),
        db.query(format!("SELECT id FROM {} LIMIT 1", PRODUCTION_FLIGHT_TABLE)),
    )
    .await
    {
        Ok(Ok(_)) => {
            println!(
                "Flight probe query completed in {:?}.",
                probe_started.elapsed()
            );
        }
        Ok(Err(error)) => {
            eprintln!(
                "Flight probe query failed after {:?}: {}",
                probe_started.elapsed(),
                error
            );
            return vec![];
        }
        Err(_) => {
            eprintln!("Flight probe query timed out after 10s.");
            return vec![];
        }
    }

    let started = Instant::now();
    let mut flights = Vec::new();
    let mut start = 0usize;
    let mut batch_size = INITIAL_FLIGHT_BATCH_SIZE;

    loop {
        let sql = format!(
            "SELECT company, flight_num, origin_code, destination_code, dep_local, arr_local, block_time_minutes, departure_terminal, arrival_terminal, operating_designator, duplicate_designators, joint_operation_airline_designators, meal_service_note, in_flight_service_info, electronic_ticketing_info FROM {} START {} LIMIT {}",
            PRODUCTION_FLIGHT_TABLE, start, batch_size
        );
        let batch_started = Instant::now();
        let mut response = match timeout(FLIGHT_PRELOAD_TIMEOUT, db.query(&sql)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                eprintln!(
                    "Flight batch query failed at offset {} after {:?}: {}",
                    start,
                    batch_started.elapsed(),
                    error
                );
                break;
            }
            Err(_) => {
                if batch_size > MIN_FLIGHT_BATCH_SIZE {
                    let next_batch_size = (batch_size / 2).max(MIN_FLIGHT_BATCH_SIZE);
                    eprintln!(
                        "Flight batch query timed out at offset {} after {:?}. Retrying with smaller batch size {} -> {}.",
                        start,
                        FLIGHT_PRELOAD_TIMEOUT,
                        batch_size,
                        next_batch_size
                    );
                    batch_size = next_batch_size;
                    continue;
                }

                eprintln!(
                    "Flight batch query timed out at offset {} after {:?} even at minimum batch size {}.",
                    start,
                    FLIGHT_PRELOAD_TIMEOUT,
                    batch_size
                );
                break;
            }
        };

        let batch_rows: Vec<FlightCacheRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!(
                    "Failed to decode flight rows at offset {} after {:?}: {}",
                    start,
                    batch_started.elapsed(),
                    error
                );
                break;
            }
        };

        let row_count = batch_rows.len();
        println!(
            "Flight batch starting at {} returned {} rows in {:?}.",
            start,
            row_count,
            batch_started.elapsed()
        );

        flights.extend(batch_rows);

        if row_count < batch_size {
            break;
        }

        start += batch_size;
    }

    println!(
        "Finished loading {} flights in {:?}.",
        flights.len(),
        started.elapsed()
    );
    flights
}
