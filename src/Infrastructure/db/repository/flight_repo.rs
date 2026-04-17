use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::domain::{flightplan::FlightPlan, route::Route};
use crate::Infrastructure::db::model::flight_row::{FlightCacheRow, FlightRow};
use actix_web::rt::time::timeout;
use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;
use surrealdb::Surreal;
use surrealdb_types::RecordId;

const FLIGHT_PRELOAD_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_FLIGHT_BATCH_SIZE: usize = 2_000;
const MIN_FLIGHT_BATCH_SIZE: usize = 250;
const INITIAL_FLIGHT_IMPORT_CHUNK_SIZE: usize = 2_000;
const MIN_FLIGHT_IMPORT_CHUNK_SIZE: usize = 125;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteUpsert {
    pub origin: String,
    pub destination: String,
    pub flights: Vec<String>,
    pub companies: Vec<String>,
}

pub async fn add_flights_batch(db: &Surreal<Any>, rows: &[FlightRow]) -> surrealdb::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // 使用 SDK 的 insert 一次性插入多条记录
    let _created: Vec<FlightRow> = db.insert("flight").content(rows.to_vec()).await?;
    Ok(())
}

pub async fn import_schedule_atomically(
    db: &Surreal<Any>,
    flight_rows: &[FlightRow],
    route_updates: &[RouteUpsert],
) -> surrealdb::Result<()> {
    let transaction = db.clone().begin().await?;

    let apply_result = apply_schedule_import(&transaction, flight_rows, route_updates).await;
    if let Err(error) = apply_result {
        if let Err(cancel_error) = transaction.cancel().await {
            eprintln!(
                "Failed to cancel schedule import transaction after error: {}",
                cancel_error
            );
        }
        return Err(error);
    }

    transaction.commit().await?;
    Ok(())
}

pub async fn add_route(db: &Surreal<Any>, plan: &FlightPlan) -> surrealdb::Result<()> {
    let origin = plan.origin.as_str();
    let destination = plan.destination.as_str();
    let flight_id = format!("{}_{}", plan.company, plan.flight_no);
    let company = plan.company.clone();
    let route_id = format!("{}_{}", origin, destination);

    match db
        .select::<Option<Route>>(("route", route_id.as_str()))
        .await?
    {
        Some(mut route) => {
            let mut dirty = false;

            if !route.flights.contains(&flight_id) {
                route.flights.push(flight_id);
                dirty = true;
            }
            if !route.companies.contains(&company) {
                route.companies.push(company);
                dirty = true;
            }

            if dirty {
                let _: Option<Route> = db.update(("route", route_id.as_str())).merge(route).await?;
            }
        }
        None => {
            let route = Route::new(
                RecordId::new("airport", origin),
                RecordId::new("airport", destination),
                RecordId::new("route", route_id.as_str()),
                vec![flight_id],
                vec![company],
            );
            let _: Vec<Route> = db.insert("route").relation(route).await?;
        }
    }

    Ok(())
}

async fn apply_schedule_import(
    transaction: &Transaction<Any>,
    flight_rows: &[FlightRow],
    route_updates: &[RouteUpsert],
) -> surrealdb::Result<()> {
    if !flight_rows.is_empty() {
        insert_flight_rows_in_transaction(transaction, flight_rows).await?;
    }

    let route_started = Instant::now();
    for (index, route_update) in route_updates.iter().enumerate() {
        upsert_route_in_transaction(transaction, route_update).await?;
        if (index + 1) % 5_000 == 0 || index + 1 == route_updates.len() {
            println!(
                "Atomic route import progress: {}/{} route updates applied in {:?}.",
                index + 1,
                route_updates.len(),
                route_started.elapsed()
            );
        }
    }

    Ok(())
}

async fn insert_flight_rows_in_transaction(
    transaction: &Transaction<Any>,
    flight_rows: &[FlightRow],
) -> surrealdb::Result<()> {
    let started = Instant::now();
    let mut inserted = 0usize;
    let mut chunk_size = INITIAL_FLIGHT_IMPORT_CHUNK_SIZE.min(flight_rows.len());

    while inserted < flight_rows.len() {
        let end = (inserted + chunk_size).min(flight_rows.len());
        let chunk = &flight_rows[inserted..end];
        let insert_result: surrealdb::Result<Vec<FlightRow>> =
            transaction.insert("flight").content(chunk.to_vec()).await;

        match insert_result {
            Ok(_) => {
                inserted = end;
                if inserted % 10_000 == 0 || inserted == flight_rows.len() {
                    println!(
                        "Atomic flight import progress: {}/{} rows inserted in {:?} (chunk_size={}).",
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
                    "Atomic flight import chunk was too large at offset {}. Reducing chunk size {} -> {}.",
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

fn is_message_too_long(error: &surrealdb::Error) -> bool {
    error.to_string().contains("Message too long")
}

async fn upsert_route_in_transaction(
    transaction: &Transaction<Any>,
    route_update: &RouteUpsert,
) -> surrealdb::Result<()> {
    let route_id = format!("{}_{}", route_update.origin, route_update.destination);

    match transaction
        .select::<Option<Route>>(("route", route_id.as_str()))
        .await?
    {
        Some(mut route) => {
            let merged_flights = route
                .flights
                .into_iter()
                .chain(route_update.flights.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let merged_companies = route
                .companies
                .into_iter()
                .chain(route_update.companies.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            route.flights = merged_flights;
            route.companies = merged_companies;

            let _: Option<Route> = transaction.update(("route", route_id.as_str())).merge(route).await?;
        }
        None => {
            let route = Route::new(
                RecordId::new("airport", route_update.origin.as_str()),
                RecordId::new("airport", route_update.destination.as_str()),
                RecordId::new("route", route_id.as_str()),
                route_update.flights.clone(),
                route_update.companies.clone(),
            );
            let _: Vec<Route> = transaction.insert("route").relation(route).await?;
        }
    }

    Ok(())
}

pub async fn get_flights(db: &Surreal<Any>) -> Vec<FlightCacheRow> {
    println!("Querying flights from SurrealDB...");
    let probe_started = Instant::now();
    match timeout(
        Duration::from_secs(10),
        db.query("SELECT id FROM flight LIMIT 1"),
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
            "SELECT company, flight_num, origin_code, destination_code, dep_local, arr_local, block_time_minutes, departure_terminal, arrival_terminal, operating_designator, duplicate_designators, joint_operation_airline_designators, meal_service_note, in_flight_service_info, electronic_ticketing_info FROM flight START {} LIMIT {}",
            start, batch_size
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
