use std::time::{Duration, Instant};

use crate::domain::{flightplan::FlightPlan, route::Route};
use crate::Infrastructure::db::model::flight_row::FlightRow;
use actix_web::rt::time::timeout;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::RecordId;

pub async fn add_flights_batch(db: &Surreal<Any>, rows: &[FlightRow]) -> surrealdb::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // 使用 SDK 的 insert 一次性插入多条记录
    let _created: Vec<FlightRow> = db.insert("flight").content(rows.to_vec()).await?;
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

pub async fn get_flights(db: &Surreal<Any>) -> Vec<FlightRow> {
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
    let batch_size = 10_000usize;

    loop {
        let sql = format!(
            "SELECT id, company, flight_num, origin_code, destination_code, dep_local, arr_local, block_time_minutes, departure_terminal, arrival_terminal, operating_designator, duplicate_designators, joint_operation_airline_designators, meal_service_note, in_flight_service_info, electronic_ticketing_info, type3_legs FROM flight START {} LIMIT {}",
            start, batch_size
        );
        let batch_started = Instant::now();
        let mut response = match timeout(Duration::from_secs(30), db.query(&sql)).await {
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
                eprintln!(
                    "Flight batch query timed out at offset {} after 30s.",
                    start
                );
                break;
            }
        };

        let batch_rows: Vec<FlightRow> = match response.take(0) {
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
