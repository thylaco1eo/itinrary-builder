use std::env;
use std::time::{Duration, Instant};

use crate::Infrastructure::db::model::airport_row::{AirportCodeRow, AirportRow};
use actix_web::rt::time::timeout;
use geo::Point;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::{Geometry, Value};

const DEFAULT_AIRPORT_PRELOAD_BATCH_SIZE: usize = 100;
const DEFAULT_AIRPORT_PRELOAD_MIN_BATCH_SIZE: usize = 25;
const DEFAULT_AIRPORT_PRELOAD_TIMEOUT_SECS: u64 = 30;

pub async fn check_airport_exists(
    db: &Surreal<Any>,
    code: &AirportCodeRow,
) -> surrealdb::Result<bool> {
    let result: Option<serde_json::Value> = db.select(("airport", code.code.clone())).await?;
    Ok(result.is_some())
}

pub async fn add_airport(db: &Surreal<Any>, airport: AirportRow) -> surrealdb::Result<bool> {
    let id = airport.code.code.clone();
    let exists = check_airport_exists(db, &airport.code).await?;
    if exists {
        return Ok(false);
    }

    // Use raw query to ensure the correct insertion if high-level API fails
    let point = Geometry::Point(Point::new(airport.longitude, airport.latitude));

    let _response = db.query("CREATE type::record('airport',$id) SET code = $code, timezone = $timezone, name = $name, city = $city, country = $country, state = $state, location = $location")
        .bind(("id", id))
        .bind(("code", airport.code.code))
        .bind(("timezone", airport.timezone))
        .bind(("name", airport.name))
        .bind(("city", airport.city))
        .bind(("country", airport.country))
        .bind(("state", airport.state))
        .bind(("location", Value::Geometry(point)))
        .await?;

    Ok(true)
}

pub async fn get_airport(db: &Surreal<Any>, code: &str) -> surrealdb::Result<Option<AirportRow>> {
    let record: Option<Value> = db.select(("airport", code.to_string())).await?;
    Ok(record.and_then(map_airport_row))
}

pub async fn get_all_airport_codes(db: &Surreal<Any>) -> surrealdb::Result<Vec<String>> {
    let mut response = db.query("SELECT code FROM airport").await?;
    let records: Vec<Value> = response.take(0)?;
    Ok(records
        .into_iter()
        .filter_map(|record| {
            let Value::Object(object) = record else {
                return None;
            };
            match object.get("code") {
                Some(Value::String(code)) => Some(code.clone()),
                _ => None,
            }
        })
        .collect())
}

pub async fn clear_legacy_airport_mct_fields(db: &Surreal<Any>) -> surrealdb::Result<()> {
    db.query("UPDATE airport SET mct = NONE, mct_records = [], connection_building_filters = []")
        .await?;
    Ok(())
}

pub async fn clear_legacy_airport_mct_fields_for_airport(
    db: &Surreal<Any>,
    code: &str,
) -> surrealdb::Result<()> {
    db.query(
        "UPDATE type::record('airport',$id) SET mct = NONE, mct_records = [], connection_building_filters = []",
    )
    .bind(("id", code.to_string()))
    .await?;
    Ok(())
}

pub async fn get_all_airports(db: &Surreal<Any>) -> Vec<AirportRow> {
    println!("Querying airports from SurrealDB...");
    let batch_timeout_secs = airport_preload_timeout_secs();
    let probe_started = Instant::now();
    match timeout(
        Duration::from_secs(10),
        db.query("SELECT id FROM airport LIMIT 1"),
    )
    .await
    {
        Ok(Ok(_)) => {
            println!(
                "Airport probe query completed in {:?}.",
                probe_started.elapsed()
            );
        }
        Ok(Err(error)) => {
            eprintln!(
                "Airport probe query failed after {:?}: {}",
                probe_started.elapsed(),
                error
            );
            return vec![];
        }
        Err(_) => {
            eprintln!("Airport probe query timed out after 10s.");
            return vec![];
        }
    }

    let started = Instant::now();
    let mut airports = Vec::new();
    let mut start = 0usize;
    let mut batch_size = airport_preload_batch_size();
    let min_batch_size = airport_preload_min_batch_size(batch_size);

    println!(
        "Airport preload settings: batch_size = {}, min_batch_size = {}, timeout = {}s.",
        batch_size, min_batch_size, batch_timeout_secs
    );

    loop {
        let sql = format!(
            "SELECT code, timezone, name, city, country, state, location FROM airport START {} LIMIT {}",
            start, batch_size
        );
        let batch_started = Instant::now();
        let mut response = match timeout(Duration::from_secs(batch_timeout_secs), db.query(&sql))
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                eprintln!(
                    "Airport batch query failed at offset {} after {:?}: {}",
                    start,
                    batch_started.elapsed(),
                    error
                );
                break;
            }
            Err(_) => {
                if batch_size > min_batch_size {
                    let next_batch_size = (batch_size / 2).max(min_batch_size);
                    eprintln!(
                        "Airport batch query timed out at offset {} after {}s with batch size {}. Retrying with batch size {}.",
                        start, batch_timeout_secs, batch_size, next_batch_size
                    );
                    batch_size = next_batch_size;
                    continue;
                }

                eprintln!(
                    "Airport batch query timed out at offset {} after {}s even at minimum batch size {}.",
                    start, batch_timeout_secs, batch_size
                );
                break;
            }
        };

        let records: Vec<Value> = match response.take(0) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!(
                    "Failed to decode airport rows at offset {} after {:?}: {}",
                    start,
                    batch_started.elapsed(),
                    error
                );
                break;
            }
        };

        let row_count = records.len();
        println!(
            "Airport batch starting at {} returned {} rows in {:?}.",
            start,
            row_count,
            batch_started.elapsed()
        );

        airports.extend(records.into_iter().filter_map(map_airport_row));

        if row_count < batch_size {
            break;
        }

        start += row_count;
    }

    println!(
        "Finished loading {} airports in {:?}.",
        airports.len(),
        started.elapsed()
    );
    airports
}

fn airport_preload_batch_size() -> usize {
    env::var("IB_AIRPORT_PRELOAD_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AIRPORT_PRELOAD_BATCH_SIZE)
}

fn airport_preload_min_batch_size(initial_batch_size: usize) -> usize {
    env::var("IB_AIRPORT_PRELOAD_MIN_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0 && *value <= initial_batch_size)
        .unwrap_or(DEFAULT_AIRPORT_PRELOAD_MIN_BATCH_SIZE.min(initial_batch_size))
}

fn airport_preload_timeout_secs() -> u64 {
    env::var("IB_AIRPORT_PRELOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AIRPORT_PRELOAD_TIMEOUT_SECS)
}

fn map_airport_row(record: Value) -> Option<AirportRow> {
    let Value::Object(object) = record else {
        return None;
    };

    let code = match object.get("code") {
        Some(Value::String(code)) => code.clone(),
        _ => return None,
    };
    let timezone = match object.get("timezone") {
        Some(Value::String(timezone)) => timezone.clone(),
        _ => return None,
    };
    let name = match object.get("name") {
        Some(Value::String(name)) => Some(name.clone()),
        Some(Value::None | Value::Null) | None => None,
        _ => None,
    };
    let city = match object.get("city") {
        Some(Value::String(city)) => Some(city.clone()),
        Some(Value::None | Value::Null) | None => None,
        _ => None,
    };
    let country = match object.get("country") {
        Some(Value::String(country)) => Some(country.clone()),
        Some(Value::None | Value::Null) | None => None,
        _ => None,
    };
    let state = match object.get("state") {
        Some(Value::String(state)) => Some(state.clone()),
        Some(Value::None | Value::Null) | None => None,
        _ => None,
    };
    let (longitude, latitude) = match object.get("location") {
        Some(Value::Geometry(Geometry::Point(point))) => (point.x(), point.y()),
        _ => return None,
    };

    Some(AirportRow {
        code: AirportCodeRow { code },
        timezone,
        name,
        city,
        country,
        state,
        latitude,
        longitude,
    })
}
