use std::time::{Duration, Instant};

use crate::domain::mct::{
    AirportMctRecord, ConnectionBuildingFilter, ensure_airport_default_mct_records,
};
use crate::Infrastructure::db::model::airport_row::{AirportCodeRow, AirportRow};
use actix_web::rt::time::timeout;
use geo::Point;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::{Geometry, Value};

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

    let _response = db.query("CREATE type::record('airport',$id) SET code = $code, timezone = $timezone, name = $name, city = $city, country = $country, state = $state, location = $location, mct_records = $mct_records, connection_building_filters = $connection_building_filters")
        .bind(("id", id))
        .bind(("code", airport.code.code))
        .bind(("timezone", airport.timezone))
        .bind(("name", airport.name))
        .bind(("city", airport.city))
        .bind(("country", airport.country))
        .bind(("state", airport.state))
        .bind(("location", Value::Geometry(point)))  // ← 关键修改
        .bind(("mct_records", airport.mct_records))
        .bind((
            "connection_building_filters",
            airport.connection_building_filters,
        ))
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

pub async fn set_airport_mct_records(
    db: &Surreal<Any>,
    code: &str,
    mct_records: Vec<AirportMctRecord>,
) -> surrealdb::Result<bool> {
    set_airport_mct_payload(db, code, mct_records, Vec::new(), false).await
}

pub async fn set_airport_mct_payload(
    db: &Surreal<Any>,
    code: &str,
    mct_records: Vec<AirportMctRecord>,
    connection_building_filters: Vec<ConnectionBuildingFilter>,
    update_filters: bool,
) -> surrealdb::Result<bool> {
    let code_row = AirportCodeRow {
        code: code.to_string(),
    };
    if !check_airport_exists(db, &code_row).await? {
        return Ok(false);
    }

    if update_filters {
        db.query(
            "UPDATE type::record('airport',$id) SET mct_records = $mct_records, connection_building_filters = $connection_building_filters UNSET mct",
        )
        .bind(("id", code.to_string()))
        .bind(("mct_records", mct_records))
        .bind(("connection_building_filters", connection_building_filters))
        .await?;
    } else {
        db.query("UPDATE type::record('airport',$id) SET mct_records = $mct_records UNSET mct")
            .bind(("id", code.to_string()))
            .bind(("mct_records", mct_records))
            .await?;
    }

    Ok(true)
}

pub async fn clear_all_airport_mct_records(db: &Surreal<Any>) -> surrealdb::Result<()> {
    db.query("UPDATE airport SET mct_records = [], connection_building_filters = [] UNSET mct")
        .await?;
    Ok(())
}

pub async fn get_all_airports(db: &Surreal<Any>) -> Vec<AirportRow> {
    println!("Querying airports from SurrealDB...");
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
    let batch_size = 5_000usize;

    loop {
        let sql = format!(
            "SELECT code, timezone, name, city, country, state, location, mct, connection_building_filters, mct_records FROM airport START {} LIMIT {}",
            start, batch_size
        );
        let batch_started = Instant::now();
        let mut response = match timeout(Duration::from_secs(30), db.query(&sql)).await {
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
                eprintln!(
                    "Airport batch query timed out at offset {} after 30s.",
                    start
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

        start += batch_size;
    }

    println!(
        "Finished loading {} airports in {:?}.",
        airports.len(),
        started.elapsed()
    );
    airports
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
    let legacy_mct = match object.get("mct") {
        Some(Value::Number(number)) => number.to_int().and_then(|value| u32::try_from(value).ok()),
        Some(Value::None | Value::Null) | None => None,
        _ => None,
    };
    let mct_records = ensure_airport_default_mct_records(
        object
            .get("mct_records")
            .map(parse_mct_records)
            .unwrap_or_default(),
        legacy_mct,
    );

    Some(AirportRow {
        code: AirportCodeRow { code },
        timezone,
        name,
        city,
        country,
        state,
        latitude,
        longitude,
        mct_records,
        connection_building_filters: object
            .get("connection_building_filters")
            .map(parse_connection_building_filters)
            .unwrap_or_default(),
    })
}

fn parse_mct_records(value: &Value) -> Vec<AirportMctRecord> {
    let Ok(json_value) = serde_json::to_value(value) else {
        return Vec::new();
    };

    serde_json::from_value(json_value).unwrap_or_default()
}

fn parse_connection_building_filters(value: &Value) -> Vec<ConnectionBuildingFilter> {
    let Ok(json_value) = serde_json::to_value(value) else {
        return Vec::new();
    };

    serde_json::from_value(json_value).unwrap_or_default()
}
