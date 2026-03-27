use std::time::{Duration, Instant};

use actix_web::rt::time::timeout;
use crate::Infrastructure::db::model::airport_row::{AirportCodeRow, AirportRow, AirportStoredRow};
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

    let _response = db.query("CREATE type::record('airport',$id) SET code = $code, timezone = $timezone, name = $name, city = $city, country = $country, location = $location, mct = $mct")
        .bind(("id", id))
        .bind(("code", airport.code.code))
        .bind(("timezone", airport.timezone))
        .bind(("name", airport.name))
        .bind(("city", airport.city))
        .bind(("country", airport.country))
        .bind(("location", Value::Geometry(point)))  // ← 关键修改
        .bind(("mct", airport.mct))
        .await?;

    Ok(true)
}

pub async fn get_all_airports(db: &Surreal<Any>) -> Vec<AirportRow> {

    let records: Vec<Value> = db.select("airport").await.unwrap_or_default();

    records
        .into_iter()
        .filter_map(|record| {
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
            let (longitude, latitude) = match object.get("location") {
                Some(Value::Geometry(Geometry::Point(point))) => (point.x(), point.y()),
                _ => return None,
            };
            let mct = match object.get("mct") {
                Some(Value::Number(number)) => number.to_int().and_then(|value| u32::try_from(value).ok()),
                Some(Value::None | Value::Null) | None => None,
                _ => None,
            };

            Some(AirportRow {
                code: AirportCodeRow { code },
                timezone,
                name,
                city,
                country,
                latitude,
                longitude,
                mct,
            })
        })
        .collect()
}
