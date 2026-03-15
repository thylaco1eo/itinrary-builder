use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::{Geometry, Value};
use geo::Point;
use crate::Infrastructure::db::model::airport_row::{AirportCodeRow, AirportRow};

pub async fn check_airport_exists(db: &Surreal<Any>, code: &AirportCodeRow) -> surrealdb::Result<bool> {
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
    db.select("airport").await.unwrap_or(vec![])
}
