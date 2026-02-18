use surrealdb::engine::any::Any;
use surrealdb::Surreal;
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
    
    // Use raw query to ensure correct insertion if high-level API fails
    let _response = db.query("CREATE type::record('airport',$id) SET code = $code, timezone = $timezone, name = $name, city = $city, country = $country, location = $location, mct = $mct")
        .bind(("id",id))
        .bind(("code", airport.code.code))
        .bind(("timezone", airport.timezone))
        .bind(("name", airport.name))
        .bind(("city", airport.city))
        .bind(("country", airport.country))
        .bind(("location", (airport.longitude, airport.latitude)))
        .bind(("mct", airport.mct))
        .await?;
        
    Ok(true)
}
