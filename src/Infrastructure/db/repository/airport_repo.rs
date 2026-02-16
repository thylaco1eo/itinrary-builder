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
    let result: Option<AirportRow> = db.insert(("airport", id)).content(airport).await?;
    Ok(result.is_some())
}
