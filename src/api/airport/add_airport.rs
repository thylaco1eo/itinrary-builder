use actix_web::{put, web, HttpResponse};
use serde_json::json;
use crate::Infrastructure::db;
use crate::Infrastructure::db::model::airport_row::AirportRow;
use crate::structure::WebData;

use crate::domain::airport::Airport;
use crate::Infrastructure::db::model::airport_row::AirportRowError;

#[put("/airport")]
pub async fn add_airport(data: web::Data<WebData>, form: web::Form<AirportRow>) -> Result<HttpResponse, actix_web::Error> {
    let row = form.into_inner();
    
    // Validate by trying to convert to domain model
    if let Err(e) = Airport::try_from(row.clone()) {
        return match e {
            AirportRowError::InvalidCode(_) => Ok(HttpResponse::BadRequest().json(json!({"status": "invalid airport code"}))),
            AirportRowError::InvalidTimezone(_) => Ok(HttpResponse::BadRequest().json(json!({"status": "invalid timezone"}))),
            AirportRowError::InvalidLatitude => Ok(HttpResponse::BadRequest().json(json!({"status": "invalid latitude"}))),
            AirportRowError::InvalidLongitude => Ok(HttpResponse::BadRequest().json(json!({"status": "invalid longitude"}))),
            AirportRowError::InvalidLocationType => Ok(HttpResponse::BadRequest().json(json!({"status": "invalid location type"}))),
        };
    }

    match db::repository::airport_repo::add_airport(&data.database(), row).await {
        Ok(true) => Ok(HttpResponse::Created().json(json!({"status": "ok"}))),
        Ok(false) => Ok(HttpResponse::Conflict().json(json!({"status": "conflict"}))),
        Err(_) => Ok(HttpResponse::InternalServerError().json(json!({"status": "error"})))
    }
}