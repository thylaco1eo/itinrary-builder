use crate::domain::airport::Airport;
use crate::memory::core::WebData;
use crate::Infrastructure::db;
use crate::Infrastructure::db::model::airport_row::AirportRow;
use crate::Infrastructure::db::model::airport_row::AirportRowError;
use actix_web::{get, put, web, HttpResponse};
use serde_json::json;

#[derive(serde::Deserialize)]
struct AirportName {
    name: String,
}

#[put("/airport")]
pub async fn add_airport(
    data: web::Data<WebData>,
    form: web::Form<AirportRow>,
) -> Result<HttpResponse, actix_web::Error> {
    let row = form.into_inner();

    // Validate by trying to convert to domain model
    if let Err(e) = Airport::try_from(row.clone()) {
        return match e {
            AirportRowError::InvalidCode(_) => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid airport code"})))
            }
            AirportRowError::InvalidTimezone(_) => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid timezone"})))
            }
            AirportRowError::InvalidLatitude => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid latitude"})))
            }
            AirportRowError::InvalidLongitude => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid longitude"})))
            }
        };
    }

    match db::repository::airport_repo::add_airport(&data.database(), row).await {
        Ok(true) => Ok(HttpResponse::Created().json(json!({"status": "ok"}))),
        Ok(false) => Ok(HttpResponse::Conflict().json(json!({"status": "conflict"}))),
        Err(e) => {
            log::error!("Error adding airport: {}", e);
            Ok(HttpResponse::InternalServerError()
                .json(json!({"status": "error", "message": e.to_string()})))
        }
    }
}

#[get("/airport")]
pub async fn get_airport(
    data: web::Data<WebData>,
    form: web::Form<AirportName>,
) -> Result<HttpResponse, actix_web::Error> {
    Ok(HttpResponse::Ok().json(json!({"status": "ok"})))
}
