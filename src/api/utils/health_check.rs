use crate::memory::core::WebData;
use actix_web::{get, web, HttpResponse};
use log::error;
use serde_json::json;

#[get("/api/healthcheck")]
pub async fn health_check(data: web::Data<WebData>) -> Result<HttpResponse, actix_web::Error> {
    data.database()
        .health()
        .await
        .map(|_| HttpResponse::Ok().json(json!({"status": "ok"})))
        .map_err(|e| {
            error!("Health check failed: {}", e);
            actix_web::error::ErrorInternalServerError("Database health check failed")
        })
}
