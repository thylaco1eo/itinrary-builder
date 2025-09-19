use actix_web::{get, HttpResponse, Responder};
use serde_json::json;

pub mod data_service;

pub mod search_service {
    pub mod search_flight;
}

pub mod rule_service {
    pub mod pre_rules;
    pub mod post_rules;
}

#[get("/api/healthcheck")]
async fn health_check() -> impl Responder {
    const MESSAGE: &str = "Itinbuilder is running";
    HttpResponse::Ok().json(json!({"status": "success","message": MESSAGE}))
}