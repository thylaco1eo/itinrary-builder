use crate::Infrastructure::db::repository::cache_repo;
use crate::memory::core::WebData;
use actix_web::{delete, get, post, web, HttpResponse};
use serde::Deserialize;
use serde_json::json;

use super::cache_builder;

#[derive(Debug, Deserialize)]
struct HotODBody {
    origin: String,
    destination: String,
}

#[get("/hot-od")]
pub async fn list_hot_ods(data: web::Data<WebData>) -> Result<HttpResponse, actix_web::Error> {
    let hot_ods = data.hot_ods();
    let ods: Vec<serde_json::Value> = hot_ods
        .iter()
        .map(|(o, d)| json!({"origin": o, "destination": d}))
        .collect();
    Ok(HttpResponse::Ok().json(json!({
        "hot_ods": ods,
        "count": ods.len()
    })))
}

#[post("/hot-od")]
pub async fn add_hot_od(
    data: web::Data<WebData>,
    body: web::Json<HotODBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let origin = body.origin.trim().to_uppercase();
    let destination = body.destination.trim().to_uppercase();

    if origin.is_empty() || destination.is_empty() {
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "error",
            "message": "origin and destination must be non-empty"
        })));
    }

    if origin == destination {
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "error",
            "message": "origin and destination must differ"
        })));
    }

    let len = origin.len().max(destination.len());
    if len != 3 {
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "error",
            "message": "origin and destination must be valid IATA codes (3 characters)"
        })));
    }

    match cache_repo::add_hot_od(data.database(), &origin, &destination).await {
        Ok(()) => {
            data.add_hot_od(origin.clone(), destination.clone());
            cache_builder::build_itin_cache_for_od(&data, &origin, &destination);
            Ok(HttpResponse::Created().json(json!({
                "status": "ok",
                "origin": origin,
                "destination": destination
            })))
        }
        Err(e) => {
            log::error!("Failed to add hot OD: {}", e);
            Ok(HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": e.to_string()
            })))
        }
    }
}

#[delete("/hot-od/{origin}/{destination}")]
pub async fn remove_hot_od(
    data: web::Data<WebData>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, actix_web::Error> {
    let (origin, destination) = path.into_inner();
    let origin = origin.trim().to_uppercase();
    let destination = destination.trim().to_uppercase();

    match cache_repo::remove_hot_od(data.database(), &origin, &destination).await {
        Ok(deleted) if deleted > 0 => {
            data.remove_hot_od(&origin, &destination);
            data.itin_cache
                .write()
                .unwrap()
                .retain(|(o, d, _), _| o != &origin || d != &destination);
            Ok(HttpResponse::Ok().json(json!({
                "status": "ok",
                "removed": deleted
            })))
        }
        Ok(_) => Ok(HttpResponse::NotFound().json(json!({
            "status": "not_found"
        }))),
        Err(e) => {
            log::error!("Failed to remove hot OD: {}", e);
            Ok(HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": e.to_string()
            })))
        }
    }
}
