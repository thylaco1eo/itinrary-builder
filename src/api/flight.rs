use crate::Infrastructure::db::model::flight_row::FlightDesignatorRow;
use crate::memory::core::{flight_storage_key, WebData};
use actix_web::{get, web, HttpResponse};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize)]
struct FlightQuery {
    company: String,
    flight_num: String,
    origin: String,
    destination: String,
    dep_date: String,
}

#[derive(Debug, Serialize)]
struct FlightResponse {
    company: String,
    flight_num: String,
    origin: String,
    destination: String,
    dep_local: String,
    arr_local: String,
    block_time_minutes: u32,
    departure_terminal: Option<String>,
    arrival_terminal: Option<String>,
    operating_designator: FlightDesignatorRow,
    duplicate_designators: Vec<FlightDesignatorRow>,
    joint_operation_airline_designators: Vec<String>,
    meal_service_note: Option<String>,
    in_flight_service_info: Option<String>,
    electronic_ticketing_info: Option<String>,
}

#[get("/flight")]
pub async fn get_flight(
    data: web::Data<WebData>,
    query: web::Query<FlightQuery>,
) -> Result<HttpResponse, actix_web::Error> {
    let params = query.into_inner();

    if params.company.is_empty()
        || params.flight_num.is_empty()
        || params.origin.is_empty()
        || params.destination.is_empty()
        || params.dep_date.is_empty()
    {
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "missing_parameters",
            "message": "company, flight_num, origin, destination, and dep_date are all required"
        })));
    }

    let dep_date = match NaiveDate::parse_from_str(&params.dep_date, "%Y-%m-%d") {
        Ok(date) => date,
        Err(e) => {
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid_date",
                "message": format!("invalid dep_date: {}. Expected format: YYYY-MM-DD", e)
            })));
        }
    };

    let company = params.company.trim().to_uppercase();
    let flight_num = params.flight_num.trim();
    let origin = params.origin.trim().to_uppercase();
    let destination = params.destination.trim().to_uppercase();

    let key = flight_storage_key(&company, flight_num, &origin, &destination, dep_date);

    let flights = data.flights();
    match flights.get(&key) {
        Some(flight) => Ok(HttpResponse::Ok().json(FlightResponse {
            company: company.clone(),
            flight_num: flight_num.to_string(),
            origin: origin.clone(),
            destination: destination.clone(),
            dep_local: flight.dep_local().to_rfc3339(),
            arr_local: flight.arr_local().to_rfc3339(),
            block_time_minutes: flight.block_time_minutes(),
            departure_terminal: flight.departure_terminal().map(|s| s.to_string()),
            arrival_terminal: flight.arrival_terminal().map(|s| s.to_string()),
            operating_designator: flight.operating_designator().clone(),
            duplicate_designators: flight.duplicate_designators().to_vec(),
            joint_operation_airline_designators: flight
                .joint_operation_airline_designators()
                .to_vec(),
            meal_service_note: flight.meal_service_note().map(|s| s.to_string()),
            in_flight_service_info: flight.in_flight_service_info().map(|s| s.to_string()),
            electronic_ticketing_info: flight.electronic_ticketing_info().map(|s| s.to_string()),
        })),
        None => Ok(HttpResponse::NotFound().json(json!({
            "status": "not_found",
            "message": format!("flight {} {} {}→{} on {} not found", company, flight_num, origin, destination, params.dep_date)
        }))),
    }
}
