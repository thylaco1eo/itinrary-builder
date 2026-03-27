use std::collections::{HashMap, HashSet};

use actix_web::{get, web, HttpResponse};
use chrono::{Duration, NaiveDate};
use serde::Serialize;
use serde_json::json;
use surrealdb_types::{RecordId, RecordIdKey};

use crate::domain::airport::AirportCode;
use crate::domain::flight::Flightcore;
use crate::domain::itinerary::Itinerary;
use crate::memory::core::WebData;
use crate::Infrastructure::db::repository::route_repo::{self, PathResult, Segment};

const MAX_HOPS: u8 = 2;
const MAX_CIRCUITY: f64 = 2.0;

#[derive(Serialize)]
struct FlightInfoResponse {
    company: String,
    flight_id: String,
    origin: String,
    destination: String,
    departure: String,
    arrival: String,
    block_time_minutes: u32,
}

#[derive(Serialize)]
struct ItineraryResponse {
    airports: Vec<String>,
    flights: Vec<FlightInfoResponse>,
}

#[get("/ib")]
pub async fn get_ib(
    data: web::Data<WebData>,
    query: web::Query<Itinerary>,
) -> Result<HttpResponse, actix_web::Error> {
    let request = query.into_inner();

    let origin = request.get_origin().trim().to_uppercase();
    let destination = request.get_destination().trim().to_uppercase();
    let dep_date_raw = request.get_dep_date();

    if origin == destination {
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "invalid request",
            "message": "origin and destination must be different"
        })));
    }

    if AirportCode::new(origin.clone()).is_err() || AirportCode::new(destination.clone()).is_err() {
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "invalid request",
            "message": "origin and destination must be valid IATA codes"
        })));
    }

    let dep_date = match NaiveDate::parse_from_str(dep_date_raw.as_str(), "%Y-%m-%d") {
        Ok(date) => date,
        Err(_) => {
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid request",
                "message": "dep_date must use YYYY-MM-DD format"
            })));
        }
    };

    if !data.airports().contains_key(&origin) || !data.airports().contains_key(&destination) {
        return Ok(HttpResponse::NotFound().json(json!({
            "status": "not found",
            "message": "origin or destination airport is not loaded"
        })));
    }

    let mut paths = match route_repo::find_paths(
        data.database(),
        origin.as_str(),
        destination.as_str(),
        MAX_HOPS,
        MAX_CIRCUITY,
    )
    .await
    {
        Ok(paths) => paths,
        Err(e) => {
            log::error!(
                "Error searching routes for {} -> {}: {}",
                origin,
                destination,
                e
            );
            return Ok(HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": e.to_string()
            })));
        }
    };

    paths.sort_by(|left, right| {
        left.segments
            .len()
            .cmp(&right.segments.len())
            .then_with(|| left.circuity.total_cmp(&right.circuity))
    });

    let edge_index = build_edge_index(data.flights());
    let mut seen = HashSet::new();
    let mut itineraries = Vec::new();

    for path in &paths {
        let Some(airports) = airport_codes(&path.airports) else {
            continue;
        };

        let combinations = build_itineraries_for_path(path, &edge_index, dep_date);
        for combination in combinations {
            let itinerary = ItineraryResponse {
                airports: airports.clone(),
                flights: combination
                    .iter()
                    .map(|flight| FlightInfoResponse {
                        company: flight.company().to_string(),
                        flight_id: flight.flight_id().to_string(),
                        origin: flight.origin().as_str().to_string(),
                        destination: flight.destination().as_str().to_string(),
                        departure: flight.dep_local().to_rfc3339(),
                        arrival: flight.arr_local().to_rfc3339(),
                        block_time_minutes: flight.block_time_minutes(),
                    })
                    .collect(),
            };

            let dedup_key = itinerary
                .flights
                .iter()
                .map(|flight| {
                    format!(
                        "{}:{}:{}:{}",
                        flight.company, flight.flight_id, flight.origin, flight.departure
                    )
                })
                .collect::<Vec<_>>()
                .join("|");

            if seen.insert(dedup_key) {
                itineraries.push(itinerary);
            }
        }
    }

    Ok(HttpResponse::Ok().json(json!({
        "status": "ok",
        "itineraries": itineraries
    })))
}

fn build_edge_index<'a>(
    flights: &'a HashMap<String, Flightcore>,
) -> HashMap<(String, String), Vec<&'a Flightcore>> {
    let mut index: HashMap<(String, String), Vec<&Flightcore>> = HashMap::new();

    for flight in flights.values() {
        index
            .entry((
                flight.origin().as_str().to_string(),
                flight.destination().as_str().to_string(),
            ))
            .or_default()
            .push(flight);
    }

    for flights in index.values_mut() {
        flights.sort_by_key(|flight| flight.dep_local().timestamp());
    }

    index
}

fn build_itineraries_for_path<'a>(
    path: &PathResult,
    edge_index: &HashMap<(String, String), Vec<&'a Flightcore>>,
    dep_date: NaiveDate,
) -> Vec<Vec<&'a Flightcore>> {
    let mut candidates_per_segment = Vec::with_capacity(path.segments.len());

    for (index, segment) in path.segments.iter().enumerate() {
        let Some(candidates) =
            collect_segment_candidates(segment, edge_index, dep_date, index == 0)
        else {
            return Vec::new();
        };

        if candidates.is_empty() {
            return Vec::new();
        }

        candidates_per_segment.push(candidates);
    }

    let mut current = Vec::with_capacity(candidates_per_segment.len());
    let mut results = Vec::new();
    build_combinations(path, &candidates_per_segment, 0, &mut current, &mut results);
    results
}

fn collect_segment_candidates<'a>(
    segment: &Segment,
    edge_index: &HashMap<(String, String), Vec<&'a Flightcore>>,
    dep_date: NaiveDate,
    is_first_segment: bool,
) -> Option<Vec<&'a Flightcore>> {
    let from = record_id_code(&segment.from)?;
    let to = record_id_code(&segment.to)?;
    let allowed_flights: HashSet<&str> = segment.flights.iter().map(String::as_str).collect();

    let candidates = edge_index
        .get(&(from, to))
        .map(|flights| {
            flights
                .iter()
                .copied()
                .filter(|flight| allowed_flights.contains(flight_route_key(flight).as_str()))
                .filter(|flight| !is_first_segment || flight.dep_local().date_naive() == dep_date)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some(candidates)
}

fn build_combinations<'a>(
    path: &PathResult,
    candidates_per_segment: &[Vec<&'a Flightcore>],
    segment_index: usize,
    current: &mut Vec<&'a Flightcore>,
    results: &mut Vec<Vec<&'a Flightcore>>,
) {
    if segment_index == candidates_per_segment.len() {
        results.push(current.clone());
        return;
    }

    for flight in &candidates_per_segment[segment_index] {
        if connection_is_valid(path, segment_index, current, flight) {
            current.push(*flight);
            build_combinations(
                path,
                candidates_per_segment,
                segment_index + 1,
                current,
                results,
            );
            current.pop();
        }
    }
}

fn connection_is_valid(
    path: &PathResult,
    segment_index: usize,
    current: &[&Flightcore],
    next_flight: &Flightcore,
) -> bool {
    if segment_index == 0 {
        return true;
    }

    let previous_flight = current[segment_index - 1];
    let minimum_connection_minutes = path.segments[segment_index - 1].mct.max(0);
    let earliest_departure =
        previous_flight.arr_local().clone() + Duration::minutes(minimum_connection_minutes);

    next_flight.dep_local() >= &earliest_departure
}

fn airport_codes(airports: &[RecordId]) -> Option<Vec<String>> {
    airports.iter().map(record_id_code).collect()
}

fn record_id_code(record_id: &RecordId) -> Option<String> {
    match &record_id.key {
        RecordIdKey::String(code) => Some(code.clone()),
        _ => None,
    }
}

fn flight_route_key(flight: &Flightcore) -> String {
    format!("{}_{}", flight.company(), flight.flight_id())
}
