use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;

use actix_web::{get, web, HttpResponse};
use chrono::{Duration, NaiveDate, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use surrealdb_types::{RecordId, RecordIdKey};

use crate::domain::airport::AirportCode;
use crate::domain::flight::Flightcore;
use crate::domain::itinerary::Itinerary;
use crate::memory::core::{flight_storage_key, WebData};
use crate::Infrastructure::db::repository::route_repo::{self, PathResult, Segment};

const MAX_HOPS: u8 = 2;
const MAX_CIRCUITY: f64 = 2.0;
const DEFAULT_MCT_MINUTES: i64 = 180;
const MAX_CONNECTION_WINDOW_HOURS: i64 = 24;
const REQUEST_LOG_PATH: &str = "./log/requests.log";

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
    let raw_origin = request.get_origin();
    let raw_destination = request.get_destination();
    let dep_date_raw = request.get_dep_date();
    let request_trace = request_trace_id(&raw_origin, &raw_destination, &dep_date_raw);

    request_info(
        &request_trace,
        "request_received",
        json!({
            "raw_origin": raw_origin,
            "raw_destination": raw_destination,
            "raw_dep_date": dep_date_raw
        }),
    );

    let origin = raw_origin.trim().to_uppercase();
    let destination = raw_destination.trim().to_uppercase();

    if origin == destination {
        request_warn(
            &request_trace,
            "request_rejected_same_airport",
            json!({
                "origin": origin,
                "destination": destination
            }),
        );
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "invalid request",
            "message": "origin and destination must be different"
        })));
    }

    if AirportCode::new(origin.clone()).is_err() || AirportCode::new(destination.clone()).is_err() {
        request_warn(
            &request_trace,
            "request_rejected_invalid_iata",
            json!({
                "origin": origin,
                "destination": destination
            }),
        );
        return Ok(HttpResponse::BadRequest().json(json!({
            "status": "invalid request",
            "message": "origin and destination must be valid IATA codes"
        })));
    }

    let dep_date = match NaiveDate::parse_from_str(dep_date_raw.as_str(), "%Y-%m-%d") {
        Ok(date) => date,
        Err(_) => {
            request_warn(
                &request_trace,
                "request_rejected_invalid_dep_date",
                json!({
                    "dep_date": dep_date_raw
                }),
            );
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid request",
                "message": "dep_date must use YYYY-MM-DD format"
            })));
        }
    };

    request_info(
        &request_trace,
        "request_normalized",
        json!({
            "origin": origin,
            "destination": destination,
            "dep_date": dep_date.to_string()
        }),
    );

    let origin_loaded = data.airports().contains_key(&origin);
    let destination_loaded = data.airports().contains_key(&destination);
    if !origin_loaded || !destination_loaded {
        request_warn(
            &request_trace,
            "request_airports_not_loaded",
            json!({
                "origin": origin,
                "destination": destination,
                "origin_loaded": origin_loaded,
                "destination_loaded": destination_loaded
            }),
        );
        return Ok(HttpResponse::NotFound().json(json!({
            "status": "not found",
            "message": "origin or destination airport is not loaded"
        })));
    }

    request_info(
        &request_trace,
        "route_search_started",
        json!({
            "origin": origin,
            "destination": destination,
            "max_hops": MAX_HOPS,
            "max_circuity": MAX_CIRCUITY
        }),
    );
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
            request_error(
                &request_trace,
                "route_search_failed",
                json!({
                    "origin": origin,
                    "destination": destination,
                    "error": e.to_string()
                }),
            );
            return Ok(HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": e.to_string()
            })));
        }
    };

    request_info(
        &request_trace,
        "route_search_completed",
        json!({
            "path_count": paths.len()
        }),
    );

    paths.sort_by(|left, right| {
        left.segments
            .len()
            .cmp(&right.segments.len())
            .then_with(|| left.circuity.total_cmp(&right.circuity))
    });

    for (path_index, path) in paths.iter().enumerate() {
        log_route_result(&request_trace, path_index + 1, path);
    }

    request_info(
        &request_trace,
        "flight_lookup_strategy",
        json!({
            "strategy": "hashmap_by_company_flight_origin_destination_dep_date",
            "flight_count": data.flights().len(),
            "max_connection_window_hours": MAX_CONNECTION_WINDOW_HOURS
        }),
    );

    let mut seen = HashSet::new();
    let mut itineraries = Vec::new();

    for (path_index, path) in paths.iter().enumerate() {
        let Some(airports) = airport_codes(&path.airports) else {
            request_warn(
                &request_trace,
                "path_skipped_invalid_airport_code",
                json!({
                    "path_index": path_index + 1
                }),
            );
            continue;
        };

        let combinations = build_itineraries_for_path(
            path,
            data.flights(),
            dep_date,
            &request_trace,
            path_index + 1,
        );
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
                request_info(
                    &request_trace,
                    "itinerary_accepted",
                    json!({
                        "path_index": path_index + 1,
                        "flight_chain": flight_chain_json(&combination)
                    }),
                );
                itineraries.push(itinerary);
            } else {
                request_info(
                    &request_trace,
                    "itinerary_duplicate",
                    json!({
                        "path_index": path_index + 1,
                        "flight_chain": flight_chain_json(&combination)
                    }),
                );
            }
        }
    }

    request_info(
        &request_trace,
        "request_completed",
        json!({
            "itinerary_count": itineraries.len()
        }),
    );

    Ok(HttpResponse::Ok().json(json!({
        "status": "ok",
        "itineraries": itineraries
    })))
}

fn build_itineraries_for_path<'a>(
    path: &PathResult,
    flights: &'a HashMap<String, Flightcore>,
    dep_date: NaiveDate,
    request_trace: &str,
    path_index: usize,
) -> Vec<Vec<&'a Flightcore>> {
    request_info(
        request_trace,
        "itinerary_build_started",
        json!({
            "path_index": path_index,
            "airport_path": airport_path_json(&path.airports),
            "dep_date": dep_date.to_string()
        }),
    );

    let mut current = Vec::with_capacity(path.segments.len());
    let mut results = Vec::new();
    build_combinations(
        path,
        flights,
        dep_date,
        0,
        &mut current,
        &mut results,
        request_trace,
        path_index,
    );
    request_info(
        request_trace,
        "itinerary_build_completed",
        json!({
            "path_index": path_index,
            "combination_count": results.len()
        }),
    );
    results
}

fn build_combinations<'a>(
    path: &PathResult,
    flights: &'a HashMap<String, Flightcore>,
    dep_date: NaiveDate,
    segment_index: usize,
    current: &mut Vec<&'a Flightcore>,
    results: &mut Vec<Vec<&'a Flightcore>>,
    request_trace: &str,
    path_index: usize,
) {
    if segment_index == path.segments.len() {
        request_info(
            request_trace,
            "itinerary_combination_completed",
            json!({
                "path_index": path_index,
                "flight_chain": flight_chain_json(current)
            }),
        );
        results.push(current.clone());
        return;
    }

    let segment_count = path.segments.len();
    let segment = &path.segments[segment_index];
    let Some(candidates) = collect_segment_candidates(
        path,
        segment,
        flights,
        dep_date,
        current,
        segment_index,
        request_trace,
        path_index,
        segment_count,
    ) else {
        request_warn(
            request_trace,
            "segment_invalid_record_ids",
            json!({
                "path_index": path_index,
                "segment_index": segment_index + 1,
                "segment_count": segment_count
            }),
        );
        return;
    };

    if candidates.is_empty() {
        request_info(
            request_trace,
            "segment_abandoned_no_candidates",
            json!({
                "path_index": path_index,
                "segment_index": segment_index + 1,
                "segment_count": segment_count,
                "current_chain": flight_chain_json(current)
            }),
        );
        return;
    }

    for flight in candidates {
        request_info(
            request_trace,
            "candidate_considered",
            json!({
                "path_index": path_index,
                "segment_index": segment_index + 1,
                "segment_count": segment_count,
                "candidate": flight_json(flight),
                "current_chain": flight_chain_json(current)
            }),
        );

        match validate_connection(path, segment_index, current, flight) {
            Ok(()) => {
                request_info(
                    request_trace,
                    "candidate_accepted",
                    json!({
                        "path_index": path_index,
                        "segment_index": segment_index + 1,
                        "segment_count": segment_count,
                        "candidate": flight_json(flight)
                    }),
                );
                current.push(flight);
                build_combinations(
                    path,
                    flights,
                    dep_date,
                    segment_index + 1,
                    current,
                    results,
                    request_trace,
                    path_index,
                );
                current.pop();
            }
            Err(reason) => {
                request_info(
                    request_trace,
                    "candidate_rejected",
                    json!({
                        "path_index": path_index,
                        "segment_index": segment_index + 1,
                        "segment_count": segment_count,
                        "candidate": flight_json(flight),
                        "reason": reason
                    }),
                );
            }
        }
    }
}

fn collect_segment_candidates<'a>(
    path: &PathResult,
    segment: &Segment,
    flights: &'a HashMap<String, Flightcore>,
    dep_date: NaiveDate,
    current: &[&'a Flightcore],
    segment_index: usize,
    request_trace: &str,
    path_index: usize,
    segment_count: usize,
) -> Option<Vec<&'a Flightcore>> {
    let from = record_id_code(&segment.from)?;
    let to = record_id_code(&segment.to)?;
    let route_flights = segment.flights.clone();
    let lookup_dates = lookup_dates_for_segment(path, current, dep_date, segment_index);
    let lookup_keys = lookup_keys_for_segment(&route_flights, &from, &to, &lookup_dates);
    let hits = lookup_keys
        .iter()
        .filter_map(|key| flights.get(key).map(|flight| (key.clone(), flight)))
        .collect::<Vec<_>>();

    request_info(
        request_trace,
        "segment_route",
        json!({
            "path_index": path_index,
            "segment_index": segment_index + 1,
            "segment_count": segment_count,
            "from": from,
            "to": to,
            "route_flights": route_flights,
            "companies": segment.companies,
            "mct_minutes": segment.mct,
            "effective_mct_minutes": segment.mct.unwrap_or(DEFAULT_MCT_MINUTES)
        }),
    );
    request_info(
        request_trace,
        "segment_lookup_window",
        json!({
            "path_index": path_index,
            "segment_index": segment_index + 1,
            "segment_count": segment_count,
            "lookup_dates": lookup_dates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "current_chain": flight_chain_json(current),
            "window": connection_window_json(path, current, segment_index)
        }),
    );
    request_info(
        request_trace,
        "segment_lookup_keys",
        json!({
            "path_index": path_index,
            "segment_index": segment_index + 1,
            "segment_count": segment_count,
            "keys": lookup_keys
        }),
    );
    request_info(
        request_trace,
        "segment_lookup_hits",
        json!({
            "path_index": path_index,
            "segment_index": segment_index + 1,
            "segment_count": segment_count,
            "hits": hits
                .iter()
                .map(|(key, flight)| {
                    json!({
                        "key": key,
                        "flight": flight_json(flight)
                    })
                })
                .collect::<Vec<_>>()
        }),
    );

    let candidates = hits
        .into_iter()
        .map(|(_, flight)| flight)
        .filter(|flight| validate_connection(path, segment_index, current, flight).is_ok())
        .collect::<Vec<_>>();

    request_info(
        request_trace,
        "segment_candidates",
        json!({
            "path_index": path_index,
            "segment_index": segment_index + 1,
            "segment_count": segment_count,
            "candidates": flight_list_json(&candidates)
        }),
    );

    Some(candidates)
}

fn lookup_dates_for_segment(
    path: &PathResult,
    current: &[&Flightcore],
    dep_date: NaiveDate,
    segment_index: usize,
) -> Vec<NaiveDate> {
    if segment_index == 0 {
        return vec![dep_date];
    }

    let (earliest_departure, latest_departure, _) =
        connection_bounds(path, segment_index, current).unwrap();
    let mut dates = Vec::new();
    let mut date = earliest_departure.date_naive();
    let latest_date = latest_departure.date_naive();

    while date <= latest_date {
        dates.push(date);
        date = date.succ_opt().unwrap();
    }

    dates
}

fn lookup_keys_for_segment(
    route_flights: &[String],
    from: &str,
    to: &str,
    dates: &[NaiveDate],
) -> Vec<String> {
    let mut keys = Vec::new();

    for route_flight in route_flights {
        let Some((company, flight_id)) = route_flight.split_once('_') else {
            continue;
        };
        for date in dates {
            keys.push(flight_storage_key(company, flight_id, from, to, *date));
        }
    }

    keys
}

fn validate_connection(
    path: &PathResult,
    segment_index: usize,
    current: &[&Flightcore],
    next_flight: &Flightcore,
) -> Result<(), String> {
    if segment_index == 0 {
        return Ok(());
    }

    let previous_flight = current[segment_index - 1];
    let (earliest_departure, latest_departure, minimum_connection_minutes) =
        connection_bounds(path, segment_index, current).unwrap();

    if next_flight.dep_local() < &earliest_departure {
        return Err(format!(
            "departure {} is earlier than minimum connection {} after previous flight {} with mct={}m",
            next_flight.dep_local().to_rfc3339(),
            earliest_departure.to_rfc3339(),
            format_flight(previous_flight),
            minimum_connection_minutes
        ));
    }

    if next_flight.dep_local() > &latest_departure {
        return Err(format!(
            "departure {} is later than connection window end {} after previous flight {}",
            next_flight.dep_local().to_rfc3339(),
            latest_departure.to_rfc3339(),
            format_flight(previous_flight)
        ));
    }

    Ok(())
}

fn connection_bounds(
    path: &PathResult,
    segment_index: usize,
    current: &[&Flightcore],
) -> Option<(
    chrono::DateTime<chrono_tz::Tz>,
    chrono::DateTime<chrono_tz::Tz>,
    i64,
)> {
    if segment_index == 0 {
        return None;
    }

    let previous_flight = current.get(segment_index - 1)?;
    let minimum_connection_minutes = path.segments[segment_index - 1]
        .mct
        .unwrap_or(DEFAULT_MCT_MINUTES)
        .max(0);
    let earliest_departure =
        previous_flight.arr_local().clone() + Duration::minutes(minimum_connection_minutes);
    let latest_departure =
        previous_flight.arr_local().clone() + Duration::hours(MAX_CONNECTION_WINDOW_HOURS);

    Some((
        earliest_departure,
        latest_departure,
        minimum_connection_minutes,
    ))
}

fn log_route_result(request_trace: &str, path_index: usize, path: &PathResult) {
    request_info(
        request_trace,
        "route_result",
        json!({
            "path_index": path_index,
            "airports": airport_path_json(&path.airports),
            "total_distance": path.total_dist,
            "circuity": path.circuity,
            "segments": path
                .segments
                .iter()
                .enumerate()
                .map(|(segment_index, segment)| {
                    json!({
                        "segment_index": segment_index + 1,
                        "from": record_id_code(&segment.from),
                        "to": record_id_code(&segment.to),
                        "flights": segment.flights,
                        "companies": segment.companies,
                        "mct_minutes": segment.mct,
                        "effective_mct_minutes": segment.mct.unwrap_or(DEFAULT_MCT_MINUTES),
                        "distance": segment.distance
                    })
                })
                .collect::<Vec<_>>()
        }),
    );
}

fn request_trace_id(origin: &str, destination: &str, dep_date: &str) -> String {
    format!(
        "IB-{}-{}-{}-{}",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
        origin.trim().to_uppercase(),
        destination.trim().to_uppercase(),
        dep_date.trim()
    )
}

fn request_info(request_trace: &str, event: &str, data: Value) {
    write_request_log("INFO", request_trace, event, data);
}

fn request_warn(request_trace: &str, event: &str, data: Value) {
    write_request_log("WARN", request_trace, event, data);
}

fn request_error(request_trace: &str, event: &str, data: Value) {
    write_request_log("ERROR", request_trace, event, data);
}

fn write_request_log(level: &str, request_trace: &str, event: &str, data: Value) {
    let entry = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "level": level,
        "request_trace": request_trace,
        "event": event,
        "data": data
    });

    if let Err(error) = append_request_log(&entry) {
        eprintln!("failed to write request log entry: {}", error);
    }
}

fn append_request_log(entry: &Value) -> std::io::Result<()> {
    create_dir_all("./log")?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(REQUEST_LOG_PATH)?;
    writeln!(file, "{}", entry)
}

fn airport_path_json(airports: &[RecordId]) -> Value {
    Value::Array(
        airports
            .iter()
            .map(|airport| {
                record_id_code(airport)
                    .map(Value::String)
                    .unwrap_or(Value::String("<invalid>".to_string()))
            })
            .collect(),
    )
}

fn flight_list_json(flights: &[&Flightcore]) -> Value {
    Value::Array(flights.iter().map(|flight| flight_json(flight)).collect())
}

fn flight_chain_json(flights: &[&Flightcore]) -> Value {
    Value::Array(flights.iter().map(|flight| flight_json(flight)).collect())
}

fn flight_json(flight: &Flightcore) -> Value {
    json!({
        "company": flight.company(),
        "flight_id": flight.flight_id(),
        "origin": flight.origin().as_str(),
        "destination": flight.destination().as_str(),
        "departure": flight.dep_local().to_rfc3339(),
        "arrival": flight.arr_local().to_rfc3339(),
        "block_time_minutes": flight.block_time_minutes()
    })
}

fn connection_window_json(
    path: &PathResult,
    current: &[&Flightcore],
    segment_index: usize,
) -> Value {
    if segment_index == 0 {
        return json!({
            "mode": "requested_departure_date_only"
        });
    }

    let previous_flight = current[segment_index - 1];
    let (earliest_departure, latest_departure, minimum_connection_minutes) =
        connection_bounds(path, segment_index, current).unwrap();

    json!({
        "mode": "previous_arrival_window",
        "previous_flight": flight_json(previous_flight),
        "minimum_connection_minutes": minimum_connection_minutes,
        "earliest_departure": earliest_departure.to_rfc3339(),
        "latest_departure": latest_departure.to_rfc3339()
    })
}

fn format_flight(flight: &Flightcore) -> String {
    format!(
        "{}_{} {} -> {} dep={} arr={}",
        flight.company(),
        flight.flight_id(),
        flight.origin().as_str(),
        flight.destination().as_str(),
        flight.dep_local().to_rfc3339(),
        flight.arr_local().to_rfc3339()
    )
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
