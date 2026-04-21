use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;

use actix_web::{get, web, HttpResponse};
use chrono::{Duration, NaiveDate, Utc};
use chrono_tz::Tz;
use serde::Serialize;
use serde_json::{json, Value};
use surrealdb_types::{RecordId, RecordIdKey};

use crate::domain::airport::{Airport, AirportCode};
use crate::domain::flight::Flightcore;
use crate::domain::itinerary::Itinerary;
use crate::domain::mct::{
    AirportMctData, AirportMctRecord, GlobalMctData, DEFAULT_AIRPORT_MCT_MINUTES,
};
use crate::memory::core::{flight_storage_key, WebData};
use crate::runtime_paths;
use crate::Infrastructure::db::model::flight_row::FlightDesignatorRow;
use crate::Infrastructure::db::repository::route_repo::{self, PathResult, Segment};

const DEFAULT_MAX_TRANSPORTS: u8 = 0;
const MAX_CIRCUITY: f64 = 2.0;
const DEFAULT_MCT_MINUTES: i64 = DEFAULT_AIRPORT_MCT_MINUTES as i64;
const MAX_CONNECTION_WINDOW_HOURS: i64 = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EffectiveMctSource {
    StructuredRecord,
    DefaultConstant,
}

#[derive(Clone, Debug)]
struct EffectiveMct {
    minutes: i64,
    source: EffectiveMctSource,
    rule_description: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MctFieldPriority {
    Station = 1,
    Aircraft = 2,
    Terminal = 3,
    FlightNumber = 4,
    Airline = 5,
    PreviousNextStation = 6,
    PreviousNextCountryState = 7,
    PreviousNextRegion = 8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MctSpecificity {
    highest_priority: u8,
    priority_count: usize,
    field_count: usize,
    station_pair_count: usize,
}

#[derive(Serialize)]
struct FlightDesignatorResponse {
    company: String,
    flight_id: String,
    operational_suffix: Option<String>,
}

#[derive(Serialize)]
struct FlightInfoResponse {
    company: String,
    flight_id: String,
    origin: String,
    destination: String,
    departure: String,
    arrival: String,
    block_time_minutes: u32,
    departure_terminal: Option<String>,
    arrival_terminal: Option<String>,
    operating_company: String,
    operating_flight_id: String,
    operating_suffix: Option<String>,
    duplicate_flights: Vec<FlightDesignatorResponse>,
    joint_operation_companies: Vec<String>,
    meal_service_note: Option<String>,
    in_flight_service_info: Option<String>,
    electronic_ticketing_info: Option<String>,
}

#[derive(Serialize)]
struct ItineraryResponse {
    airports: Vec<String>,
    flights: Vec<FlightInfoResponse>,
    total_flight_time_minutes: u32,
    total_travel_time_minutes: u32,
    transfer_time_minutes: u32,
    transfer_count: u32,
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
    let raw_transport = request.get_transport();
    let request_trace = request_trace_id(&raw_origin, &raw_destination, &dep_date_raw);

    request_info(
        &request_trace,
        "request_received",
        json!({
            "raw_origin": raw_origin,
            "raw_destination": raw_destination,
            "raw_dep_date": dep_date_raw,
            "raw_transport": raw_transport
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

    let max_transports = match parse_transport_limit(raw_transport.as_deref()) {
        Ok(value) => value,
        Err(message) => {
            request_warn(
                &request_trace,
                "request_rejected_invalid_transport",
                json!({
                    "transport": raw_transport,
                    "message": message
                }),
            );
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid request",
                "message": message
            })));
        }
    };
    let max_hops = match max_hops_for_transport_limit(max_transports) {
        Ok(value) => value,
        Err(message) => {
            request_warn(
                &request_trace,
                "request_rejected_invalid_transport",
                json!({
                    "transport": raw_transport,
                    "message": message
                }),
            );
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid request",
                "message": message
            })));
        }
    };

    request_info(
        &request_trace,
        "request_normalized",
        json!({
            "origin": origin,
            "destination": destination,
            "dep_date": dep_date.to_string(),
            "transport": max_transports
        }),
    );

    let airport_cache = data.airports();
    let origin_loaded = airport_cache.contains_key(&origin);
    let destination_loaded = airport_cache.contains_key(&destination);
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
            "transport": max_transports,
            "max_hops": max_hops,
            "max_circuity": MAX_CIRCUITY
        }),
    );
    let mut paths = match route_repo::find_paths(
        data.database(),
        origin.as_str(),
        destination.as_str(),
        max_hops,
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

    let flights = data.flights();
    let airport_mct = data.airport_mct();
    let global_mct = data.global_mct();
    request_info(
        &request_trace,
        "flight_lookup_strategy",
        json!({
            "strategy": "hashmap_by_company_flight_origin_destination_dep_date",
            "flight_count": flights.len(),
            "airport_mct_airport_count": airport_mct.len(),
            "airport_mct_record_count": airport_mct
                .values()
                .map(|payload| payload.mct_records.len())
                .sum::<usize>(),
            "max_connection_window_hours": MAX_CONNECTION_WINDOW_HOURS,
            "global_mct_record_count": global_mct.mct_records.len(),
            "global_connection_building_filter_count": global_mct.connection_building_filters.len()
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
            &flights,
            &airport_cache,
            &airport_mct,
            &global_mct,
            dep_date,
            &request_trace,
            path_index + 1,
        );
        for combination in combinations {
            let expanded_segments = expand_itinerary_segments(&combination, &flights);
            let effective_transport_count = transfer_count(&expanded_segments);
            if effective_transport_count > u32::from(max_transports) {
                request_info(
                    &request_trace,
                    "itinerary_skipped_transport_limit",
                    json!({
                        "path_index": path_index + 1,
                        "flight_chain": flight_chain_json(&combination),
                        "transport_limit": max_transports,
                        "effective_transport_count": effective_transport_count
                    }),
                );
                continue;
            }

            let itinerary = ItineraryResponse {
                airports: airports_for_segments(&expanded_segments)
                    .unwrap_or_else(|| airports.clone()),
                flights: expanded_segments
                    .iter()
                    .map(|flight| FlightInfoResponse {
                        company: flight.company().to_string(),
                        flight_id: flight.flight_id().to_string(),
                        origin: flight.origin().as_str().to_string(),
                        destination: flight.destination().as_str().to_string(),
                        departure: flight.dep_local().to_rfc3339(),
                        arrival: flight.arr_local().to_rfc3339(),
                        block_time_minutes: flight.block_time_minutes(),
                        departure_terminal: flight.departure_terminal().map(ToOwned::to_owned),
                        arrival_terminal: flight.arrival_terminal().map(ToOwned::to_owned),
                        operating_company: flight.operating_designator().company.clone(),
                        operating_flight_id: flight.operating_designator().flight_number.clone(),
                        operating_suffix: flight.operating_designator().operational_suffix.clone(),
                        duplicate_flights: flight
                            .duplicate_designators()
                            .iter()
                            .map(flight_designator_response)
                            .collect(),
                        joint_operation_companies: flight
                            .joint_operation_airline_designators()
                            .to_vec(),
                        meal_service_note: flight.meal_service_note().map(ToOwned::to_owned),
                        in_flight_service_info: flight
                            .in_flight_service_info()
                            .map(ToOwned::to_owned),
                        electronic_ticketing_info: flight
                            .electronic_ticketing_info()
                            .map(ToOwned::to_owned),
                    })
                    .collect(),
                total_flight_time_minutes: expanded_segments
                    .iter()
                    .map(|flight| flight.block_time_minutes())
                    .sum(),
                total_travel_time_minutes: total_travel_time_minutes(&expanded_segments),
                transfer_time_minutes: transfer_time_minutes(&expanded_segments),
                transfer_count: transfer_count(&expanded_segments),
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
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    global_mct: &GlobalMctData,
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
        airports,
        airport_mct,
        global_mct,
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
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    global_mct: &GlobalMctData,
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
        airports,
        airport_mct,
        global_mct,
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

        match validate_connection(
            path,
            airports,
            airport_mct,
            global_mct,
            segment_index,
            current,
            flight,
        ) {
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
                    airports,
                    airport_mct,
                    global_mct,
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
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    global_mct: &GlobalMctData,
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
    let lookup_dates = lookup_dates_for_segment(current, dep_date, segment_index);
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
            "mct_strategy": "candidate_specific_mct_record_evaluation"
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
            "window": connection_window_json(current, segment_index)
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
        .filter(|flight| {
            validate_connection(
                path,
                airports,
                airport_mct,
                global_mct,
                segment_index,
                current,
                flight,
            )
            .is_ok()
        })
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
    current: &[&Flightcore],
    dep_date: NaiveDate,
    segment_index: usize,
) -> Vec<NaiveDate> {
    if segment_index == 0 {
        return vec![dep_date];
    }

    let (earliest_departure, latest_departure) =
        connection_search_window(current, segment_index).unwrap();
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
    _path: &PathResult,
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    global_mct: &GlobalMctData,
    segment_index: usize,
    current: &[&Flightcore],
    next_flight: &Flightcore,
) -> Result<(), String> {
    if segment_index == 0 {
        return Ok(());
    }

    let previous_flight = current[segment_index - 1];
    if same_operating_flight(previous_flight, next_flight) {
        return Err(format!(
            "same-flight continuation {} -> {} should be matched by a synthesized through-flight, not a transfer",
            format_flight(previous_flight),
            format_flight(next_flight)
        ));
    }

    let (earliest_departure, latest_departure, effective_mct) =
        connection_bounds(
            airports,
            airport_mct,
            global_mct,
            segment_index,
            current,
            next_flight,
        )
        .unwrap();

    if next_flight.dep_local() < &earliest_departure {
        return Err(format!(
            "departure {} is earlier than minimum connection {} after previous flight {} with mct={}m [{}: {}]",
            next_flight.dep_local().to_rfc3339(),
            earliest_departure.to_rfc3339(),
            format_flight(previous_flight),
            effective_mct.minutes,
            effective_mct_source_label(effective_mct.source),
            effective_mct.rule_description
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

fn same_operating_flight(left: &Flightcore, right: &Flightcore) -> bool {
    left.company() == right.company() && left.flight_id() == right.flight_id()
}

fn expand_itinerary_segments<'a>(
    flights: &[&'a Flightcore],
    all_flights: &'a HashMap<String, Flightcore>,
) -> Vec<&'a Flightcore> {
    let mut expanded = Vec::new();

    for flight in flights {
        expanded.extend(expand_flight_segments(flight, all_flights));
    }

    expanded
}

fn expand_flight_segments<'a>(
    flight: &'a Flightcore,
    all_flights: &'a HashMap<String, Flightcore>,
) -> Vec<&'a Flightcore> {
    let candidates = all_flights
        .values()
        .filter(|candidate| is_subflight_candidate(flight, candidate))
        .collect::<Vec<_>>();

    let Some(path) = find_same_flight_path(flight, &candidates) else {
        return vec![flight];
    };

    let mut expanded = Vec::new();
    for subflight in path {
        expanded.extend(expand_flight_segments(subflight, all_flights));
    }
    expanded
}

fn is_subflight_candidate(parent: &Flightcore, candidate: &Flightcore) -> bool {
    same_operating_flight(parent, candidate)
        && !same_flight_instance(parent, candidate)
        && candidate.dep_local() >= parent.dep_local()
        && candidate.arr_local() <= parent.arr_local()
        && (candidate.origin() != parent.origin()
            || candidate.destination() != parent.destination())
}

fn same_flight_instance(left: &Flightcore, right: &Flightcore) -> bool {
    same_operating_flight(left, right)
        && left.origin() == right.origin()
        && left.destination() == right.destination()
        && left.dep_local() == right.dep_local()
        && left.arr_local() == right.arr_local()
}

fn find_same_flight_path<'a>(
    parent: &'a Flightcore,
    candidates: &[&'a Flightcore],
) -> Option<Vec<&'a Flightcore>> {
    let mut ordered = candidates.to_vec();
    ordered.sort_by(|left, right| {
        left.dep_local()
            .cmp(right.dep_local())
            .then_with(|| left.arr_local().cmp(right.arr_local()))
            .then_with(|| left.origin().as_str().cmp(right.origin().as_str()))
            .then_with(|| {
                left.destination()
                    .as_str()
                    .cmp(right.destination().as_str())
            })
    });

    let mut path = Vec::new();
    let mut visited = HashSet::new();
    if search_same_flight_path(parent, &ordered, &mut path, &mut visited) {
        Some(path)
    } else {
        None
    }
}

fn search_same_flight_path<'a>(
    parent: &'a Flightcore,
    candidates: &[&'a Flightcore],
    path: &mut Vec<&'a Flightcore>,
    visited: &mut HashSet<String>,
) -> bool {
    let current_origin = path
        .last()
        .map(|flight| flight.destination().as_str())
        .unwrap_or(parent.origin().as_str());
    let earliest_departure = path
        .last()
        .map(|flight| flight.arr_local())
        .unwrap_or(parent.dep_local());

    for candidate in candidates {
        let signature = flight_signature(candidate);
        if visited.contains(&signature) {
            continue;
        }
        if candidate.origin().as_str() != current_origin {
            continue;
        }
        if path.is_empty() && candidate.dep_local() != parent.dep_local() {
            continue;
        }
        if candidate.dep_local() < earliest_departure {
            continue;
        }

        path.push(candidate);
        visited.insert(signature.clone());

        let is_complete = candidate.destination() == parent.destination()
            && candidate.arr_local() == parent.arr_local()
            && path.len() >= 2;
        if is_complete || search_same_flight_path(parent, candidates, path, visited) {
            return true;
        }

        path.pop();
        visited.remove(&signature);
    }

    false
}

fn airports_for_segments(flights: &[&Flightcore]) -> Option<Vec<String>> {
    let first = flights.first()?;
    let mut airports = vec![first.origin().as_str().to_string()];
    airports.extend(
        flights
            .iter()
            .map(|flight| flight.destination().as_str().to_string()),
    );
    Some(airports)
}

fn transfer_count(flights: &[&Flightcore]) -> u32 {
    transport_count(flights).saturating_sub(1)
}

fn transport_count(flights: &[&Flightcore]) -> u32 {
    if flights.is_empty() {
        return 0;
    }

    1 + flights
        .windows(2)
        .filter(|pair| !same_operating_flight(pair[0], pair[1]))
        .count() as u32
}

fn transfer_time_minutes(flights: &[&Flightcore]) -> u32 {
    flights
        .windows(2)
        .filter(|pair| !same_operating_flight(pair[0], pair[1]))
        .map(|pair| connection_minutes(pair[0], pair[1]))
        .sum()
}

fn total_travel_time_minutes(flights: &[&Flightcore]) -> u32 {
    let Some(first) = flights.first() else {
        return 0;
    };
    let Some(last) = flights.last() else {
        return 0;
    };

    let departure = first.dep_local().with_timezone(&Utc);
    let arrival = last.arr_local().with_timezone(&Utc);
    arrival
        .signed_duration_since(departure)
        .num_minutes()
        .max(0) as u32
}

fn connection_minutes(previous: &Flightcore, next: &Flightcore) -> u32 {
    let previous_arrival = previous.arr_local().with_timezone(&Utc);
    let next_departure = next.dep_local().with_timezone(&Utc);
    next_departure
        .signed_duration_since(previous_arrival)
        .num_minutes()
        .max(0) as u32
}

fn connection_search_window(
    current: &[&Flightcore],
    segment_index: usize,
) -> Option<(
    chrono::DateTime<chrono_tz::Tz>,
    chrono::DateTime<chrono_tz::Tz>,
)> {
    if segment_index == 0 {
        return None;
    }

    let previous_flight = current.get(segment_index - 1)?;
    let earliest_departure = previous_flight.arr_local().clone();
    let latest_departure =
        previous_flight.arr_local().clone() + Duration::hours(MAX_CONNECTION_WINDOW_HOURS);

    Some((earliest_departure, latest_departure))
}

fn connection_bounds(
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    global_mct: &GlobalMctData,
    segment_index: usize,
    current: &[&Flightcore],
    next_flight: &Flightcore,
) -> Option<(
    chrono::DateTime<chrono_tz::Tz>,
    chrono::DateTime<chrono_tz::Tz>,
    EffectiveMct,
)> {
    if segment_index == 0 {
        return None;
    }

    let previous_flight = current.get(segment_index - 1)?;
    let effective_mct = resolve_effective_mct_with_global(
        airports,
        airport_mct,
        global_mct,
        previous_flight,
        next_flight,
    );
    let earliest_departure =
        previous_flight.arr_local().clone() + Duration::minutes(effective_mct.minutes);
    let latest_departure =
        previous_flight.arr_local().clone() + Duration::hours(MAX_CONNECTION_WINDOW_HOURS);

    Some((earliest_departure, latest_departure, effective_mct))
}

#[cfg(test)]
fn resolve_effective_mct(
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> EffectiveMct {
    resolve_effective_mct_with_global(
        airports,
        airport_mct,
        &GlobalMctData::default(),
        previous_flight,
        next_flight,
    )
}

fn resolve_effective_mct_with_global(
    airports: &HashMap<String, Airport>,
    airport_mct: &HashMap<String, AirportMctData>,
    global_mct: &GlobalMctData,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> EffectiveMct {
    let candidate_airports = candidate_transfer_airports(airports, previous_flight, next_flight);
    let status = determine_connection_status(airports, previous_flight, next_flight);
    let connection_building_filters = global_mct
        .connection_building_filters
        .iter()
        .chain(
            candidate_airports
                .iter()
                .filter_map(|airport| airport_mct.get(airport.id().as_str()))
                .flat_map(|payload| payload.connection_building_filters.iter()),
        )
        .collect::<Vec<_>>();

    if let Some(status) = status.as_deref() {
        let mut matching_records = candidate_airports
            .iter()
            .filter_map(|airport| airport_mct.get(airport.id().as_str()))
            .flat_map(|payload| payload.mct_records.iter())
            .chain(global_mct.mct_records.iter())
            .filter(|record| {
                matches_mct_record(
                    record,
                    airports,
                    previous_flight,
                    next_flight,
                    status,
                    &connection_building_filters,
                )
            })
            .collect::<Vec<_>>();

        matching_records.sort_by(|left, right| {
            mct_specificity(right)
                .cmp(&mct_specificity(left))
                .then_with(|| right.suppression_indicator.cmp(&left.suppression_indicator))
        });

        let suppression_records = matching_records
            .iter()
            .copied()
            .filter(|record| record.suppression_indicator)
            .collect::<Vec<_>>();

        for record in matching_records {
            if record.suppression_indicator {
                continue;
            }
            if suppression_records.iter().any(|suppression| {
                suppression_applies_to_record(suppression, record)
            }) {
                continue;
            }
            if let Some(minutes) = parse_mct_minutes(record.time.as_deref()) {
                return EffectiveMct {
                    minutes,
                    source: EffectiveMctSource::StructuredRecord,
                    rule_description: format!("matched {}", describe_mct_record(record)),
                };
            }
        }
    }

    EffectiveMct {
        minutes: DEFAULT_MCT_MINUTES,
        source: EffectiveMctSource::DefaultConstant,
        rule_description: format!("default {}m fallback", DEFAULT_MCT_MINUTES),
    }
}

fn candidate_transfer_airports<'a>(
    airports: &'a HashMap<String, Airport>,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> Vec<&'a Airport> {
    let mut result = Vec::new();

    if let Some(arrival_airport) = airports.get(previous_flight.destination().as_str()) {
        result.push(arrival_airport);
    }

    if previous_flight.destination().as_str() != next_flight.origin().as_str() {
        if let Some(departure_airport) = airports.get(next_flight.origin().as_str()) {
            result.push(departure_airport);
        }
    }

    result
}

fn determine_connection_status(
    airports: &HashMap<String, Airport>,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> Option<String> {
    let inbound = leg_status(
        airports,
        previous_flight.origin().as_str(),
        previous_flight.destination().as_str(),
    )?;
    let outbound = leg_status(
        airports,
        next_flight.origin().as_str(),
        next_flight.destination().as_str(),
    )?;
    Some(format!("{inbound}{outbound}"))
}

fn leg_status(
    airports: &HashMap<String, Airport>,
    origin: &str,
    destination: &str,
) -> Option<char> {
    let origin_country = airports.get(origin)?.country()?;
    let destination_country = airports.get(destination)?.country()?;
    Some(if origin_country == destination_country {
        'D'
    } else {
        'I'
    })
}

fn matches_mct_record(
    record: &AirportMctRecord,
    airports: &HashMap<String, Airport>,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
    status: &str,
    connection_building_filters: &[&crate::domain::mct::ConnectionBuildingFilter],
) -> bool {
    record.status == status
        && matches_station_scope(record, previous_flight, next_flight)
        && matches_previous_next_scope(record, airports, previous_flight, next_flight)
        && matches_carrier_scope(record, previous_flight, next_flight)
        && matches_flight_number_scope(record, previous_flight, next_flight)
        && matches_terminal_scope(record, previous_flight, next_flight)
        && matches_aircraft_scope(record)
        && matches_effective_dates(record, next_flight)
        && matches_suppression_scope(record)
        && matches_connection_building_filter(
            record,
            previous_flight,
            next_flight,
            connection_building_filters,
        )
}

fn matches_station_scope(
    record: &AirportMctRecord,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> bool {
    record
        .arrival_station
        .as_deref()
        .is_none_or(|station| station == previous_flight.destination().as_str())
        && record
            .departure_station
            .as_deref()
            .is_none_or(|station| station == next_flight.origin().as_str())
}

fn matches_previous_next_scope(
    record: &AirportMctRecord,
    airports: &HashMap<String, Airport>,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> bool {
    if record.previous_region.is_some() || record.next_region.is_some() {
        return false;
    }

    record
        .previous_station
        .as_deref()
        .is_none_or(|station| station == previous_flight.origin().as_str())
        && record
            .next_station
            .as_deref()
            .is_none_or(|station| station == next_flight.destination().as_str())
        && record.previous_country.as_deref().is_none_or(|country| {
            airports
                .get(previous_flight.origin().as_str())
                .and_then(|airport| airport.country())
                == Some(country)
        })
        && record.previous_state.as_deref().is_none_or(|state| {
            airports
                .get(previous_flight.origin().as_str())
                .and_then(|airport| airport.state())
                == Some(state)
        })
        && record.next_country.as_deref().is_none_or(|country| {
            airports
                .get(next_flight.destination().as_str())
                .and_then(|airport| airport.country())
                == Some(country)
        })
        && record.next_state.as_deref().is_none_or(|state| {
            airports
                .get(next_flight.destination().as_str())
                .and_then(|airport| airport.state())
                == Some(state)
        })
}

fn matches_carrier_scope(
    record: &AirportMctRecord,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> bool {
    matches_flight_carrier_scope(
        previous_flight,
        record.arrival_carrier.as_deref(),
        record.arrival_codeshare_indicator,
        record.arrival_codeshare_operating_carrier.as_deref(),
    ) && matches_flight_carrier_scope(
        next_flight,
        record.departure_carrier.as_deref(),
        record.departure_codeshare_indicator,
        record.departure_codeshare_operating_carrier.as_deref(),
    )
}

fn matches_flight_carrier_scope(
    flight: &Flightcore,
    marketing_carrier: Option<&str>,
    codeshare_indicator: bool,
    operating_carrier: Option<&str>,
) -> bool {
    if let Some(marketing_carrier) = marketing_carrier {
        if flight.company() != marketing_carrier {
            return false;
        }
        if !codeshare_indicator && operating_carrier.is_none() && is_codeshare_flight(flight) {
            return false;
        }
    } else if codeshare_indicator && !is_codeshare_flight(flight) {
        return false;
    }

    operating_carrier.is_none_or(|carrier| {
        is_codeshare_flight(flight) && flight.operating_designator().company == carrier
    })
}

fn is_codeshare_flight(flight: &Flightcore) -> bool {
    flight.company() != flight.operating_designator().company
        || flight.flight_id() != flight.operating_designator().flight_number
}

fn matches_flight_number_scope(
    record: &AirportMctRecord,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> bool {
    matches_flight_number_range(
        previous_flight.flight_id(),
        record.arrival_flight_number_range_start.as_deref(),
        record.arrival_flight_number_range_end.as_deref(),
    ) && matches_flight_number_range(
        next_flight.flight_id(),
        record.departure_flight_number_range_start.as_deref(),
        record.departure_flight_number_range_end.as_deref(),
    )
}

fn matches_flight_number_range(
    flight_number: &str,
    start: Option<&str>,
    end: Option<&str>,
) -> bool {
    let (Some(start), Some(end)) = (start, end) else {
        return true;
    };
    let Ok(flight_number) = flight_number.parse::<u32>() else {
        return false;
    };
    let Ok(start) = start.parse::<u32>() else {
        return false;
    };
    let Ok(end) = end.parse::<u32>() else {
        return false;
    };
    (start..=end).contains(&flight_number)
}

fn matches_terminal_scope(
    record: &AirportMctRecord,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
) -> bool {
    record
        .arrival_terminal
        .as_deref()
        .is_none_or(|terminal| previous_flight.arrival_terminal() == Some(terminal))
        && record
            .departure_terminal
            .as_deref()
            .is_none_or(|terminal| next_flight.departure_terminal() == Some(terminal))
}

fn matches_aircraft_scope(record: &AirportMctRecord) -> bool {
    record.arrival_aircraft_type.is_none()
        && record.arrival_aircraft_body.is_none()
        && record.departure_aircraft_type.is_none()
        && record.departure_aircraft_body.is_none()
}

fn matches_effective_dates(record: &AirportMctRecord, next_flight: &Flightcore) -> bool {
    let connection_date = next_flight.dep_local().date_naive();
    record
        .effective_from_local
        .as_deref()
        .is_none_or(|value| parse_effective_date(value).is_some_and(|date| date <= connection_date))
        && record.effective_to_local.as_deref().is_none_or(|value| {
            parse_effective_date(value).is_some_and(|date| date >= connection_date)
        })
}

fn parse_effective_date(value: &str) -> Option<NaiveDate> {
    if value.len() != 7 {
        return None;
    }

    let month = value[2..5].to_ascii_lowercase();
    let mut normalized = String::with_capacity(7);
    normalized.push_str(&value[0..2]);
    normalized.push_str(&month[..1].to_ascii_uppercase());
    normalized.push_str(&month[1..]);
    normalized.push_str(&value[5..7]);

    NaiveDate::parse_from_str(&normalized, "%d%b%y").ok()
}

fn matches_suppression_scope(record: &AirportMctRecord) -> bool {
    record.suppression_region.is_none()
        && record.suppression_country.is_none()
        && record.suppression_state.is_none()
}

fn matches_connection_building_filter(
    record: &AirportMctRecord,
    previous_flight: &Flightcore,
    next_flight: &Flightcore,
    connection_building_filters: &[&crate::domain::mct::ConnectionBuildingFilter],
) -> bool {
    if !record.requires_connection_building_filter {
        return true;
    }

    connection_building_filters.iter().any(|filter| {
        (filter.submitting_carrier == previous_flight.company()
            && filter
                .partner_carrier_codes
                .iter()
                .any(|partner| partner == next_flight.company()))
            || (filter.submitting_carrier == next_flight.company()
                && filter
                    .partner_carrier_codes
                    .iter()
                    .any(|partner| partner == previous_flight.company()))
    })
}

fn suppression_applies_to_record(
    suppression_record: &AirportMctRecord,
    candidate_record: &AirportMctRecord,
) -> bool {
    suppression_record.arrival_station == candidate_record.arrival_station
        && suppression_record.status == candidate_record.status
        && suppression_record.departure_station == candidate_record.departure_station
        && suppression_record.requires_connection_building_filter
            == candidate_record.requires_connection_building_filter
        && suppression_record.arrival_carrier == candidate_record.arrival_carrier
        && suppression_record.arrival_codeshare_indicator
            == candidate_record.arrival_codeshare_indicator
        && suppression_record.arrival_codeshare_operating_carrier
            == candidate_record.arrival_codeshare_operating_carrier
        && suppression_record.departure_carrier == candidate_record.departure_carrier
        && suppression_record.departure_codeshare_indicator
            == candidate_record.departure_codeshare_indicator
        && suppression_record.departure_codeshare_operating_carrier
            == candidate_record.departure_codeshare_operating_carrier
        && suppression_record.arrival_aircraft_type == candidate_record.arrival_aircraft_type
        && suppression_record.arrival_aircraft_body == candidate_record.arrival_aircraft_body
        && suppression_record.departure_aircraft_type == candidate_record.departure_aircraft_type
        && suppression_record.departure_aircraft_body == candidate_record.departure_aircraft_body
        && suppression_record.arrival_terminal == candidate_record.arrival_terminal
        && suppression_record.departure_terminal == candidate_record.departure_terminal
        && suppression_record.previous_country == candidate_record.previous_country
        && suppression_record.previous_station == candidate_record.previous_station
        && suppression_record.next_country == candidate_record.next_country
        && suppression_record.next_station == candidate_record.next_station
        && suppression_record.arrival_flight_number_range_start
            == candidate_record.arrival_flight_number_range_start
        && suppression_record.arrival_flight_number_range_end
            == candidate_record.arrival_flight_number_range_end
        && suppression_record.departure_flight_number_range_start
            == candidate_record.departure_flight_number_range_start
        && suppression_record.departure_flight_number_range_end
            == candidate_record.departure_flight_number_range_end
        && suppression_record.previous_state == candidate_record.previous_state
        && suppression_record.next_state == candidate_record.next_state
        && suppression_record.previous_region == candidate_record.previous_region
        && suppression_record.next_region == candidate_record.next_region
        && suppression_record.effective_from_local == candidate_record.effective_from_local
        && suppression_record.effective_to_local == candidate_record.effective_to_local
}

fn parse_mct_minutes(value: Option<&str>) -> Option<i64> {
    let value = value?;
    if value.len() != 4 {
        return None;
    }

    let hours = value[0..2].parse::<i64>().ok()?;
    let minutes = value[2..4].parse::<i64>().ok()?;
    Some(hours * 60 + minutes)
}

fn describe_mct_record(record: &AirportMctRecord) -> String {
    let time = record.time.as_deref().unwrap_or("suppressed");
    let arrival = record.arrival_station.as_deref().unwrap_or("***");
    let departure = record.departure_station.as_deref().unwrap_or("***");
    format!("MCT {} {} {}->{}", record.status, time, arrival, departure)
}

fn mct_specificity(record: &AirportMctRecord) -> MctSpecificity {
    let mut priorities = Vec::new();

    if record.arrival_station.is_some() || record.departure_station.is_some() {
        priorities.push(MctFieldPriority::Station);
    }
    if record.arrival_aircraft_type.is_some()
        || record.arrival_aircraft_body.is_some()
        || record.departure_aircraft_type.is_some()
        || record.departure_aircraft_body.is_some()
    {
        priorities.push(MctFieldPriority::Aircraft);
    }
    if record.arrival_terminal.is_some() || record.departure_terminal.is_some() {
        priorities.push(MctFieldPriority::Terminal);
    }
    if record.arrival_flight_number_range_start.is_some()
        || record.arrival_flight_number_range_end.is_some()
        || record.departure_flight_number_range_start.is_some()
        || record.departure_flight_number_range_end.is_some()
    {
        priorities.push(MctFieldPriority::FlightNumber);
    }
    if record.arrival_carrier.is_some()
        || record.departure_carrier.is_some()
        || record.arrival_codeshare_indicator
        || record.departure_codeshare_indicator
        || record.arrival_codeshare_operating_carrier.is_some()
        || record.departure_codeshare_operating_carrier.is_some()
        || record.requires_connection_building_filter
    {
        priorities.push(MctFieldPriority::Airline);
    }
    if record.previous_station.is_some() || record.next_station.is_some() {
        priorities.push(MctFieldPriority::PreviousNextStation);
    }
    if record.previous_country.is_some()
        || record.next_country.is_some()
        || record.previous_state.is_some()
        || record.next_state.is_some()
    {
        priorities.push(MctFieldPriority::PreviousNextCountryState);
    }
    if record.previous_region.is_some() || record.next_region.is_some() {
        priorities.push(MctFieldPriority::PreviousNextRegion);
    }

    let field_count = usize::from(record.arrival_station.is_some())
        + usize::from(record.departure_station.is_some())
        + usize::from(record.requires_connection_building_filter)
        + usize::from(record.arrival_carrier.is_some())
        + usize::from(record.arrival_codeshare_indicator)
        + usize::from(record.arrival_codeshare_operating_carrier.is_some())
        + usize::from(record.departure_carrier.is_some())
        + usize::from(record.departure_codeshare_indicator)
        + usize::from(record.departure_codeshare_operating_carrier.is_some())
        + usize::from(record.arrival_aircraft_type.is_some())
        + usize::from(record.arrival_aircraft_body.is_some())
        + usize::from(record.departure_aircraft_type.is_some())
        + usize::from(record.departure_aircraft_body.is_some())
        + usize::from(record.arrival_terminal.is_some())
        + usize::from(record.departure_terminal.is_some())
        + usize::from(record.previous_country.is_some())
        + usize::from(record.previous_station.is_some())
        + usize::from(record.next_country.is_some())
        + usize::from(record.next_station.is_some())
        + usize::from(record.arrival_flight_number_range_start.is_some())
        + usize::from(record.arrival_flight_number_range_end.is_some())
        + usize::from(record.departure_flight_number_range_start.is_some())
        + usize::from(record.departure_flight_number_range_end.is_some())
        + usize::from(record.previous_state.is_some())
        + usize::from(record.next_state.is_some())
        + usize::from(record.previous_region.is_some())
        + usize::from(record.next_region.is_some())
        + usize::from(record.effective_from_local.is_some())
        + usize::from(record.effective_to_local.is_some());

    MctSpecificity {
        highest_priority: priorities
            .iter()
            .map(|priority| *priority as u8)
            .max()
            .unwrap_or(0),
        priority_count: priorities.len(),
        field_count,
        station_pair_count: usize::from(record.arrival_station.is_some())
            + usize::from(record.departure_station.is_some()),
    }
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
                        "mct_strategy": "candidate_specific_mct_record_evaluation",
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
    let request_log_path = runtime_paths::request_log_file()?;
    runtime_paths::create_parent_dir(&request_log_path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(request_log_path)?;
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
        "block_time_minutes": flight.block_time_minutes(),
        "departure_terminal": flight.departure_terminal(),
        "arrival_terminal": flight.arrival_terminal(),
        "operating_company": flight.operating_designator().company.clone(),
        "operating_flight_id": flight.operating_designator().flight_number.clone(),
        "operating_suffix": flight.operating_designator().operational_suffix.clone(),
        "duplicate_flights": flight
            .duplicate_designators()
            .iter()
            .map(flight_designator_response)
            .collect::<Vec<_>>(),
        "joint_operation_companies": flight.joint_operation_airline_designators(),
        "meal_service_note": flight.meal_service_note(),
        "in_flight_service_info": flight.in_flight_service_info(),
        "electronic_ticketing_info": flight.electronic_ticketing_info()
    })
}

fn flight_designator_response(designator: &FlightDesignatorRow) -> FlightDesignatorResponse {
    FlightDesignatorResponse {
        company: designator.company.clone(),
        flight_id: designator.flight_number.clone(),
        operational_suffix: designator.operational_suffix.clone(),
    }
}

fn connection_window_json(current: &[&Flightcore], segment_index: usize) -> Value {
    if segment_index == 0 {
        return json!({
            "mode": "requested_departure_date_only"
        });
    }

    let previous_flight = current[segment_index - 1];
    let (earliest_departure, latest_departure) =
        connection_search_window(current, segment_index).unwrap();

    json!({
        "mode": "previous_arrival_lookup_window",
        "previous_flight": flight_json(previous_flight),
        "minimum_connection_minutes": "evaluated_per_candidate",
        "earliest_departure": earliest_departure.to_rfc3339(),
        "latest_departure": latest_departure.to_rfc3339()
    })
}

fn effective_mct_source_label(source: EffectiveMctSource) -> &'static str {
    match source {
        EffectiveMctSource::StructuredRecord => "mct_records",
        EffectiveMctSource::DefaultConstant => "default",
    }
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

fn flight_signature(flight: &Flightcore) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        flight.company(),
        flight.flight_id(),
        flight.origin().as_str(),
        flight.destination().as_str(),
        format_datetime_for_signature(flight.dep_local()),
        format_datetime_for_signature(flight.arr_local())
    )
}

fn format_datetime_for_signature(value: &chrono::DateTime<Tz>) -> String {
    value.to_rfc3339()
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

fn parse_transport_limit(raw_transport: Option<&str>) -> Result<u8, String> {
    let Some(raw_transport) = raw_transport
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(DEFAULT_MAX_TRANSPORTS);
    };

    let transport = raw_transport
        .parse::<u8>()
        .map_err(|_| "transport must be a non-negative integer".to_string())?;

    Ok(transport)
}

fn max_hops_for_transport_limit(transport: u8) -> Result<u8, String> {
    transport
        .checked_add(1)
        .ok_or_else(|| "transport must be less than 255".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::airport::{Airport, AirportCode};
    use crate::domain::flight::Flightcore;
    use crate::domain::mct::{
        AirportMctData, AirportMctRecord, ConnectionBuildingFilter, airport_default_mct_records,
    };
    use chrono::TimeZone;
    use std::collections::HashMap;

    #[test]
    fn expands_same_flight_stopover_into_physical_segments() {
        let flights = sample_same_flight_map();
        let through = flights
            .get("CA_897_PEK_GRU_2026-04-02")
            .expect("through-flight should exist");

        let segments = expand_flight_segments(through, &flights);

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].origin().as_str(), "PEK");
        assert_eq!(segments[0].destination().as_str(), "MAD");
        assert_eq!(segments[1].origin().as_str(), "MAD");
        assert_eq!(segments[1].destination().as_str(), "GRU");
        assert_eq!(transport_count(&segments), 1);
        assert_eq!(transfer_count(&segments), 0);
        assert_eq!(transfer_time_minutes(&segments), 0);
        assert_eq!(total_travel_time_minutes(&segments), 1500);
        assert_eq!(
            segments
                .iter()
                .map(|flight| flight.block_time_minutes())
                .sum::<u32>(),
            1365
        );
    }

    #[test]
    fn counts_transfers_between_different_flights_only() {
        let tz = chrono_tz::UTC;
        let first = Flightcore::new(
            "CA".to_string(),
            "123".to_string(),
            AirportCode::new("PEK").unwrap(),
            AirportCode::new("MUC").unwrap(),
            tz.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
            tz.with_ymd_and_hms(2026, 4, 2, 12, 0, 0).unwrap(),
            240,
            None,
            None,
            sample_designator("CA", "123"),
            vec![],
            vec![],
            None,
            None,
            None,
        );
        let second = Flightcore::new(
            "LH".to_string(),
            "456".to_string(),
            AirportCode::new("MUC").unwrap(),
            AirportCode::new("GRU").unwrap(),
            tz.with_ymd_and_hms(2026, 4, 2, 14, 30, 0).unwrap(),
            tz.with_ymd_and_hms(2026, 4, 2, 23, 0, 0).unwrap(),
            510,
            None,
            None,
            sample_designator("LH", "456"),
            vec![],
            vec![],
            None,
            None,
            None,
        );
        let itinerary = vec![&first, &second];

        assert_eq!(transport_count(&itinerary), 2);
        assert_eq!(transfer_count(&itinerary), 1);
        assert_eq!(transfer_time_minutes(&itinerary), 150);
        assert_eq!(total_travel_time_minutes(&itinerary), 900);
    }

    #[test]
    fn defaults_transport_limit_when_request_omits_it() {
        assert_eq!(parse_transport_limit(None).unwrap(), DEFAULT_MAX_TRANSPORTS);
        assert_eq!(
            parse_transport_limit(Some("   ")).unwrap(),
            DEFAULT_MAX_TRANSPORTS
        );
    }

    #[test]
    fn transport_limit_maps_to_hops() {
        assert_eq!(max_hops_for_transport_limit(0).unwrap(), 1);
        assert_eq!(max_hops_for_transport_limit(2).unwrap(), 3);
    }

    #[test]
    fn allows_zero_and_rejects_invalid_transport_limit() {
        assert_eq!(parse_transport_limit(Some("0")).unwrap(), 0);
        assert_eq!(
            parse_transport_limit(Some("-1")).unwrap_err(),
            "transport must be a non-negative integer"
        );
        assert_eq!(
            parse_transport_limit(Some("abc")).unwrap_err(),
            "transport must be a non-negative integer"
        );
    }

    #[test]
    fn structured_mct_record_overrides_baseline_airport_default() {
        let mut fra_records = airport_default_mct_records(180);
        fra_records.push(sample_mct_record("FRA", "FRA", "II", "0130"));
        let (airports, airport_mct) = sample_airport_context(fra_records, vec![]);
        let previous = sample_flight("AA", "100", "JFK", "FRA", 8, 0, Some("T1"), Some("T2"));
        let next = sample_flight("LH", "400", "FRA", "MAD", 10, 0, Some("T1"), Some("T1"));

        let effective = resolve_effective_mct(&airports, &airport_mct, &previous, &next);

        assert_eq!(effective.minutes, 90);
        assert_eq!(effective.source, EffectiveMctSource::StructuredRecord);
    }

    #[test]
    fn connection_building_filter_limits_global_interline_default() {
        let mut fra_records = airport_default_mct_records(180);
        fra_records.push(AirportMctRecord {
                arrival_station: None,
                time: Some("0400".to_string()),
                status: "II".to_string(),
                departure_station: None,
                requires_connection_building_filter: true,
                arrival_carrier: None,
                arrival_codeshare_indicator: false,
                arrival_codeshare_operating_carrier: None,
                departure_carrier: None,
                departure_codeshare_indicator: false,
                departure_codeshare_operating_carrier: None,
                arrival_aircraft_type: None,
                arrival_aircraft_body: None,
                departure_aircraft_type: None,
                departure_aircraft_body: None,
                arrival_terminal: None,
                departure_terminal: None,
                previous_country: None,
                previous_station: None,
                next_country: None,
                next_station: None,
                arrival_flight_number_range_start: None,
                arrival_flight_number_range_end: None,
                departure_flight_number_range_start: None,
                departure_flight_number_range_end: None,
                previous_state: None,
                next_state: None,
                previous_region: None,
                next_region: None,
                effective_from_local: None,
                effective_to_local: None,
                suppression_indicator: false,
                suppression_region: None,
                suppression_country: None,
                suppression_state: None,
            });
        let (airports, airport_mct) = sample_airport_context(
            fra_records,
            vec![ConnectionBuildingFilter {
                submitting_carrier: "AA".to_string(),
                partner_carrier_codes: vec!["UA".to_string()],
            }],
        );
        let previous = sample_flight("AA", "100", "JFK", "FRA", 8, 0, None, None);
        let next_allowed = sample_flight("UA", "900", "FRA", "MAD", 13, 0, None, None);
        let next_blocked = sample_flight("LH", "400", "FRA", "MAD", 13, 0, None, None);

        let allowed = resolve_effective_mct(&airports, &airport_mct, &previous, &next_allowed);
        let blocked = resolve_effective_mct(&airports, &airport_mct, &previous, &next_blocked);

        assert_eq!(allowed.minutes, 240);
        assert_eq!(allowed.source, EffectiveMctSource::StructuredRecord);
        assert_eq!(blocked.minutes, 180);
        assert_eq!(blocked.source, EffectiveMctSource::StructuredRecord);
    }

    #[test]
    fn suppression_record_falls_through_to_broader_default() {
        let mut fra_records = airport_default_mct_records(180);
        let broader_default = sample_mct_record("", "", "II", "0130");
        if let Some(index) = fra_records
            .iter()
            .position(|existing| existing.same_scope_as(&broader_default))
        {
            fra_records[index] = broader_default;
        }
        fra_records.extend(vec![
            sample_mct_record("FRA", "FRA", "II", "0100"),
            AirportMctRecord {
                arrival_station: Some("FRA".to_string()),
                time: None,
                status: "II".to_string(),
                departure_station: Some("FRA".to_string()),
                requires_connection_building_filter: false,
                arrival_carrier: None,
                arrival_codeshare_indicator: false,
                arrival_codeshare_operating_carrier: None,
                departure_carrier: None,
                departure_codeshare_indicator: false,
                departure_codeshare_operating_carrier: None,
                arrival_aircraft_type: None,
                arrival_aircraft_body: None,
                departure_aircraft_type: None,
                departure_aircraft_body: None,
                arrival_terminal: None,
                departure_terminal: None,
                previous_country: None,
                previous_station: None,
                next_country: None,
                next_station: None,
                arrival_flight_number_range_start: None,
                arrival_flight_number_range_end: None,
                departure_flight_number_range_start: None,
                departure_flight_number_range_end: None,
                previous_state: None,
                next_state: None,
                previous_region: None,
                next_region: None,
                effective_from_local: None,
                effective_to_local: None,
                suppression_indicator: true,
                suppression_region: None,
                suppression_country: None,
                suppression_state: None,
            },
        ]);
        let (airports, airport_mct) = sample_airport_context(fra_records, vec![]);
        let previous = sample_flight("AA", "100", "JFK", "FRA", 8, 0, None, None);
        let next = sample_flight("LH", "400", "FRA", "MAD", 10, 0, None, None);

        let effective = resolve_effective_mct(&airports, &airport_mct, &previous, &next);

        assert_eq!(effective.minutes, 90);
        assert_eq!(effective.source, EffectiveMctSource::StructuredRecord);
    }

    #[test]
    fn imported_global_default_replaces_baseline_airport_default_scope() {
        let mut fra_records = airport_default_mct_records(180);
        let global_imported = sample_mct_record("", "", "II", "0130");
        if let Some(index) = fra_records
            .iter()
            .position(|existing| existing.same_scope_as(&global_imported))
        {
            fra_records[index] = global_imported;
        }
        let (airports, airport_mct) = sample_airport_context(fra_records, vec![]);
        let previous = sample_flight("AA", "100", "JFK", "FRA", 8, 0, None, None);
        let next = sample_flight("LH", "400", "FRA", "MAD", 10, 0, None, None);

        let effective = resolve_effective_mct(&airports, &airport_mct, &previous, &next);

        assert_eq!(effective.minutes, 90);
        assert_eq!(effective.source, EffectiveMctSource::StructuredRecord);
    }

    #[test]
    fn state_scoped_mct_records_match_airport_state_codes() {
        let airports = HashMap::from([
            (
                "HNL".to_string(),
                sample_airport("HNL", "US", "HI"),
            ),
            (
                "ITO".to_string(),
                sample_airport("ITO", "US", "HI"),
            ),
            (
                "LAX".to_string(),
                sample_airport("LAX", "US", "CA"),
            ),
        ]);
        let previous = sample_flight("WN", "101", "ITO", "HNL", 8, 0, None, None);
        let next_hawaii = sample_flight("WN", "202", "HNL", "ITO", 10, 0, None, None);
        let next_california = sample_flight("WN", "203", "HNL", "LAX", 10, 0, None, None);
        let record = AirportMctRecord {
            arrival_station: Some("HNL".to_string()),
            time: Some("0035".to_string()),
            status: "DD".to_string(),
            departure_station: Some("HNL".to_string()),
            requires_connection_building_filter: false,
            arrival_carrier: Some("WN".to_string()),
            arrival_codeshare_indicator: false,
            arrival_codeshare_operating_carrier: None,
            departure_carrier: Some("WN".to_string()),
            departure_codeshare_indicator: false,
            departure_codeshare_operating_carrier: None,
            arrival_aircraft_type: None,
            arrival_aircraft_body: None,
            departure_aircraft_type: None,
            departure_aircraft_body: None,
            arrival_terminal: None,
            departure_terminal: None,
            previous_country: Some("US".to_string()),
            previous_station: None,
            next_country: Some("US".to_string()),
            next_station: None,
            arrival_flight_number_range_start: None,
            arrival_flight_number_range_end: None,
            departure_flight_number_range_start: None,
            departure_flight_number_range_end: None,
            previous_state: Some("HI".to_string()),
            next_state: Some("HI".to_string()),
            previous_region: None,
            next_region: None,
            effective_from_local: None,
            effective_to_local: None,
            suppression_indicator: false,
            suppression_region: None,
            suppression_country: None,
            suppression_state: None,
        };
        let airport_mct = HashMap::from([(
            "HNL".to_string(),
            AirportMctData {
                mct_records: {
                    let mut hnl_records = airport_default_mct_records(180);
                    hnl_records.push(record.clone());
                    hnl_records
                },
                connection_building_filters: vec![],
            },
        )]);

        let hawaii_effective =
            resolve_effective_mct(&airports, &airport_mct, &previous, &next_hawaii);
        let california_effective =
            resolve_effective_mct(&airports, &airport_mct, &previous, &next_california);

        assert_eq!(hawaii_effective.minutes, 35);
        assert_eq!(
            hawaii_effective.source,
            EffectiveMctSource::StructuredRecord
        );
        assert_eq!(california_effective.minutes, 180);
        assert_eq!(
            california_effective.source,
            EffectiveMctSource::StructuredRecord
        );
    }

    fn sample_same_flight_map() -> HashMap<String, Flightcore> {
        let mut flights = HashMap::new();
        let pek_tz = chrono_tz::Asia::Shanghai;
        let mad_tz = chrono_tz::Europe::Madrid;
        let gru_tz = chrono_tz::America::Sao_Paulo;

        let pek_mad = Flightcore::new(
            "CA".to_string(),
            "897".to_string(),
            AirportCode::new("PEK").unwrap(),
            AirportCode::new("MAD").unwrap(),
            pek_tz.with_ymd_and_hms(2026, 4, 2, 15, 0, 0).unwrap(),
            mad_tz.with_ymd_and_hms(2026, 4, 2, 21, 0, 0).unwrap(),
            720,
            Some("T3".to_string()),
            Some("T1".to_string()),
            sample_designator("CA", "897"),
            vec![sample_designator("LH", "7172")],
            vec!["XB".to_string()],
            Some("M".to_string()),
            Some("9".to_string()),
            Some("ET".to_string()),
        );
        let mad_gru = Flightcore::new(
            "CA".to_string(),
            "897".to_string(),
            AirportCode::new("MAD").unwrap(),
            AirportCode::new("GRU").unwrap(),
            mad_tz.with_ymd_and_hms(2026, 4, 2, 23, 15, 0).unwrap(),
            gru_tz.with_ymd_and_hms(2026, 4, 3, 5, 0, 0).unwrap(),
            645,
            Some("T1".to_string()),
            Some("T2".to_string()),
            sample_designator("CA", "897"),
            vec![sample_designator("LH", "7172")],
            vec!["XB".to_string()],
            Some("M".to_string()),
            Some("9".to_string()),
            Some("ET".to_string()),
        );
        let pek_gru = Flightcore::new(
            "CA".to_string(),
            "897".to_string(),
            AirportCode::new("PEK").unwrap(),
            AirportCode::new("GRU").unwrap(),
            pek_tz.with_ymd_and_hms(2026, 4, 2, 15, 0, 0).unwrap(),
            gru_tz.with_ymd_and_hms(2026, 4, 3, 5, 0, 0).unwrap(),
            1500,
            Some("T3".to_string()),
            Some("T2".to_string()),
            sample_designator("CA", "897"),
            vec![sample_designator("LH", "7172")],
            vec!["XB".to_string()],
            Some("M".to_string()),
            Some("9".to_string()),
            Some("ET".to_string()),
        );

        flights.insert(
            flight_storage_key(
                "CA",
                "897",
                "PEK",
                "MAD",
                NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            ),
            pek_mad,
        );
        flights.insert(
            flight_storage_key(
                "CA",
                "897",
                "MAD",
                "GRU",
                NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            ),
            mad_gru,
        );
        flights.insert(
            flight_storage_key(
                "CA",
                "897",
                "PEK",
                "GRU",
                NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            ),
            pek_gru,
        );

        flights
    }

    fn sample_designator(company: &str, flight_id: &str) -> FlightDesignatorRow {
        FlightDesignatorRow {
            company: company.to_string(),
            flight_number: flight_id.to_string(),
            operational_suffix: None,
        }
    }

    fn sample_airport_context(
        fra_records: Vec<AirportMctRecord>,
        fra_filters: Vec<ConnectionBuildingFilter>,
    ) -> (HashMap<String, Airport>, HashMap<String, AirportMctData>) {
        (
            HashMap::from([
                ("JFK".to_string(), sample_airport("JFK", "US", "NY")),
                ("FRA".to_string(), sample_airport("FRA", "DE", "HE")),
                ("MAD".to_string(), sample_airport("MAD", "ES", "MD")),
            ]),
            HashMap::from([(
                "FRA".to_string(),
                AirportMctData {
                    mct_records: fra_records,
                    connection_building_filters: fra_filters,
                },
            )]),
        )
    }

    fn sample_airport(
        code: &str,
        country: &str,
        state: &str,
    ) -> Airport {
        Airport::new_full(
            AirportCode::new(code).unwrap(),
            chrono_tz::UTC,
            Some(code.to_string()),
            None,
            Some(country.to_string()),
            Some(state.to_string()),
            0.0,
            0.0,
        )
    }

    fn sample_flight(
        company: &str,
        flight_number: &str,
        origin: &str,
        destination: &str,
        dep_hour: u32,
        arr_hour: u32,
        departure_terminal: Option<&str>,
        arrival_terminal: Option<&str>,
    ) -> Flightcore {
        let tz = chrono_tz::UTC;
        Flightcore::new(
            company.to_string(),
            flight_number.to_string(),
            AirportCode::new(origin).unwrap(),
            AirportCode::new(destination).unwrap(),
            tz.with_ymd_and_hms(2026, 4, 2, dep_hour, 0, 0).unwrap(),
            tz.with_ymd_and_hms(2026, 4, 2, arr_hour, 0, 0).unwrap(),
            (arr_hour.saturating_sub(dep_hour)) * 60,
            departure_terminal.map(ToOwned::to_owned),
            arrival_terminal.map(ToOwned::to_owned),
            sample_designator(company, flight_number),
            vec![],
            vec![],
            None,
            None,
            None,
        )
    }

    fn sample_mct_record(
        arrival_station: &str,
        departure_station: &str,
        status: &str,
        time: &str,
    ) -> AirportMctRecord {
        AirportMctRecord {
            arrival_station: (!arrival_station.is_empty()).then(|| arrival_station.to_string()),
            time: Some(time.to_string()),
            status: status.to_string(),
            departure_station: (!departure_station.is_empty())
                .then(|| departure_station.to_string()),
            requires_connection_building_filter: false,
            arrival_carrier: None,
            arrival_codeshare_indicator: false,
            arrival_codeshare_operating_carrier: None,
            departure_carrier: None,
            departure_codeshare_indicator: false,
            departure_codeshare_operating_carrier: None,
            arrival_aircraft_type: None,
            arrival_aircraft_body: None,
            departure_aircraft_type: None,
            departure_aircraft_body: None,
            arrival_terminal: None,
            departure_terminal: None,
            previous_country: None,
            previous_station: None,
            next_country: None,
            next_station: None,
            arrival_flight_number_range_start: None,
            arrival_flight_number_range_end: None,
            departure_flight_number_range_start: None,
            departure_flight_number_range_end: None,
            previous_state: None,
            next_state: None,
            previous_region: None,
            next_region: None,
            effective_from_local: None,
            effective_to_local: None,
            suppression_indicator: false,
            suppression_region: None,
            suppression_country: None,
            suppression_state: None,
        }
    }
}
