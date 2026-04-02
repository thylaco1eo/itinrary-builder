use crate::domain::airport::{Airport, AirportCode};
use crate::domain::flight::Flightcore;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::Infrastructure::db::repository::airport_repo::get_all_airports;
use crate::Infrastructure::db::repository::flight_repo::get_flights;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::mem::size_of;
use surrealdb::{engine::any, Surreal};

pub struct WebData {
    database: Surreal<any::Any>,
    flights: HashMap<String, Flightcore>,
    airports: HashMap<String, Airport>,
}

impl WebData {
    pub async fn new(data_base: Surreal<any::Any>) -> Self {
        println!("Loading airports into memory...");
        let mut flights = HashMap::new();
        let mut airports = HashMap::new();

        let airport_rows = get_all_airports(&data_base).await;
        let source_airport_rows = airport_rows.len();
        let airport_row_buffer_bytes =
            estimate_airport_row_vec_bytes(&airport_rows, airport_rows.capacity());
        let mut rejected_airports = 0usize;
        for row in airport_rows {
            let code = row.code.code.clone();
            match Airport::try_from(row) {
                Ok(airport) => {
                    airports.insert(code, airport);
                }
                Err(_) => {
                    rejected_airports += 1;
                }
            }
        }
        println!(
            "Loaded {} airports into memory from {} source rows ({} rejected).",
            airports.len(),
            source_airport_rows,
            rejected_airports
        );

        println!("Loading flights into memory...");
        let flight_rows: Vec<FlightRow> = get_flights(&data_base).await;
        let source_flight_rows = flight_rows.len();
        let flight_row_buffer_bytes =
            estimate_flight_row_vec_bytes(&flight_rows, flight_rows.capacity());
        let mut skipped_missing_airports = 0usize;
        let mut duplicate_keys = 0usize;
        for row in flight_rows {
            match build_flight_entry(row, &airports) {
                FlightRowLoadResult::Ready { key, flight } => {
                    if flights.insert(key, flight).is_some() {
                        duplicate_keys += 1;
                    }
                }
                FlightRowLoadResult::MissingAirports => {
                    skipped_missing_airports += 1;
                }
            }
        }
        println!(
            "Loaded {} flights into memory from {} source rows.",
            flights.len(),
            source_flight_rows
        );
        println!(
            "Flight preload summary: {} unique retained, {} duplicate-key overwrites, {} skipped because an airport was missing from the core map.",
            flights.len(),
            duplicate_keys,
            skipped_missing_airports
        );
        println!(
            "Core cache sizes: airports.len() = {}, flights.len() = {}.",
            airports.len(),
            flights.len()
        );

        let airport_map_bytes = estimate_airport_map_bytes(&airports);
        let flight_map_bytes = estimate_flight_map_bytes(&flights);
        let retained_bytes = airport_map_bytes + flight_map_bytes;
        let startup_peak_bytes = retained_bytes + flight_row_buffer_bytes;
        println!(
            "Approximate memory usage: airport row buffer ~= {}, raw flight row buffer ~= {}, airport map ~= {}, flight map ~= {}, retained ~= {}, startup peak during preload ~= {}.",
            format_bytes(airport_row_buffer_bytes),
            format_bytes(flight_row_buffer_bytes),
            format_bytes(airport_map_bytes),
            format_bytes(flight_map_bytes),
            format_bytes(retained_bytes),
            format_bytes(startup_peak_bytes)
        );
        println!(
            "Type sizes: Airport = {} B, FlightRow = {} B, Flightcore = {} B.",
            size_of::<Airport>(),
            size_of::<FlightRow>(),
            size_of::<Flightcore>()
        );

        WebData {
            database: data_base,
            flights,
            airports,
        }
    }

    pub fn database(&self) -> &Surreal<any::Any> {
        &self.database
    }

    pub fn flights(&self) -> &HashMap<String, Flightcore> {
        &self.flights
    }

    pub fn airports(&self) -> &HashMap<String, Airport> {
        &self.airports
    }
}

pub fn flight_storage_key(
    company: &str,
    flight_id: &str,
    origin: &str,
    destination: &str,
    dep_date: NaiveDate,
) -> String {
    format!(
        "{}_{}_{}_{}_{}",
        company,
        flight_id,
        origin,
        destination,
        dep_date.format("%Y-%m-%d")
    )
}

fn try_from(row: FlightRow, airports: &HashMap<String, Airport>) -> Option<Flightcore> {
    if !airports.contains_key(&row.origin_code) || !airports.contains_key(&row.destination_code) {
        return None;
    }
    let origin_tz = airports.get(&row.origin_code).unwrap().timezone();
    let destination_tz = airports.get(&row.destination_code).unwrap().timezone();
    let flight = Flightcore::new(
        row.company,
        row.flight_num,
        AirportCode::new(row.origin_code).unwrap(),
        AirportCode::new(row.destination_code).unwrap(),
        row.dep_local.with_timezone(&origin_tz),
        row.arr_local.with_timezone(&destination_tz),
        row.block_time_minutes,
    );
    Some(flight)
}

enum FlightRowLoadResult {
    Ready { key: String, flight: Flightcore },
    MissingAirports,
}

fn build_flight_entry(row: FlightRow, airports: &HashMap<String, Airport>) -> FlightRowLoadResult {
    match try_from(row, airports) {
        Some(flight) => {
            let key = flight_storage_key(
                flight.company(),
                flight.flight_id(),
                flight.origin().as_str(),
                flight.destination().as_str(),
                flight.dep_local().date_naive(),
            );
            FlightRowLoadResult::Ready { key, flight }
        }
        None => FlightRowLoadResult::MissingAirports,
    }
}

fn estimate_airport_row_vec_bytes(
    rows: &[crate::Infrastructure::db::model::airport_row::AirportRow],
    capacity: usize,
) -> usize {
    size_of::<Vec<crate::Infrastructure::db::model::airport_row::AirportRow>>()
        + capacity * size_of::<crate::Infrastructure::db::model::airport_row::AirportRow>()
        + rows
            .iter()
            .map(|row| {
                row.code.code.capacity()
                    + row.timezone.capacity()
                    + row.name.as_ref().map_or(0, |value| value.capacity())
                    + row.city.as_ref().map_or(0, |value| value.capacity())
                    + row.country.as_ref().map_or(0, |value| value.capacity())
            })
            .sum::<usize>()
}

fn estimate_flight_row_vec_bytes(rows: &[FlightRow], capacity: usize) -> usize {
    size_of::<Vec<FlightRow>>()
        + capacity * size_of::<FlightRow>()
        + rows
            .iter()
            .map(|row| {
                row.company.capacity()
                    + row.flight_num.capacity()
                    + row.origin_code.capacity()
                    + row.destination_code.capacity()
            })
            .sum::<usize>()
}

fn estimate_airport_map_bytes(airports: &HashMap<String, Airport>) -> usize {
    size_of::<HashMap<String, Airport>>()
        + airports.capacity() * size_of::<(String, Airport)>()
        + airports
            .iter()
            .map(|(key, airport)| {
                key.capacity()
                    + airport.id().as_str().len()
                    + airport.name().map_or(0, str::len)
                    + airport.city().map_or(0, str::len)
                    + airport.country().map_or(0, str::len)
            })
            .sum::<usize>()
}

fn estimate_flight_map_bytes(flights: &HashMap<String, Flightcore>) -> usize {
    size_of::<HashMap<String, Flightcore>>()
        + flights.capacity() * size_of::<(String, Flightcore)>()
        + flights
            .iter()
            .map(|(key, flight)| {
                key.capacity()
                    + flight.company().len()
                    + flight.flight_id().len()
                    + flight.origin().as_str().len()
                    + flight.destination().as_str().len()
            })
            .sum::<usize>()
}

fn format_bytes(bytes: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit_index = 0usize;

    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{value:.2} {}", UNITS[unit_index])
    }
}
