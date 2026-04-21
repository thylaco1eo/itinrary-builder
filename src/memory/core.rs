use crate::domain::airport::{Airport, AirportCode};
use crate::domain::flight::Flightcore;
use crate::domain::mct::{AirportMctData, GlobalMctData};
use crate::Infrastructure::db::model::airport_row::{AirportRow, AirportRowError};
use crate::Infrastructure::db::model::flight_row::{FlightCacheRow, FlightRow};
use crate::Infrastructure::db::repository::airport_repo::get_all_airports;
use crate::Infrastructure::db::repository::flight_repo::get_flights;
use crate::Infrastructure::db::repository::mct_repo::{get_all_airport_mct, get_global_mct};
use chrono::NaiveDate;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{RwLock, RwLockReadGuard};
use surrealdb::{engine::any, Surreal};

pub struct WebData {
    database: Surreal<any::Any>,
    flights: RwLock<HashMap<String, Flightcore>>,
    airports: RwLock<HashMap<String, Airport>>,
    airport_mct: RwLock<HashMap<String, AirportMctData>>,
    global_mct: RwLock<GlobalMctData>,
}

#[derive(Debug, Default)]
pub struct FlightCacheUpdateSummary {
    pub upserted: usize,
    pub overwritten: usize,
    pub skipped_missing_airports: usize,
}

#[derive(Debug, Default)]
pub struct FlightCacheReplaceSummary {
    pub active_flights: usize,
    pub duplicate_keys_within_snapshot: usize,
    pub skipped_missing_airports: usize,
}

impl WebData {
    pub async fn new(data_base: Surreal<any::Any>) -> Self {
        println!("Loading airports into memory...");
        let mut flights = HashMap::new();
        let airport_rows = get_all_airports(&data_base).await;
        let airport_mct = match get_all_airport_mct(&data_base).await {
            Ok(airport_mct) => airport_mct.into_iter().collect::<HashMap<_, _>>(),
            Err(error) => {
                eprintln!("Failed to load airport MCT payloads from database: {}", error);
                HashMap::new()
            }
        };
        let global_mct = match get_global_mct(&data_base).await {
            Ok(global_mct) => global_mct,
            Err(error) => {
                eprintln!("Failed to load global MCT payload from database: {}", error);
                GlobalMctData::default()
            }
        };
        let source_airport_rows = airport_rows.len();
        let airport_row_buffer_bytes =
            estimate_airport_row_vec_bytes(&airport_rows, airport_rows.capacity());
        let (airports, rejected_airports) = build_airport_map(airport_rows);
        println!(
            "Loaded {} airports into memory from {} source rows ({} rejected).",
            airports.len(),
            source_airport_rows,
            rejected_airports
        );
        println!(
            "Loaded {} airport-specific MCT payloads into memory ({} records total).",
            airport_mct.len(),
            airport_mct
                .values()
                .map(|payload| payload.mct_records.len())
                .sum::<usize>()
        );
        println!(
            "Loaded {} global MCT records and {} global connection-building filters into memory.",
            global_mct.mct_records.len(),
            global_mct.connection_building_filters.len()
        );

        println!("Loading flights into memory...");
        let flight_rows: Vec<FlightCacheRow> = get_flights(&data_base).await;
        let source_flight_rows = flight_rows.len();
        let flight_row_buffer_bytes =
            estimate_flight_row_vec_bytes(&flight_rows, flight_rows.capacity());
        let mut skipped_missing_airports = 0usize;
        let mut duplicate_keys = 0usize;
        for row in flight_rows {
            match build_flight_entry_from_cache_row(row, &airports) {
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
        let airport_mct_map_bytes = estimate_airport_mct_map_bytes(&airport_mct);
        let flight_map_bytes = estimate_flight_map_bytes(&flights);
        let global_mct_bytes = estimate_mct_payload_bytes(&global_mct);
        let retained_bytes = airport_map_bytes + airport_mct_map_bytes + global_mct_bytes + flight_map_bytes;
        let startup_peak_bytes = retained_bytes + flight_row_buffer_bytes;
        println!(
            "Approximate memory usage: airport row buffer ~= {}, raw flight row buffer ~= {}, airport map ~= {}, airport MCT map ~= {}, global MCT ~= {}, flight map ~= {}, retained ~= {}, startup peak during preload ~= {}.",
            format_bytes(airport_row_buffer_bytes),
            format_bytes(flight_row_buffer_bytes),
            format_bytes(airport_map_bytes),
            format_bytes(airport_mct_map_bytes),
            format_bytes(global_mct_bytes),
            format_bytes(flight_map_bytes),
            format_bytes(retained_bytes),
            format_bytes(startup_peak_bytes)
        );
        println!(
            "Type sizes: Airport = {} B, AirportMctData = {} B, GlobalMctData = {} B, FlightCacheRow = {} B, FlightRow = {} B, Flightcore = {} B.",
            size_of::<Airport>(),
            size_of::<AirportMctData>(),
            size_of::<GlobalMctData>(),
            size_of::<FlightCacheRow>(),
            size_of::<FlightRow>(),
            size_of::<Flightcore>()
        );

        WebData {
            database: data_base,
            flights: RwLock::new(flights),
            airports: RwLock::new(airports),
            airport_mct: RwLock::new(airport_mct),
            global_mct: RwLock::new(global_mct),
        }
    }

    pub fn database(&self) -> &Surreal<any::Any> {
        &self.database
    }

    pub fn flights(&self) -> RwLockReadGuard<'_, HashMap<String, Flightcore>> {
        self.flights.read().unwrap()
    }

    pub fn airports(&self) -> RwLockReadGuard<'_, HashMap<String, Airport>> {
        self.airports.read().unwrap()
    }

    pub fn airport_mct(&self) -> RwLockReadGuard<'_, HashMap<String, AirportMctData>> {
        self.airport_mct.read().unwrap()
    }

    pub fn global_mct(&self) -> RwLockReadGuard<'_, GlobalMctData> {
        self.global_mct.read().unwrap()
    }

    pub fn upsert_airport(&self, row: AirportRow) -> Result<(), AirportRowError> {
        let code = row.code.code.clone();
        let airport = Airport::try_from(row)?;
        self.airports.write().unwrap().insert(code, airport);
        Ok(())
    }

    pub async fn reload_airports(&self) -> usize {
        let airport_rows = get_all_airports(&self.database).await;
        let (airports, rejected_airports) = build_airport_map(airport_rows);
        let airport_count = airports.len();
        println!(
            "Reloaded {} airports into memory ({} rejected).",
            airport_count, rejected_airports
        );
        *self.airports.write().unwrap() = airports;
        airport_count
    }

    pub async fn reload_airport_mct(&self) -> usize {
        let airport_mct = match get_all_airport_mct(&self.database).await {
            Ok(airport_mct) => airport_mct.into_iter().collect::<HashMap<_, _>>(),
            Err(error) => {
                eprintln!("Failed to reload airport MCT payloads from database: {}", error);
                HashMap::new()
            }
        };
        let record_count = airport_mct
            .values()
            .map(|payload| payload.mct_records.len())
            .sum::<usize>();
        println!(
            "Reloaded airport MCT payloads ({} airports, {} records).",
            airport_mct.len(),
            record_count
        );
        *self.airport_mct.write().unwrap() = airport_mct;
        record_count
    }

    pub async fn reload_global_mct(&self) -> usize {
        let global_mct = match get_global_mct(&self.database).await {
            Ok(global_mct) => global_mct,
            Err(error) => {
                eprintln!("Failed to reload global MCT payload from database: {}", error);
                GlobalMctData::default()
            }
        };
        let record_count = global_mct.mct_records.len();
        println!(
            "Reloaded global MCT payload ({} records, {} connection-building filters).",
            record_count,
            global_mct.connection_building_filters.len()
        );
        *self.global_mct.write().unwrap() = global_mct;
        record_count
    }

    pub fn set_airport_mct(&self, code: String, airport_mct: AirportMctData) {
        let mut airport_mct_map = self.airport_mct.write().unwrap();
        if airport_mct.mct_records.is_empty() && airport_mct.connection_building_filters.is_empty()
        {
            airport_mct_map.remove(&code);
        } else {
            airport_mct_map.insert(code, airport_mct);
        }
    }

    pub fn set_global_mct(&self, global_mct: GlobalMctData) {
        *self.global_mct.write().unwrap() = global_mct;
    }

    pub fn upsert_flights(&self, rows: Vec<FlightRow>) -> FlightCacheUpdateSummary {
        let airports = self.airports.read().unwrap();
        let mut flights = self.flights.write().unwrap();
        let mut summary = FlightCacheUpdateSummary::default();

        for row in rows {
            match build_flight_entry(row, &airports) {
                FlightRowLoadResult::Ready { key, flight } => {
                    if flights.insert(key, flight).is_some() {
                        summary.overwritten += 1;
                    } else {
                        summary.upserted += 1;
                    }
                }
                FlightRowLoadResult::MissingAirports => {
                    summary.skipped_missing_airports += 1;
                }
            }
        }

        summary
    }

    pub fn replace_flights(&self, rows: Vec<FlightRow>) -> FlightCacheReplaceSummary {
        let airports = self.airports.read().unwrap();
        let mut next_flights = HashMap::new();
        let mut summary = FlightCacheReplaceSummary::default();

        for row in rows {
            match build_flight_entry(row, &airports) {
                FlightRowLoadResult::Ready { key, flight } => {
                    if next_flights.insert(key, flight).is_some() {
                        summary.duplicate_keys_within_snapshot += 1;
                    }
                }
                FlightRowLoadResult::MissingAirports => {
                    summary.skipped_missing_airports += 1;
                }
            }
        }

        summary.active_flights = next_flights.len();
        *self.flights.write().unwrap() = next_flights;
        summary
    }
}

fn build_airport_map(rows: Vec<AirportRow>) -> (HashMap<String, Airport>, usize) {
    let mut airports = HashMap::new();
    let mut rejected_airports = 0usize;

    for row in rows {
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

    (airports, rejected_airports)
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

fn try_from(row: FlightCacheRow, airports: &HashMap<String, Airport>) -> Option<Flightcore> {
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
        row.departure_terminal,
        row.arrival_terminal,
        row.operating_designator,
        row.duplicate_designators,
        row.joint_operation_airline_designators,
        row.meal_service_note,
        row.in_flight_service_info,
        row.electronic_ticketing_info,
    );
    Some(flight)
}

enum FlightRowLoadResult {
    Ready { key: String, flight: Flightcore },
    MissingAirports,
}

fn build_flight_entry(row: FlightRow, airports: &HashMap<String, Airport>) -> FlightRowLoadResult {
    build_flight_entry_from_cache_row(flight_cache_row_from_flight_row(row), airports)
}

fn build_flight_entry_from_cache_row(
    row: FlightCacheRow,
    airports: &HashMap<String, Airport>,
) -> FlightRowLoadResult {
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

fn flight_cache_row_from_flight_row(row: FlightRow) -> FlightCacheRow {
    FlightCacheRow {
        company: row.company,
        flight_num: row.flight_num,
        origin_code: row.origin_code,
        destination_code: row.destination_code,
        dep_local: row.dep_local,
        arr_local: row.arr_local,
        block_time_minutes: row.block_time_minutes,
        departure_terminal: row.departure_terminal,
        arrival_terminal: row.arrival_terminal,
        operating_designator: row.operating_designator,
        duplicate_designators: row.duplicate_designators,
        joint_operation_airline_designators: row.joint_operation_airline_designators,
        meal_service_note: row.meal_service_note,
        in_flight_service_info: row.in_flight_service_info,
        electronic_ticketing_info: row.electronic_ticketing_info,
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
                    + row.state.as_ref().map_or(0, |value| value.capacity())
            })
            .sum::<usize>()
}

fn estimate_airport_mct_map_bytes(airport_mct: &HashMap<String, AirportMctData>) -> usize {
    size_of::<HashMap<String, AirportMctData>>()
        + airport_mct.capacity() * size_of::<(String, AirportMctData)>()
        + airport_mct
            .iter()
            .map(|(key, payload)| key.capacity() + estimate_mct_payload_bytes(payload))
            .sum::<usize>()
}

fn estimate_mct_payload_bytes(payload: &AirportMctData) -> usize {
    size_of::<AirportMctData>()
        + payload
            .mct_records
            .iter()
            .map(estimate_mct_record_bytes)
            .sum::<usize>()
        + payload
            .connection_building_filters
            .iter()
            .map(estimate_connection_building_filter_bytes)
            .sum::<usize>()
}

fn estimate_flight_row_vec_bytes(rows: &[FlightCacheRow], capacity: usize) -> usize {
    size_of::<Vec<FlightCacheRow>>()
        + capacity * size_of::<FlightCacheRow>()
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
                    + airport.state().map_or(0, str::len)
            })
            .sum::<usize>()
}

fn estimate_mct_record_bytes(record: &crate::domain::mct::AirportMctRecord) -> usize {
    option_string_capacity(&record.arrival_station)
        + option_string_capacity(&record.time)
        + record.status.capacity()
        + option_string_capacity(&record.departure_station)
        + option_string_capacity(&record.arrival_carrier)
        + option_string_capacity(&record.arrival_codeshare_operating_carrier)
        + option_string_capacity(&record.departure_carrier)
        + option_string_capacity(&record.departure_codeshare_operating_carrier)
        + option_string_capacity(&record.arrival_aircraft_type)
        + option_string_capacity(&record.arrival_aircraft_body)
        + option_string_capacity(&record.departure_aircraft_type)
        + option_string_capacity(&record.departure_aircraft_body)
        + option_string_capacity(&record.arrival_terminal)
        + option_string_capacity(&record.departure_terminal)
        + option_string_capacity(&record.previous_country)
        + option_string_capacity(&record.previous_station)
        + option_string_capacity(&record.next_country)
        + option_string_capacity(&record.next_station)
        + option_string_capacity(&record.arrival_flight_number_range_start)
        + option_string_capacity(&record.arrival_flight_number_range_end)
        + option_string_capacity(&record.departure_flight_number_range_start)
        + option_string_capacity(&record.departure_flight_number_range_end)
        + option_string_capacity(&record.previous_state)
        + option_string_capacity(&record.next_state)
        + option_string_capacity(&record.previous_region)
        + option_string_capacity(&record.next_region)
        + option_string_capacity(&record.effective_from_local)
        + option_string_capacity(&record.effective_to_local)
        + option_string_capacity(&record.suppression_region)
        + option_string_capacity(&record.suppression_country)
        + option_string_capacity(&record.suppression_state)
}

fn estimate_connection_building_filter_bytes(
    filter: &crate::domain::mct::ConnectionBuildingFilter,
) -> usize {
    filter.submitting_carrier.capacity()
        + size_of::<Vec<String>>()
        + filter
            .partner_carrier_codes
            .iter()
            .map(String::capacity)
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

fn option_string_capacity(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
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
