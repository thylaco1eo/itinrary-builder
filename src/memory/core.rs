use crate::domain::airport::{Airport, AirportCode};
use crate::domain::flight::Flightcore;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::Infrastructure::db::repository::airport_repo::get_all_airports;
use crate::Infrastructure::db::repository::flight_repo::get_flights;
use std::collections::HashMap;
use surrealdb::{engine::any, Surreal};
use surrealdb_types::ToSql;

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
        for row in airport_rows {
            let code = row.code.code.clone();
            match Airport::try_from(row) {
                Ok(airport) => {
                    airports.insert(code, airport);
                }
                Err(_) => {}
            }
        }
        println!("Loaded {} airports into memory.", airports.len());

        println!("Loading flights into memory...");
        let flight_rows: Vec<FlightRow> = get_flights(&data_base).await;
        for row in flight_rows {
            if let Some(e) = try_from(row.clone(), &airports) {
                flights.insert(row.id.key.to_sql(), e);
            }
        }
        println!("Loaded {} flights into memory.", flights.len());

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
