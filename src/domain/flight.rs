use crate::domain::airport::AirportCode;
use crate::Infrastructure::db::model::flight_row::FlightDesignatorRow;
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;

#[derive(Clone, Debug)]
pub struct Flight {
    company: String,
    flight_id: String,
    origin: AirportCode,
    destination: AirportCode,
    departure: DateTime<Utc>,
    arrival: DateTime<Utc>,
    block_time: Duration,
}

impl Flight {
    pub fn new(
        company: String,
        flt_id: String,
        origin: AirportCode,
        destination: AirportCode,
        departure: DateTime<Utc>,
        arrival: DateTime<Utc>,
        block_time: Duration,
    ) -> Self {
        Self {
            company,
            flight_id: flt_id,
            origin,
            destination,
            departure,
            arrival,
            block_time,
        }
    }

    pub fn flight_id(&self) -> &String {
        &self.flight_id
    }
    pub fn origin(&self) -> &AirportCode {
        &self.origin
    }
    pub fn destination(&self) -> &AirportCode {
        &self.destination
    }
    pub fn dep_utc(&self) -> &DateTime<Utc> {
        &self.departure
    }
    pub fn arr_utc(&self) -> &DateTime<Utc> {
        &self.arrival
    }
    pub fn block_time(&self) -> &Duration {
        &self.block_time
    }
    pub fn company(&self) -> &str {
        &self.company
    }
}

#[derive(Clone, Debug)]
pub struct Flightcore {
    company: String,
    flight_id: String,
    origin: AirportCode,
    destination: AirportCode,
    departure: DateTime<Tz>,
    arrival: DateTime<Tz>,
    block_time: u32,
    departure_terminal: Option<String>,
    arrival_terminal: Option<String>,
    operating_designator: FlightDesignatorRow,
    duplicate_designators: Vec<FlightDesignatorRow>,
    joint_operation_airline_designators: Vec<String>,
    meal_service_note: Option<String>,
    in_flight_service_info: Option<String>,
    electronic_ticketing_info: Option<String>,
}

impl Flightcore {
    pub fn new(
        company: String,
        flt_id: String,
        origin: AirportCode,
        destination: AirportCode,
        departure: DateTime<Tz>,
        arrival: DateTime<Tz>,
        block_time: u32,
        departure_terminal: Option<String>,
        arrival_terminal: Option<String>,
        operating_designator: FlightDesignatorRow,
        duplicate_designators: Vec<FlightDesignatorRow>,
        joint_operation_airline_designators: Vec<String>,
        meal_service_note: Option<String>,
        in_flight_service_info: Option<String>,
        electronic_ticketing_info: Option<String>,
    ) -> Self {
        Self {
            company,
            flight_id: flt_id,
            origin,
            destination,
            departure,
            arrival,
            block_time,
            departure_terminal,
            arrival_terminal,
            operating_designator,
            duplicate_designators,
            joint_operation_airline_designators,
            meal_service_note,
            in_flight_service_info,
            electronic_ticketing_info,
        }
    }

    pub fn flight_id(&self) -> &str {
        &self.flight_id
    }

    pub fn origin(&self) -> &AirportCode {
        &self.origin
    }

    pub fn destination(&self) -> &AirportCode {
        &self.destination
    }

    pub fn dep_local(&self) -> &DateTime<Tz> {
        &self.departure
    }

    pub fn arr_local(&self) -> &DateTime<Tz> {
        &self.arrival
    }

    pub fn block_time_minutes(&self) -> u32 {
        self.block_time
    }

    pub fn company(&self) -> &str {
        &self.company
    }

    pub fn departure_terminal(&self) -> Option<&str> {
        self.departure_terminal.as_deref()
    }

    pub fn arrival_terminal(&self) -> Option<&str> {
        self.arrival_terminal.as_deref()
    }

    pub fn operating_designator(&self) -> &FlightDesignatorRow {
        &self.operating_designator
    }

    pub fn duplicate_designators(&self) -> &[FlightDesignatorRow] {
        &self.duplicate_designators
    }

    pub fn joint_operation_airline_designators(&self) -> &[String] {
        &self.joint_operation_airline_designators
    }

    pub fn meal_service_note(&self) -> Option<&str> {
        self.meal_service_note.as_deref()
    }

    pub fn in_flight_service_info(&self) -> Option<&str> {
        self.in_flight_service_info.as_deref()
    }

    pub fn electronic_ticketing_info(&self) -> Option<&str> {
        self.electronic_ticketing_info.as_deref()
    }
}
