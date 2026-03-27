use crate::domain::airport::AirportCode;
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
}
