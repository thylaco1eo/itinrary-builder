use chrono::{Duration,DateTime, Utc};
use crate::domain::airport::AirportCode;
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

    pub fn flight_id(&self) -> &String{
        &self.flight_id
    }
    pub fn origin(&self) -> &AirportCode{
        &self.origin
    }
    pub fn destination(&self) -> &AirportCode{
        &self.destination
    }
    pub fn dep_utc(&self) -> &DateTime<Utc>{
        &self.departure
    }
    pub fn arr_utc(&self) -> &DateTime<Utc>{
        &self.arrival
    }
    pub fn block_time(&self) -> &Duration{
        &self.block_time
    }
    pub fn company(&self) -> &str{ &self.company }
}

pub trait TimeResolver {
    fn airport_timezone(&self, code: &AirportCode) -> Tz;
}
