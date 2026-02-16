use chrono::{NaiveDateTime, Duration,DateTime, Utc,TimeZone, Weekday};
use crate::domain::airport::AirportCode;
use chrono_tz::Tz;

#[derive(Clone, Debug)]
pub struct Flight {
    company: String,
    origin: AirportCode,
    destination: AirportCode,
    departure: NaiveDateTime,
    arrival: NaiveDateTime,
    block_time: Duration,
}

impl Flight {
    pub fn new(
        company: String,
        origin: AirportCode,
        destination: AirportCode,
        departure: NaiveDateTime,
        arrival: NaiveDateTime,
        block_time: Duration,
    ) -> Self {
        Self {
            company,
            origin,
            destination,
            departure,
            arrival,
            block_time,
        }
    }

    pub fn dep_utc<R: TimeResolver>(&self, r: &R) -> DateTime<Utc> {
        let tz = r.airport_timezone(&self.origin);
        tz.from_local_datetime(&self.departure)
            .single()
            .expect("invalid local departure time")
            .with_timezone(&Utc)
    }

    pub fn arr_utc<R: TimeResolver>(&self, r: &R) -> DateTime<Utc> {
        let tz = r.airport_timezone(&self.destination);
        tz.from_local_datetime(&self.arrival)
            .single()
            .expect("invalid local arrival time")
            .with_timezone(&Utc)
    }

    pub fn origin(&self) -> &AirportCode{
        &self.origin
    }
    pub fn destination(&self) -> &AirportCode{
        &self.destination
    }
    pub fn dep_local(&self) -> &NaiveDateTime{
        &self.departure
    }
    pub fn arr_local(&self) -> &NaiveDateTime{
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
