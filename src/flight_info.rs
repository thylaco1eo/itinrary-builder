use chrono::{DateTime, FixedOffset};
#[derive(Debug, Clone)]
pub struct FlightInfo {
    fltid: String,
    dpt_start_local: DateTime<FixedOffset>,
    dpt_end_local: DateTime<FixedOffset>,
    dpt_station: String,
    arr_station: String,
    frequency: Vec<u8>, // 0-6 for Sun-Sat
    flight_time: i64, // in minutes
}

impl FlightInfo{
    pub fn new(fltid: String, dpt_start_local: DateTime<FixedOffset>, dpt_end_local: DateTime<FixedOffset>, dpt_station:String,arr_station: String, frequency: Vec<u8>, flight_time: i64) -> Self {
        FlightInfo {
            fltid,
            dpt_start_local,
            dpt_end_local,
            dpt_station,
            arr_station,
            frequency,
            flight_time,
        }
    }
    pub fn fltid(&self) -> &String {
        &self.fltid
    }
    pub fn dpt_start_local(&self) -> &DateTime<FixedOffset> {
        &self.dpt_start_local
    }
    pub fn dpt_end_local(&self) -> &DateTime<FixedOffset> {
        &self.dpt_end_local
    }
    pub fn arr_station(&self) -> &String {
        &self.arr_station
    }
    pub fn frequency(&self) -> &Vec<u8> {
        &self.frequency
    }
    pub fn flight_time(&self) -> i64 {
        self.flight_time
    }
    pub fn dpt_station(&self) -> &String {
        &self.dpt_station
    }
}