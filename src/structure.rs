use serde::Deserialize;
use std::sync::Mutex;
use std::collections::HashMap;
use chrono::{DateTime, FixedOffset};
use actix_multipart::form::MultipartForm;
use actix_multipart::form::tempfile::TempFile;

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

pub struct Configuration {
    flights: Mutex<HashMap<String, Vec<FlightInfo>>>,
    db_info: DataBase,
}

impl Configuration {
    pub fn new(flights: Mutex<HashMap<String, Vec<FlightInfo>>>, db_info: DataBase) -> Self {
        Configuration { flights, db_info }
    }
    pub fn flights(&self) -> &Mutex<HashMap<String, Vec<FlightInfo>>> {
        &self.flights
    }
    pub fn db_info(&self) -> &DataBase {
        &self.db_info
    }
}

#[derive(MultipartForm)]
pub struct SSIM{
    file : TempFile,
}

impl SSIM {
    pub fn file(&mut self) -> &mut TempFile {
        &mut self.file
    }   
}

#[derive(Deserialize)]
pub struct DataBase{
    host: String,
    port: String,
    username: String,
    password: String,
    dbname: String,
}

impl DataBase {
    pub fn new(host: String, port: String, username: String, password: String, dbname: String) -> Self {
        DataBase { host, port, username, password, dbname }
    }
    pub fn host(&self) -> &String {
        &self.host
    }
    pub fn port(&self) -> &String {
        &self.port
    }
    pub fn username(&self) -> &String {
        &self.username
    }
    pub fn password(&self) -> &String {
        &self.password
    }
    pub fn dbname(&self) -> &String {
        &self.dbname
    } 
}