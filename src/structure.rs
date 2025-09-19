use serde::Deserialize;
use std::sync::Mutex;
use std::collections::HashMap;
use chrono::{DateTime, FixedOffset, Utc};
use actix_multipart::form::MultipartForm;
use actix_multipart::form::tempfile::TempFile;


#[derive(Debug, Clone)]
pub struct FlightInfo {
    fltid: String,
    carrier: String,
    dpt_start_utc: DateTime<Utc>,
    dpt_end_utc: DateTime<Utc>,
    dpt_station: String,
    arr_station: String,
    frequency: Vec<u8>, // 0-6 for Sun-Sat
    flight_time: i64, // in minutes
}

impl FlightInfo{
    pub fn new(fltid: String, carrier: String,dpt_start_utc: DateTime<Utc>, dpt_end_utc: DateTime<Utc>, dpt_station:String,arr_station: String, frequency: Vec<u8>, flight_time: i64) -> Self {
        FlightInfo {
            fltid,
            carrier,
            dpt_start_utc,
            dpt_end_utc,
            dpt_station,
            arr_station,
            frequency,
            flight_time,
        }
    }
    pub fn fltid(&self) -> &String {
        &self.fltid
    }
    pub fn carrier(&self) -> &String {
        &self.carrier
    }
    pub fn dpt_start_local(&self) -> &DateTime<Utc> {
        &self.dpt_start_utc
    }
    pub fn dpt_end_local(&self) -> &DateTime<Utc> {
        &self.dpt_end_utc
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

pub struct WebData {
    flights: Mutex<HashMap<String, Vec<FlightInfo>>>,
    db_info: DataBase,
}

impl WebData {
    pub fn new(flights: Mutex<HashMap<String, Vec<FlightInfo>>>, db_info: DataBase) -> Self {
        WebData { flights, db_info }
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

#[derive(Deserialize,Clone)]
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

#[derive(Deserialize)]
pub struct log{
    level: String,
    file: String,
    pattern: String,
}

impl log {
    pub fn new(level: String, file: String, pattern: String) -> Self {
        log { level, file, pattern }
    }
    pub fn level(&self) -> &String {
        &self.level
    }
    pub fn file(&self) -> &String {
        &self.file
    }
    pub fn pattern(&self) -> &String {
        &self.pattern
    }
}

#[derive(Deserialize)]
pub struct Configuration{
    database: DataBase,
    log: log,
}
impl Configuration {
    pub fn new(database: DataBase, log: log) -> Self {
        Configuration { database, log }
    }
    pub fn database(&self) -> &DataBase {
        &self.database
    }
    pub fn log(&self) -> &log {
        &self.log
    } 
}