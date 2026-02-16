use actix_multipart::form::tempfile::TempFile;
use actix_multipart::form::MultipartForm;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use surrealdb::{Surreal, engine::any};

pub struct WebData {
    database: Surreal<any::Any>,
}

impl WebData {
    pub fn new(data_base: Surreal<any::Any>) -> Self {
        WebData {
            database: data_base,
        }
    }

    pub fn database(&self) -> &Surreal<any::Any> {
        &self.database
    }
}

#[derive(Debug, Clone)]
pub struct FlightInfo {
    flt_id: String,
    carrier: String,
    dpt_station: String,
    arr_station: String,
    dpt_start_utc: DateTime<Utc>,
    dpt_end_utc: DateTime<Utc>,
    frequency: String, // 0-6 for Sun-Sat
    flight_time: i64,  // in minutes
}

impl FlightInfo {
    pub fn new(
        flt_id: String,
        carrier: String,
        dpt_start_utc: DateTime<Utc>,
        dpt_end_utc: DateTime<Utc>,
        dpt_station: String,
        arr_station: String,
        frequency: String,
        flight_time: i64,
    ) -> Self {
        FlightInfo {
            flt_id,
            carrier,
            dpt_start_utc,
            dpt_end_utc,
            dpt_station,
            arr_station,
            frequency,
            flight_time,
        }
    }
    pub fn flt_id(&self) -> &String {
        &self.flt_id
    }
    pub fn carrier(&self) -> &String {
        &self.carrier
    }
    pub fn dpt_start_utc(&self) -> &DateTime<Utc> {
        &self.dpt_start_utc
    }
    pub fn dpt_end_utc(&self) -> &DateTime<Utc> {
        &self.dpt_end_utc
    }
    pub fn arr_station(&self) -> &String {
        &self.arr_station
    }
    pub fn frequency(&self) -> &String {
        &self.frequency
    }
    pub fn flight_time(&self) -> i64 {
        self.flight_time
    }
    pub fn dpt_station(&self) -> &String {
        &self.dpt_station
    }
}

#[derive(MultipartForm)]
pub struct SSIM {
    file: TempFile,
}

impl SSIM {
    pub fn file(&mut self) -> &mut TempFile {
        &mut self.file
    }
}

#[derive(Deserialize, Clone)]
pub struct DataBase {
    host: String,
    port: String,
    username: String,
    password: String,
    namespace: String,
    dbname: String,
}

impl DataBase {
    pub fn new(
        host: String,
        port: String,
        username: String,
        password: String,
        namespace: String,
        dbname: String,
    ) -> Self {
        DataBase {
            host,
            port,
            username,
            password,
            namespace,
            dbname,
        }
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
    pub fn namespace(&self) -> &String {
        &self.namespace
    }
}

#[derive(Deserialize)]
pub struct Log {
    level: String,
    file: String,
    pattern: String,
}

impl Log {
    pub fn new(level: String, file: String, pattern: String) -> Self {
        Log {
            level,
            file,
            pattern,
        }
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
pub struct Configuration {
    database: DataBase,
    log: Log,
}

impl Configuration {
    pub fn new(database: DataBase,log: Log) -> Self {
        Configuration {
            database,
            log,
        }
    }
    pub fn database(&self) -> &DataBase {
        &self.database
    }
    pub fn log(&self) -> &Log {
        &self.log
    }
}


#[derive(Deserialize)]
pub struct Airport{
    id: String,
    name: String,
    city: String,
    country: String,
    timezone: String,
    mct: usize,
}

impl Airport {
    pub fn new(id: String, name: String, city: String, country: String, timezone: String, mct: usize) -> Self {
        Self {
            id,
            name,
            city : "".to_string(),
            country: "".to_string(),
            timezone,
            mct : 1440
        }
    }
    pub fn id(&self) -> &String {
        &self.id
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn city(&self) -> &String {
        &self.city
    }
    pub fn country(&self) -> &String {
        &self.country
    }
    pub fn timezone(&self) -> &String {
        &self.timezone
    }
    pub fn mct(&self) -> &usize {
        &self.mct
    }
}
