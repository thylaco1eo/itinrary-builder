//extern crate chrono;
mod flight_info;
pub mod services;
use postgres::{Client, NoTls};
use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;
use serde_json;

#[derive(Deserialize)]
struct DataBase{
    host: String,
    user: String,
    port: String,
    password: String,
    dbname: String,
}

fn main() {
    //let mut path = String::new();
    //println!("Please enter the path to the flight data file:");
    //io::stdin().read_line(&mut path).expect("Failed to read line");
    // let dpt_apt: std::collections::HashMap<String, Vec<flight_info::FlightInfo>> = services::data_service::import_schedule_file("./data/cassim0401");
    // let request = "PEKFRA01MAY25+0800";
    // let path_list = services::search_service::search_flight::search_flight(&dpt_apt, request);
    // if path_list.is_empty() {
    //     println!("No flights found");
    // } else {
    //     for path in path_list {
    //         println!("Found path:");
    //         for (flt_id, dep_time, arr_station, flight_time) in path {
    //             println!("Flight ID: {}, Departure Time: {}, Arrival Station: {}, Flight Time: {} minutes", flt_id, dep_time, arr_station, flight_time);
    //         }
    //     }
    // }
    let mut file = File::open("sample.json").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let db_info:DataBase = serde_json::from_str(&contents).unwrap();
    let connection_string = format!("postgresql://{}:{}@{}:{}/{}", 
        db_info.user, db_info.password, db_info.host, db_info.port, db_info.dbname);
    let mut client = Client::connect(&connection_string, NoTls)
        .expect("Failed to connect to the database");
    let query = client.query("SELECT tablename FROM pg_tables", &[])
        .expect("Failed to execute query");
    println!("{:?}", query);
}