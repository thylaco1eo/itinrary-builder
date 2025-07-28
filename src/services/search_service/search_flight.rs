use chrono::{FixedOffset, NaiveDate,TimeDelta,DateTime,Datelike};
use std::{collections::HashMap, str::FromStr};
use crate::flight_info;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct SearchInfo{}


pub fn search_flight(dpt_apt: &HashMap<String, Vec<flight_info::FlightInfo>>,request: &str) -> Vec<Vec<(String, DateTime<FixedOffset>, String, i64)>> {
    let dpt_station = &request[..3];
    let arr_station = &request[3..6];
    let dpt_date = NaiveDate::parse_from_str(&request[6..13], "%d%b%y").expect("Failed to parse date")
        .and_hms_opt(0,0,0).unwrap()
        .and_local_timezone(FixedOffset::from_str(&request[13..]).expect("Failed to parse timezone")).unwrap();
    let mut stack = Vec::new();
    let mut path_list = Vec::new();
    stack.push((dpt_station,dpt_date,0,vec![dpt_station.to_string()],Vec::new()));
    while !stack.is_empty(){
        let (current_station, current_date, stops,path,flight_taken) = stack.pop().unwrap();
        if current_station == arr_station {
            path_list.push(flight_taken.clone());
            continue;
        }
        if stops >= 2 {
            continue;
        }
        let flights = dpt_apt.get(current_station).unwrap();
        for flight in flights {
            if flight.dpt_start_local() <= &current_date && flight.dpt_end_local() >= &current_date && flight.frequency().contains(&(current_date.weekday().num_days_from_monday() as u8)) {
                let next_station = flight.arr_station();
                let dep_time = current_date.with_timezone(&flight.dpt_start_local().timezone()).with_time(flight.dpt_start_local().time()).unwrap();
                let mut min_connect_time = 60; // Minimum connection time in minutes
                let mut max_connect_time = 300; // Maximum connection time in minutes
                if path.len() == 1{
                    min_connect_time = 0;
                    max_connect_time = 1440; // Allow same day connections
                }
                if min_connect_time<= dep_time.signed_duration_since(current_date).num_minutes() && 
                   dep_time.signed_duration_since(current_date).num_minutes() <= max_connect_time {
                    let mut new_path = path.clone();
                    new_path.push(next_station.to_string());
                    let mut new_flight_taken = flight_taken.clone();
                    new_flight_taken.push((flight.fltid().clone(), dep_time, flight.arr_station().clone(), flight.flight_time()));
                    stack.push((next_station, dep_time.checked_add_signed(TimeDelta::minutes(flight.flight_time())).unwrap(), stops + 1, new_path,new_flight_taken));
                }
            }
        }
    }
    path_list
}