extern crate chrono;
use std::path;
use std::{fs, str::FromStr};
use std::collections::HashMap;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime,Datelike,TimeDelta};
mod flight_info;

fn main() {
    let contents = fs::read_to_string("./data/cassim0401").expect("Should have been able to read the file");
    let mut dpt_apt: HashMap<String, Vec<flight_info::FlightInfo>> = HashMap::new();
    for lines in contents.lines(){
        if lines.as_bytes()[0 as usize] == '3' as u8 {
            let fltid: String = lines[5..9].chars().filter(|c| !c.is_whitespace()).collect::<String>().clone();
            let start_date = NaiveDate::parse_from_str(&lines[14..21], "%d%b%y").expect("Failed to parse date");
            let end_date = NaiveDate::parse_from_str(&lines[21..28], "%d%b%y").expect("Failed to parse date");
            let frequency = lines[28..35].chars().filter_map(|c| c.to_digit(10).map(|d| d as u8));
            let dpt_station = lines[36..39].chars().collect::<String>();
            let arr_station = lines[54..57].chars().collect::<String>();
            let dpt_local = NaiveTime::parse_from_str(&lines[43..47], "%H%M").expect("Failed to parse time");
            let arr_local = NaiveTime::parse_from_str(&lines[61..65], "%H%M").expect("Failed to parse time");
            let dpt_start_local: DateTime<FixedOffset> = start_date.and_time(dpt_local).and_local_timezone(FixedOffset::from_str(&lines[47..52]).expect("Failed to parse timezone")).unwrap();
            let dpt_end_local:DateTime<FixedOffset> = end_date.and_time(dpt_local).and_local_timezone(FixedOffset::from_str(&lines[47..52]).expect("Failed to parse timezone")).unwrap();
            let flight_time = if dpt_start_local.with_timezone(&FixedOffset::from_str(&lines[65..70]).expect("Failed to parse timezone"))
                .with_time(arr_local).unwrap().signed_duration_since(dpt_start_local).num_minutes()< 0 {
                    dpt_start_local.with_timezone(&FixedOffset::from_str(&lines[65..70]).expect("Failed to parse timezone"))
                .with_time(arr_local).unwrap().signed_duration_since(dpt_start_local).num_minutes() + 1440
                }else {
                    dpt_start_local.with_timezone(&FixedOffset::from_str(&lines[65..70]).expect("Failed to parse timezone"))
                .with_time(arr_local).unwrap().signed_duration_since(dpt_start_local).num_minutes()
                };
            let flt = flight_info::FlightInfo::new(fltid.clone(), dpt_start_local, dpt_end_local, arr_station, frequency.collect(), flight_time);
            dpt_apt.entry(dpt_station.clone()).and_modify(|e| e.push(flt.clone())).or_insert(vec![flt]);
        }
    }
    let request = "PEKFRA01MAY25+0800";
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
    if path_list.is_empty() {
        println!("No flights found from {} to {}", dpt_station, arr_station);
    } else {
        for path in path_list {
            println!("Found path:");
            for (flt_id, dep_time, arr_station, flight_time) in path {
                println!("Flight ID: {}, Departure Time: {}, Arrival Station: {}, Flight Time: {} minutes", flt_id, dep_time, arr_station, flight_time);
            }
        }
        
    }
}
