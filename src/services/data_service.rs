use std::fs::File;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, Utc};
use crate::structure::{FlightInfo, WebData,Airport};
use std::str::FromStr;
use std::io::Read;
use actix_web::{web, Responder, HttpResponse};
use crate::db::*;

pub fn import_schedule_file(file: &mut File)->Vec<FlightInfo>{
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Should have been able to read the file");
    let mut flights: Vec<FlightInfo> = Vec::new();
    for lines in contents.lines(){
        if lines.as_bytes()[0usize] == '3' as u8 {
            let flt_id: String = lines[5..9].chars().filter(|c| !c.is_whitespace()).collect::<String>().clone();
            let carrier:String = lines[2..5].chars().filter(|c| !c.is_whitespace()).collect::<String>().clone();
            let start_date = NaiveDate::parse_from_str(&lines[14..21], "%d%b%y").expect("Failed to parse date");
            let end_date = NaiveDate::parse_from_str(&lines[21..28], "%d%b%y").expect("Failed to parse date");
            let frequency = (1..=7).map(|i| if lines[28..35].contains(&i.to_string()) { '1' } else { '0' }).collect();
            let dpt_station = lines[36..39].chars().collect::<String>();
            let arr_station = lines[54..57].chars().collect::<String>();
            let dpt_local = NaiveTime::parse_from_str(&lines[43..47], "%H%M").expect("Failed to parse time");
            let arr_local = NaiveTime::parse_from_str(&lines[61..65], "%H%M").expect("Failed to parse time");
            let dpt_start_utc: DateTime<Utc> = start_date.and_time(dpt_local).and_local_timezone(FixedOffset::from_str(&lines[47..52]).expect("Failed to parse timezone")).unwrap().to_utc();
            let dpt_end_utc:DateTime<Utc> = end_date.and_time(dpt_local).and_local_timezone(FixedOffset::from_str(&lines[47..52]).expect("Failed to parse timezone")).unwrap().to_utc();
            let flight_time = if dpt_start_utc.with_timezone(&FixedOffset::from_str(&lines[65..70]).expect("Failed to parse timezone"))
                .with_time(arr_local).unwrap().signed_duration_since(dpt_start_utc).num_minutes()< 0 {
                    dpt_start_utc.with_timezone(&FixedOffset::from_str(&lines[65..70]).expect("Failed to parse timezone"))
                .with_time(arr_local).unwrap().signed_duration_since(dpt_start_utc).num_minutes() + 1440
                }else {
                    dpt_start_utc.with_timezone(&FixedOffset::from_str(&lines[65..70]).expect("Failed to parse timezone"))
                .with_time(arr_local).unwrap().signed_duration_since(dpt_start_utc).num_minutes()
                };
            let flt = FlightInfo::new(flt_id,carrier, dpt_start_utc, dpt_end_utc, dpt_station ,arr_station, frequency, flight_time);
            flights.push(flt);
            //dpt_apt.entry(dpt_station).and_modify(|e| e.push(flt.clone())).or_insert(vec![flt]);
        }
    }
    flights
}


// pub fn cache_refresh(path: &str) {
//     let dpt_apt = import_schedule_file(path);
//     let cache_path = "./data/cache.json";
//     let json_data = serde_json::to_string(&dpt_apt).expect("Failed to serialize data");
//     fs::write(cache_path, json_data).expect("Failed to write cache file");
// }

pub async fn creat_airport(data: web::Data<WebData>, form: web::Form<Airport>) -> impl Responder {
    let result = create_airport_neo4j(data.database(), form.0).await;
    match result {
        Ok(created) => {
            if created {
                HttpResponse::Created().body("Airport created successfully")
            } else {
                HttpResponse::Conflict().body("Airport already exists")
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("Error creating airport: {}", e))
    }
}