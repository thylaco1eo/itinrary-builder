use std::collections::HashMap;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use crate::flight_info;
use std::{fs, str::FromStr};

pub fn import_schedule_file(path: &str) -> HashMap<String, Vec<flight_info::FlightInfo>> {
    let contents = fs::read_to_string(path).expect("Should have been able to read the file");
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
            let flt = flight_info::FlightInfo::new(fltid.clone(), dpt_start_local, dpt_end_local, dpt_station.clone() ,arr_station, frequency.collect(), flight_time);
            dpt_apt.entry(dpt_station).and_modify(|e| e.push(flt.clone())).or_insert(vec![flt]);
        }
    }
    dpt_apt
}


// pub fn cache_refresh(path: &str) {
//     let dpt_apt = import_schedule_file(path);
//     let cache_path = "./data/cache.json";
//     let json_data = serde_json::to_string(&dpt_apt).expect("Failed to serialize data");
//     fs::write(cache_path, json_data).expect("Failed to write cache file");
// }