use chrono::Datelike;
use chrono::{NaiveDate, NaiveTime,Duration};
use crate::domain::airport::AirportCode;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::domain::time::compute_block_time;

pub struct FlightPlan {
    pub company: String,
    pub flight_no: String,
    pub origin: AirportCode,
    pub destination: AirportCode,
    pub dep_time: NaiveTime,
    pub arr_time: NaiveTime,
    pub block_time: Duration,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub weekdays: [bool; 7],
    pub dep_tz: String,
    pub arr_tz: String,
}


pub fn parse_line(line: &str) -> Option<FlightPlan> {
    if !line.starts_with('3') {
        return None;
    }

    let mut weekdays = [false; 7];
    for i in 0..7 {
        weekdays[i] = line[28..35].contains(&(i + 1).to_string());
    }

    let dep = NaiveTime::parse_from_str(&line[43..47], "%H%M").ok()?;
    let arr = NaiveTime::parse_from_str(&line[61..65], "%H%M").ok()?;

    let block_time = compute_block_time(dep, arr);

    Some(FlightPlan {
        company: line[2..5].trim().to_string(),
        flight_no: line[5..9].trim().to_string(),
        origin: AirportCode::new(line[36..39].trim()).ok()?,
        destination: AirportCode::new(line[54..57].trim()).ok()?,
        dep_time: dep,
        arr_time: arr,
        block_time,
        start_date: NaiveDate::parse_from_str(&line[14..21], "%d%b%y").ok()?,
        end_date: NaiveDate::parse_from_str(&line[21..28], "%d%b%y").ok()?,
        weekdays,
        dep_tz: line[47..52].trim().to_string(),
        arr_tz: line[65..70].trim().to_string(),
    })
}


pub fn expand(plan: &FlightPlan) -> Vec<FlightRow> {
    let mut rows = Vec::new();
    let mut date = plan.start_date;

    while date <= plan.end_date {
        let w = date.weekday().num_days_from_monday() as usize;
        if plan.weekdays[w] {
            rows.push(FlightRow::from_plan(plan, date));
        }
        date = date.succ_opt().unwrap();
    }
    rows
}

