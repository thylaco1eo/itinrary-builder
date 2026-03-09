use chrono::Datelike;
use chrono::{NaiveDate, NaiveTime,Duration};
use crate::domain::airport::AirportCode;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::domain::time::compute_block_time;
use crate::Infrastructure::file_loader::oag_parser::FlightLegRecord;

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

impl TryFrom<FlightLegRecord> for FlightPlan {
    type Error = ();

    fn try_from(record: FlightLegRecord) -> Result<Self, Self::Error> {
        let mut weekdays = [false; 7];
        for i in 0..7 {
            weekdays[i] = record.days_of_operation.contains(&(i + 1).to_string());
        }
        Ok(FlightPlan{
            company:record.airline_designator,
            flight_no: record.flight_number,
            origin: AirportCode::new(record.departure_station).ok().unwrap(),
            destination: AirportCode::new(record.arrival_station).ok().unwrap(),
            dep_time: record.aircraft_std,
            arr_time: record.pax_sta,
            block_time: compute_block_time(record.aircraft_std, record.pax_sta),
            start_date: record.valid_from,
            end_date: record.valid_to,
            weekdays,
            dep_tz: record.time_var_dep,
            arr_tz: record.time_var_arr
        })
    }
}


pub fn expand(plan: &FlightPlan) -> Vec<FlightRow> {
    let mut rows = Vec::new();
    let mut date = plan.start_date;

    let limit_date = plan.start_date + Duration::days(60);
    let end_date = if plan.end_date < limit_date {
        plan.end_date
    } else {
        limit_date
    };

    while date <= end_date {
        let w = date.weekday().num_days_from_monday() as usize;
        if plan.weekdays[w] {
            rows.push(FlightRow::from_plan(plan, date));
        }
        date = date.succ_opt().unwrap();
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, Duration};
    use crate::domain::airport::AirportCode;

    #[test]
    fn test_expand_limit_30_days() {
        let plan = FlightPlan {
            company: "AA".to_string(),
            flight_no: "123".to_string(),
            origin: AirportCode::new("JFK").unwrap(),
            destination: AirportCode::new("LAX").unwrap(),
            dep_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            block_time: Duration::hours(3),
            start_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(), // 60 days
            weekdays: [true; 7],
            dep_tz: "+0000".to_string(),
            arr_tz: "+0000".to_string(),
        };

        let rows = expand(&plan);
        // Expansion from Jan 1st to Jan 31st (inclusive) is 31 days
        assert_eq!(rows.len(), 31, "Should expand exactly 31 days (start_date to start_date + 30 days)");
        assert_eq!(rows.first().unwrap().dep_local.date_naive(), NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
        assert_eq!(rows.last().unwrap().dep_local.date_naive(), NaiveDate::from_ymd_opt(2026, 1, 31).unwrap());
    }

    #[test]
    fn test_expand_less_than_30_days() {
        let plan = FlightPlan {
            company: "AA".to_string(),
            flight_no: "123".to_string(),
            origin: AirportCode::new("JFK").unwrap(),
            destination: AirportCode::new("LAX").unwrap(),
            dep_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            block_time: Duration::hours(3),
            start_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 1, 10).unwrap(), // 10 days
            weekdays: [true; 7],
            dep_tz: "+0000".to_string(),
            arr_tz: "+0000".to_string(),
        };

        let rows = expand(&plan);
        assert_eq!(rows.len(), 10);
    }
}

