use crate::domain::airport::AirportCode;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::Infrastructure::file_loader::oag_parser::FlightLegRecord;
use anyhow::{anyhow, Result};
use chrono::Datelike;
use chrono::{Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone};
use std::str::FromStr;

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
    pub arrival_day_offset: i64,
}

pub fn parse_line(line: &str) -> Option<FlightPlan> {
    if !line.starts_with('3') {
        return None;
    }

    let dep = NaiveTime::parse_from_str(&line[43..47], "%H%M").ok()?;
    let arr = NaiveTime::parse_from_str(&line[61..65], "%H%M").ok()?;
    let dep_date_var = line[192..193].chars().next()?;
    let arr_date_var = line[193..194].chars().next()?;
    let arrival_day_offset =
        relative_arrival_day_offset(dep_date_var, arr_date_var).ok()?;

    let block_time = compute_block_time_with_offsets(
        dep,
        line[47..52].trim(),
        arr,
        line[65..70].trim(),
        arrival_day_offset,
    )
    .ok()?;

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
        weekdays: build_weekdays(&line[28..35]),
        dep_tz: line[47..52].trim().to_string(),
        arr_tz: line[65..70].trim().to_string(),
        arrival_day_offset,
    })
}

impl TryFrom<FlightLegRecord> for FlightPlan {
    type Error = anyhow::Error;

    fn try_from(record: FlightLegRecord) -> Result<Self, Self::Error> {
        plan_from_leg_records(std::slice::from_ref(&record))
    }
}

pub fn plans_from_leg_records(records: &[FlightLegRecord]) -> Result<Vec<FlightPlan>> {
    let mut plans = Vec::new();

    for start in 0..records.len() {
        for end in start..records.len() {
            plans.push(plan_from_leg_records(&records[start..=end])?);
        }
    }

    Ok(plans)
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
    use crate::domain::airport::AirportCode;
    use chrono::{Duration, NaiveDate, NaiveTime};

    #[test]
    fn test_expand_limit_60_days() {
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
            arrival_day_offset: 0,
        };

        let rows = expand(&plan);
        // Expansion is capped at 60 days from the schedule start date.
        assert_eq!(
            rows.len(),
            60,
            "Should expand exactly 60 days when the end date exceeds the import cap"
        );
        assert_eq!(
            rows.first().unwrap().dep_local.date_naive(),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
        );
        assert_eq!(
            rows.last().unwrap().dep_local.date_naive(),
            NaiveDate::from_ymd_opt(2026, 3, 1).unwrap()
        );
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
            arrival_day_offset: 0,
        };

        let rows = expand(&plan);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn builds_single_leg_and_through_plans_for_multi_segment_flight() {
        let records = vec![
            FlightLegRecord {
                airline_designator: "CA".to_string(),
                flight_number: "865".to_string(),
                itinerary_variation: 1,
                leg_sequence: 1,
                service_type: "J".to_string(),
                valid_from: NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(),
                valid_to: NaiveDate::from_ymd_opt(2026, 3, 4).unwrap(),
                days_of_operation: "3".to_string(),
                departure_station: "PEK".to_string(),
                pax_std: NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
                aircraft_std: NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
                time_var_dep: "+0800".to_string(),
                arrival_station: "MAD".to_string(),
                aircraft_sta: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                pax_sta: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                time_var_arr: "+0100".to_string(),
                aircraft_type: "789".to_string(),
                aircraft_config: None,
                dep_date_var: '0',
                arr_date_var: '0',
                record_serial_number: 1,
            },
            FlightLegRecord {
                airline_designator: "CA".to_string(),
                flight_number: "865".to_string(),
                itinerary_variation: 1,
                leg_sequence: 2,
                service_type: "J".to_string(),
                valid_from: NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(),
                valid_to: NaiveDate::from_ymd_opt(2026, 3, 4).unwrap(),
                days_of_operation: "3".to_string(),
                departure_station: "MAD".to_string(),
                pax_std: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                aircraft_std: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                time_var_dep: "+0100".to_string(),
                arrival_station: "HAV".to_string(),
                aircraft_sta: NaiveTime::from_hms_opt(18, 15, 0).unwrap(),
                pax_sta: NaiveTime::from_hms_opt(18, 15, 0).unwrap(),
                time_var_arr: "-0500".to_string(),
                aircraft_type: "789".to_string(),
                aircraft_config: None,
                dep_date_var: '0',
                arr_date_var: '0',
                record_serial_number: 2,
            },
        ];

        let plans = plans_from_leg_records(&records).unwrap();
        assert_eq!(plans.len(), 3);

        assert_eq!(plans[0].origin.as_str(), "PEK");
        assert_eq!(plans[0].destination.as_str(), "MAD");

        assert_eq!(plans[1].origin.as_str(), "PEK");
        assert_eq!(plans[1].destination.as_str(), "HAV");
        assert_eq!(plans[1].block_time.num_minutes(), 1455);

        assert_eq!(plans[2].origin.as_str(), "MAD");
        assert_eq!(plans[2].destination.as_str(), "HAV");
    }
}

fn plan_from_leg_records(records: &[FlightLegRecord]) -> Result<FlightPlan> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot build a flight plan from an empty leg list"))?;
    let last = records.last().unwrap();

    for pair in records.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];

        if previous.airline_designator != next.airline_designator
            || previous.flight_number != next.flight_number
            || previous.itinerary_variation != next.itinerary_variation
            || previous.service_type != next.service_type
        {
            return Err(anyhow!(
                "legs do not belong to the same OAG itinerary: {}{} IVI {} leg {} -> {}{} IVI {} leg {}",
                previous.airline_designator,
                previous.flight_number,
                previous.itinerary_variation,
                previous.leg_sequence,
                next.airline_designator,
                next.flight_number,
                next.itinerary_variation,
                next.leg_sequence
            ));
        }

        if next.leg_sequence != previous.leg_sequence.saturating_add(1) {
            return Err(anyhow!(
                "non-contiguous leg sequence for {}{} IVI {}: expected {}, got {}",
                previous.airline_designator,
                previous.flight_number,
                previous.itinerary_variation,
                previous.leg_sequence.saturating_add(1),
                next.leg_sequence
            ));
        }

        if previous.arrival_station != next.departure_station {
            return Err(anyhow!(
                "leg station mismatch for {}{} IVI {}: {} does not connect to {}",
                previous.airline_designator,
                previous.flight_number,
                previous.itinerary_variation,
                previous.arrival_station,
                next.departure_station
            ));
        }
    }

    let dep_tz = first.time_var_dep.clone();
    let arr_tz = last.time_var_arr.clone();
    let arrival_day_offset = relative_arrival_day_offset(first.dep_date_var, last.arr_date_var)?;
    let dep_time = first.aircraft_std;
    let arr_time = last.pax_sta;

    Ok(FlightPlan {
        company: first.airline_designator.clone(),
        flight_no: first.flight_number.clone(),
        origin: AirportCode::new(first.departure_station.clone())
            .map_err(|_| anyhow!("invalid origin airport code {}", first.departure_station))?,
        destination: AirportCode::new(last.arrival_station.clone())
            .map_err(|_| anyhow!("invalid destination airport code {}", last.arrival_station))?,
        dep_time,
        arr_time,
        block_time: compute_block_time_with_offsets(
            dep_time,
            dep_tz.as_str(),
            arr_time,
            arr_tz.as_str(),
            arrival_day_offset,
        )?,
        start_date: first.valid_from,
        end_date: first.valid_to,
        weekdays: build_weekdays(&first.days_of_operation),
        dep_tz,
        arr_tz,
        arrival_day_offset,
    })
}

fn build_weekdays(days_of_operation: &str) -> [bool; 7] {
    let mut weekdays = [false; 7];
    for i in 0..7 {
        weekdays[i] = days_of_operation.contains(&(i + 1).to_string());
    }
    weekdays
}

fn relative_arrival_day_offset(dep_date_var: char, arr_date_var: char) -> Result<i64> {
    Ok(day_variation_to_offset(arr_date_var)? - day_variation_to_offset(dep_date_var)?)
}

fn day_variation_to_offset(value: char) -> Result<i64> {
    match value {
        ' ' | '0' => Ok(0),
        '1'..='9' => Ok((value as u8 - b'0') as i64),
        'A' | 'J' => Ok(-1),
        _ => Err(anyhow!("unsupported SSIM date variation '{value}'")),
    }
}

fn compute_block_time_with_offsets(
    dep_time: NaiveTime,
    dep_tz: &str,
    arr_time: NaiveTime,
    arr_tz: &str,
    arrival_day_offset: i64,
) -> Result<Duration> {
    let reference_date = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let dep_offset =
        FixedOffset::from_str(dep_tz).map_err(|_| anyhow!("invalid departure timezone {dep_tz}"))?;
    let arr_offset =
        FixedOffset::from_str(arr_tz).map_err(|_| anyhow!("invalid arrival timezone {arr_tz}"))?;

    let dep_local = dep_offset
        .from_local_datetime(&reference_date.and_time(dep_time))
        .single()
        .ok_or_else(|| anyhow!("invalid departure local datetime"))?;
    let arr_local = arr_offset
        .from_local_datetime(
            &(reference_date + Duration::days(arrival_day_offset)).and_time(arr_time),
        )
        .single()
        .ok_or_else(|| anyhow!("invalid arrival local datetime"))?;

    Ok(arr_local.to_utc() - dep_local.to_utc())
}
