use crate::domain::airport::AirportCode;
use crate::domain::flight::Flight;
use crate::domain::flightplan::FlightPlan;
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use surrealdb_types::{RecordId, SurrealValue};

#[derive(Debug)]
pub enum FlightRowError {
    InvalidOriginCode(crate::domain::airport::AirportCodeError),
    InvalidDestinationCode(crate::domain::airport::AirportCodeError),
    InvalidBlockTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue, PartialEq, Eq)]
pub struct FlightDesignatorRow {
    pub company: String,
    pub flight_number: String,
    pub operational_suffix: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue, PartialEq, Eq)]
pub struct FlightType3LegRow {
    pub leg_sequence: u8,
    pub departure_station: String,
    pub arrival_station: String,
    pub departure_terminal: Option<String>,
    pub arrival_terminal: Option<String>,
    pub prbd: Option<String>,
    pub prbm: Option<String>,
    pub meal_service_note: Option<String>,
    #[serde(default)]
    pub joint_operation_airline_designators: Vec<String>,
    pub secure_flight_indicator: Option<String>,
    pub itinerary_variation_overflow: Option<String>,
    pub aircraft_owner: Option<String>,
    pub cockpit_crew_employer: Option<String>,
    pub cabin_crew_employer: Option<String>,
    pub onward_airline_designator: Option<String>,
    pub onward_flight_number: Option<String>,
    pub onward_aircraft_rotation_layover: Option<String>,
    pub onward_operational_suffix: Option<String>,
    pub operating_airline_disclosure: Option<String>,
    pub traffic_restriction_code: Option<String>,
    pub traffic_restriction_code_leg_overflow_indicator: Option<String>,
    pub operating_designator: FlightDesignatorRow,
    #[serde(default)]
    pub duplicate_designators: Vec<FlightDesignatorRow>,
    pub in_flight_service_info: Option<String>,
    pub electronic_ticketing_info: Option<String>,
}

/// 数据库或 CSV 行结构
#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
pub struct FlightRow {
    pub id: RecordId,
    pub company: String,
    pub flight_num: String,
    pub origin_code: String,
    pub destination_code: String,
    pub dep_local: DateTime<Utc>,
    pub arr_local: DateTime<Utc>,
    pub block_time_minutes: u32, // 持久化为分钟
    #[serde(default)]
    pub departure_terminal: Option<String>,
    #[serde(default)]
    pub arrival_terminal: Option<String>,
    pub operating_designator: FlightDesignatorRow,
    #[serde(default)]
    pub duplicate_designators: Vec<FlightDesignatorRow>,
    #[serde(default)]
    pub joint_operation_airline_designators: Vec<String>,
    pub meal_service_note: Option<String>,
    pub in_flight_service_info: Option<String>,
    pub electronic_ticketing_info: Option<String>,
    #[serde(default)]
    pub type3_legs: Vec<FlightType3LegRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
pub struct FlightCacheRow {
    pub company: String,
    pub flight_num: String,
    pub origin_code: String,
    pub destination_code: String,
    pub dep_local: DateTime<Utc>,
    pub arr_local: DateTime<Utc>,
    pub block_time_minutes: u32,
    #[serde(default)]
    pub departure_terminal: Option<String>,
    #[serde(default)]
    pub arrival_terminal: Option<String>,
    pub operating_designator: FlightDesignatorRow,
    #[serde(default)]
    pub duplicate_designators: Vec<FlightDesignatorRow>,
    #[serde(default)]
    pub joint_operation_airline_designators: Vec<String>,
    pub meal_service_note: Option<String>,
    pub in_flight_service_info: Option<String>,
    pub electronic_ticketing_info: Option<String>,
}

impl TryFrom<FlightRow> for Flight {
    type Error = FlightRowError;

    fn try_from(row: FlightRow) -> Result<Self, Self::Error> {
        let origin =
            AirportCode::new(row.origin_code).map_err(FlightRowError::InvalidOriginCode)?;
        let destination = AirportCode::new(row.destination_code)
            .map_err(FlightRowError::InvalidDestinationCode)?;

        if row.block_time_minutes == 0 {
            return Err(FlightRowError::InvalidBlockTime);
        }

        Ok(Flight::new(
            row.company,
            row.flight_num,
            origin,
            destination,
            row.dep_local,
            row.arr_local,
            chrono::Duration::minutes(row.block_time_minutes as i64),
        ))
    }
}

impl FlightRow {
    pub fn from_plan(flight_plan: &FlightPlan, date: NaiveDate) -> Self {
        let id_str = format!(
            "{}_{}_{}_{}_{}",
            flight_plan.company,
            flight_plan.flight_no,
            flight_plan.origin.as_str(),
            flight_plan.destination.as_str(),
            date.format("%Y-%m-%d")
        );
        let dep_offset = FixedOffset::from_str(flight_plan.dep_tz.as_str()).unwrap();
        let arr_offset = FixedOffset::from_str(flight_plan.arr_tz.as_str()).unwrap();
        FlightRow {
            id: RecordId::new("flight", id_str.as_str()),
            company: flight_plan.company.clone(),
            flight_num: flight_plan.flight_no.clone(),
            origin_code: flight_plan.origin.as_str().to_string(),
            destination_code: flight_plan.destination.as_str().to_string(),
            dep_local: dep_offset
                .from_local_datetime(&date.and_time(flight_plan.dep_time))
                .single()
                .map(|dt| dt.to_utc())
                .unwrap(),
            arr_local: arr_offset
                .from_local_datetime(
                    &(date + Duration::days(flight_plan.arrival_day_offset))
                        .and_time(flight_plan.arr_time),
                )
                .single()
                .map(|dt| dt.to_utc())
                .unwrap(),
            block_time_minutes: flight_plan.block_time.num_minutes() as u32,
            departure_terminal: flight_plan
                .type3_legs
                .first()
                .and_then(|leg| leg.departure_terminal.clone()),
            arrival_terminal: flight_plan
                .type3_legs
                .last()
                .and_then(|leg| leg.arrival_terminal.clone()),
            operating_designator: flight_plan.operating_designator.clone(),
            duplicate_designators: flight_plan.duplicate_designators.clone(),
            joint_operation_airline_designators: flight_plan
                .joint_operation_airline_designators
                .clone(),
            meal_service_note: flight_plan.meal_service_note.clone(),
            in_flight_service_info: flight_plan.in_flight_service_info.clone(),
            electronic_ticketing_info: flight_plan.electronic_ticketing_info.clone(),
            type3_legs: flight_plan.type3_legs.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::flightplan::FlightPlan;
    use chrono::{Duration, NaiveDate, NaiveTime};

    #[test]
    fn row_carries_type3_leg_metadata() {
        let plan = FlightPlan {
            company: "CA".to_string(),
            flight_no: "897".to_string(),
            origin: AirportCode::new("PEK").unwrap(),
            destination: AirportCode::new("GRU").unwrap(),
            dep_time: NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(5, 5, 0).unwrap(),
            block_time: Duration::minutes(1500),
            start_date: NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            weekdays: [false, false, false, true, false, false, false],
            frequency_rate: None,
            dep_tz: "+0800".to_string(),
            arr_tz: "-0300".to_string(),
            arrival_day_offset: 1,
            operating_designator: FlightDesignatorRow {
                company: "UA".to_string(),
                flight_number: "551".to_string(),
                operational_suffix: None,
            },
            duplicate_designators: vec![FlightDesignatorRow {
                company: "CA".to_string(),
                flight_number: "897".to_string(),
                operational_suffix: None,
            }],
            joint_operation_airline_designators: vec!["XB".to_string(), "XA".to_string()],
            meal_service_note: Some("M M M".to_string()),
            in_flight_service_info: Some("9".to_string()),
            electronic_ticketing_info: Some("ET".to_string()),
            type3_legs: vec![
                FlightType3LegRow {
                    leg_sequence: 1,
                    departure_station: "PEK".to_string(),
                    arrival_station: "MAD".to_string(),
                    departure_terminal: Some("T3".to_string()),
                    arrival_terminal: Some("T1".to_string()),
                    prbd: Some("JCD".to_string()),
                    prbm: Some("MBMHM".to_string()),
                    meal_service_note: Some("M M M".to_string()),
                    joint_operation_airline_designators: vec!["XB".to_string()],
                    secure_flight_indicator: Some("S".to_string()),
                    itinerary_variation_overflow: Some("A".to_string()),
                    aircraft_owner: Some("CA".to_string()),
                    cockpit_crew_employer: Some("CCA".to_string()),
                    cabin_crew_employer: Some("CCB".to_string()),
                    onward_airline_designator: Some("LH".to_string()),
                    onward_flight_number: Some("1234".to_string()),
                    onward_aircraft_rotation_layover: Some("1".to_string()),
                    onward_operational_suffix: Some("Z".to_string()),
                    operating_airline_disclosure: Some("L".to_string()),
                    traffic_restriction_code: Some("A".to_string()),
                    traffic_restriction_code_leg_overflow_indicator: Some("Z".to_string()),
                    operating_designator: FlightDesignatorRow {
                        company: "UA".to_string(),
                        flight_number: "551".to_string(),
                        operational_suffix: None,
                    },
                    duplicate_designators: vec![FlightDesignatorRow {
                        company: "CA".to_string(),
                        flight_number: "897".to_string(),
                        operational_suffix: None,
                    }],
                    in_flight_service_info: Some("9".to_string()),
                    electronic_ticketing_info: Some("ET".to_string()),
                },
                FlightType3LegRow {
                    leg_sequence: 2,
                    departure_station: "MAD".to_string(),
                    arrival_station: "GRU".to_string(),
                    departure_terminal: Some("T2".to_string()),
                    arrival_terminal: Some("T5".to_string()),
                    prbd: Some("Z".to_string()),
                    prbm: None,
                    meal_service_note: None,
                    joint_operation_airline_designators: vec!["XA".to_string()],
                    secure_flight_indicator: None,
                    itinerary_variation_overflow: None,
                    aircraft_owner: None,
                    cockpit_crew_employer: None,
                    cabin_crew_employer: None,
                    onward_airline_designator: None,
                    onward_flight_number: None,
                    onward_aircraft_rotation_layover: None,
                    onward_operational_suffix: None,
                    operating_airline_disclosure: Some("S".to_string()),
                    traffic_restriction_code: Some("K".to_string()),
                    traffic_restriction_code_leg_overflow_indicator: None,
                    operating_designator: FlightDesignatorRow {
                        company: "CA".to_string(),
                        flight_number: "897".to_string(),
                        operational_suffix: None,
                    },
                    duplicate_designators: vec![],
                    in_flight_service_info: None,
                    electronic_ticketing_info: Some("ET".to_string()),
                },
            ],
        };

        let row = FlightRow::from_plan(&plan, plan.start_date);

        assert_eq!(row.departure_terminal.as_deref(), Some("T3"));
        assert_eq!(row.arrival_terminal.as_deref(), Some("T5"));
        assert_eq!(row.type3_legs.len(), 2);
        assert_eq!(row.type3_legs[0].prbd.as_deref(), Some("JCD"));
        assert_eq!(
            row.type3_legs[1].traffic_restriction_code.as_deref(),
            Some("K")
        );
        assert_eq!(row.operating_designator.company, "UA");
        assert_eq!(row.in_flight_service_info.as_deref(), Some("9"));
        assert_eq!(row.electronic_ticketing_info.as_deref(), Some("ET"));
    }
}
