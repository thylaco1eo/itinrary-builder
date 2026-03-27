use crate::domain::airport::AirportCode;
use crate::domain::flight::Flight;
use crate::domain::flightplan::FlightPlan;
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use surrealdb_types::{RecordId, SurrealValue};

#[derive(Debug)]
pub enum FlightRowError {
    InvalidOriginCode(crate::domain::airport::AirportCodeError),
    InvalidDestinationCode(crate::domain::airport::AirportCodeError),
    InvalidBlockTime,
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
                .from_local_datetime(&date.and_time(flight_plan.arr_time))
                .single()
                .map(|dt| dt.to_utc())
                .unwrap(),
            block_time_minutes: flight_plan.block_time.num_minutes() as u32,
        }
    }
}
