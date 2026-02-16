use chrono::{NaiveDate, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use crate::domain::airport::AirportCode;
use crate::domain::flight::Flight;
use crate::domain::flightplan::FlightPlan;
use surrealdb::types::{Kind, SurrealValue, Value};

#[derive(Debug)]
pub enum FlightRowError {
    InvalidOriginCode(crate::domain::airport::AirportCodeError),
    InvalidDestinationCode(crate::domain::airport::AirportCodeError),
    InvalidBlockTime,
}

/// 数据库或 CSV 行结构
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlightRow {
    pub flight_key: String,
    pub company: String,
    pub origin_code: String,
    pub destination_code: String,
    pub dep_local: NaiveDateTime,
    pub arr_local: NaiveDateTime,
    pub block_time_minutes: u32, // 持久化为分钟
}

impl SurrealValue for FlightRow {
    fn kind_of() -> Kind {
        Kind::Object
    }

    fn is_value(value: &Value) -> bool {
        matches!(value, Value::Object(_))
    }

    fn into_value(self) -> Value {
        serde_json::from_value(serde_json::to_value(self).unwrap_or_default()).unwrap_or(Value::None)
    }

    fn from_value(value: Value) -> surrealdb::types::anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(serde_json::from_value(serde_json::to_value(value)?)?)
    }
}


impl TryFrom<FlightRow> for Flight {
    type Error = FlightRowError;

    fn try_from(row: FlightRow) -> Result<Self, Self::Error> {
        let origin = AirportCode::new(row.origin_code)
            .map_err(FlightRowError::InvalidOriginCode)?;
        let destination = AirportCode::new(row.destination_code)
            .map_err(FlightRowError::InvalidDestinationCode)?;

        if row.block_time_minutes == 0 {
            return Err(FlightRowError::InvalidBlockTime);
        }

        Ok(Flight::new(
            row.company,
            origin,
            destination,
            row.dep_local,
            row.arr_local,
            chrono::Duration::minutes(row.block_time_minutes as i64),
        ))
    }
}

impl FlightRow {
    pub fn from_plan(flight_plan: &FlightPlan,date: NaiveDate) -> Self{
        let id_str = format!(
            "{}_{}_{}_{}_{}",
            flight_plan.company,
            flight_plan.flight_no,
            flight_plan.origin.as_str(),
            flight_plan.destination.as_str(),
            date.format("%Y-%m-%d")
        );
        FlightRow{
            flight_key: id_str,
            company: flight_plan.company.clone(),
            origin_code: flight_plan.origin.as_str().to_string(),
            destination_code: flight_plan.destination.as_str().to_string(),
            dep_local: date.and_hms_opt(flight_plan.dep_time.hour(), flight_plan.dep_time.minute(), 0).unwrap(),
            arr_local: date.and_hms_opt(flight_plan.arr_time.hour(), flight_plan.arr_time.minute(), 0).unwrap(),
            block_time_minutes: flight_plan.block_time.num_minutes() as u32,
        }
    }
}