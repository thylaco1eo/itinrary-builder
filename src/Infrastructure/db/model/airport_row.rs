use serde::{Deserialize, Serialize};
use crate::domain::airport::{Airport, AirportCode,AirportCodeError};
use surrealdb::types::{Geometry, Kind, SurrealValue, Value};
use surrealdb::types::Kind::Object;
use crate::Infrastructure::db::model::flight_row::FlightRow;

#[derive(Serialize, Deserialize, Clone)]
#[serde(transparent)]
pub struct AirportCodeRow {
    pub code: String,
}

impl TryFrom<AirportCodeRow> for AirportCode {
    type Error = AirportCodeError;
    fn try_from(row: AirportCodeRow) -> Result<Self, AirportCodeError> {
        AirportCode::new(row.code)
    }
}

#[derive(Serialize,Deserialize,Clone)]
pub struct AirportRow {
    pub code: AirportCodeRow,
    pub timezone: String,
    pub name: Option<String>,
    pub city: Option<String>,
    pub country: Option<String>,
    pub location:Geometry,
    pub mct: Option<u32>,
}


#[derive(Debug)]
pub enum AirportRowError {
    InvalidCode(AirportCodeError),
    InvalidTimezone(chrono_tz::ParseError),
    InvalidLatitude,
    InvalidLongitude,
    InvalidLocationType
}

impl From<AirportCodeError> for AirportRowError {
    fn from(e: AirportCodeError) -> Self {
        AirportRowError::InvalidCode(e)
    }
}

impl From<chrono_tz::ParseError> for AirportRowError {
    fn from(e: chrono_tz::ParseError) -> Self {
        AirportRowError::InvalidTimezone(e)
    }
}


impl TryFrom<AirportRow> for Airport {
    type Error = AirportRowError;

    fn try_from(row: AirportRow) -> Result<Self, Self::Error> {
        let point = match row.location {
            Geometry::Point(p) => p,
            _ => return Err(AirportRowError::InvalidLocationType),
        };
        let longitude = point.x();
        let latitude = point.y();

        if latitude < -90.0 || latitude > 90.0 {
            return Err(AirportRowError::InvalidLatitude);
        }
        if longitude < -180.0 || longitude > 180.0 {
            return Err(AirportRowError::InvalidLongitude);
        }

        Ok(Airport::new_full(
            AirportCode::new(row.code.code)?,
            row.timezone.parse()?,
            row.name,
            row.city,
            row.country,
            latitude,
            longitude,
            row.mct
        ))
    }
}

impl SurrealValue for AirportRow {
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