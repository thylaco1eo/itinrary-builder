use crate::domain::airport::{Airport, AirportCode, AirportCodeError};
use serde::{Deserialize, Serialize};
use surrealdb::types::{Kind, SurrealValue, Value};

#[derive(Serialize, Deserialize, Clone, SurrealValue)]
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

#[derive(Serialize, Deserialize, Clone)]
pub struct AirportRow {
    pub code: AirportCodeRow,
    pub timezone: String,
    pub name: Option<String>,
    pub city: Option<String>,
    pub country: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub mct: Option<u32>,
}

#[derive(Debug)]
pub enum AirportRowError {
    InvalidCode(AirportCodeError),
    InvalidTimezone(chrono_tz::ParseError),
    InvalidLatitude,
    InvalidLongitude,
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
        if row.latitude < -90.0 || row.latitude > 90.0 {
            return Err(AirportRowError::InvalidLatitude);
        }
        if row.longitude < -180.0 || row.longitude > 180.0 {
            return Err(AirportRowError::InvalidLongitude);
        }

        Ok(Airport::new_full(
            AirportCode::new(row.code.code)?,
            row.timezone.parse()?,
            row.name,
            row.city,
            row.country,
            row.latitude,
            row.longitude,
            row.mct,
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
        match serde_json::to_value(self) {
            Ok(v) => match v {
                serde_json::Value::Object(o) => Value::Object(
                    o.into_iter()
                        .map(|(k, v)| (k, serde_json::from_value(v).unwrap_or(Value::None)))
                        .collect(),
                ),
                _ => Value::None,
            },
            Err(_) => Value::None,
        }
    }

    fn from_value(value: Value) -> surrealdb::types::anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(serde_json::from_value(serde_json::to_value(value)?)?)
    }
}
