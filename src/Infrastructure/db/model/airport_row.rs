use crate::domain::airport::{Airport, AirportCode, AirportCodeError};
use crate::domain::mct::{
    AirportMctRecord, ConnectionBuildingFilter, ensure_airport_default_mct_records,
};
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
    pub state: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub mct_records: Vec<AirportMctRecord>,
    #[serde(default)]
    pub connection_building_filters: Vec<ConnectionBuildingFilter>,
}

#[derive(Debug)]
pub enum AirportRowError {
    InvalidCode(AirportCodeError),
    InvalidTimezone(chrono_tz::ParseError),
    InvalidLatitude,
    InvalidLongitude,
}

impl std::fmt::Display for AirportRowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCode(_) => write!(f, "invalid airport code"),
            Self::InvalidTimezone(_) => write!(f, "invalid airport timezone"),
            Self::InvalidLatitude => write!(f, "invalid airport latitude"),
            Self::InvalidLongitude => write!(f, "invalid airport longitude"),
        }
    }
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

        let mct_records = ensure_airport_default_mct_records(row.mct_records, None);

        Ok(Airport::new_full(
            AirportCode::new(row.code.code)?,
            row.timezone.parse()?,
            row.name,
            row.city,
            row.country,
            row.state,
            row.longitude,
            row.latitude,
            mct_records,
            row.connection_building_filters,
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
