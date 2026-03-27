use chrono_tz::Tz;
use geo::Point;
#[derive(Debug)]
pub enum AirportCodeError {
    InvalidIata(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AirportCode(String);

impl AirportCode {
    pub fn new(code: impl Into<String>) -> Result<Self, AirportCodeError> {
        let code = code.into().to_uppercase();
        if code.len() != 3 || !code.chars().all(|c| c.is_ascii_uppercase()) {
            return Err(AirportCodeError::InvalidIata(code));
        }
        Ok(Self(code))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct Airport {
    id: AirportCode,
    name: Option<String>,
    city: Option<String>,
    country: Option<String>,
    location: Point,
    timezone: Tz,
    minimum_connection_time: u32,
}

impl Airport {
    pub fn new_minimal(id: AirportCode, timezone: Tz, longitude: f64, latitude: f64) -> Self {
        Self {
            id,
            timezone,
            name: None,
            city: None,
            country: None,
            location: Point::new(longitude, latitude),
            minimum_connection_time: 180, // 你当前的业务假设
        }
    }

    pub fn new_full(
        id: AirportCode,
        timezone: Tz,
        name: Option<String>,
        city: Option<String>,
        country: Option<String>,
        longitude: f64,
        latitude: f64,
        minimum_connection_time: Option<u32>,
    ) -> Self {
        Self {
            id,
            timezone,
            name,
            city,
            country,
            location: Point::new(longitude, latitude),
            minimum_connection_time: minimum_connection_time.unwrap_or(180),
        }
    }
    pub fn id(&self) -> &AirportCode {
        &self.id
    }
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn city(&self) -> Option<&str> {
        self.city.as_deref()
    }

    pub fn country(&self) -> Option<&str> {
        self.country.as_deref()
    }
    pub fn timezone(&self) -> Tz {
        self.timezone
    }
    pub fn latitude(&self) -> f64 {
        self.location.0.y
    }
    pub fn longitude(&self) -> f64 {
        self.location.0.x
    }
    pub fn minimum_connection_time(&self) -> u32 {
        self.minimum_connection_time
    }
}
