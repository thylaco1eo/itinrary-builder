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
    state: Option<String>,
    location: Point,
    timezone: Tz,
}

impl Airport {
    pub fn new_minimal(id: AirportCode, timezone: Tz, longitude: f64, latitude: f64) -> Self {
        Self {
            id,
            timezone,
            name: None,
            city: None,
            country: None,
            state: None,
            location: Point::new(longitude, latitude),
        }
    }

    pub fn new_full(
        id: AirportCode,
        timezone: Tz,
        name: Option<String>,
        city: Option<String>,
        country: Option<String>,
        state: Option<String>,
        longitude: f64,
        latitude: f64,
    ) -> Self {
        Self {
            id,
            timezone,
            name,
            city,
            country,
            state,
            location: Point::new(longitude, latitude),
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

    pub fn state(&self) -> Option<&str> {
        self.state.as_deref()
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
}
