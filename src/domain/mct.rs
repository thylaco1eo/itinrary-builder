use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};
use surrealdb::types::SurrealValue;

pub const DEFAULT_AIRPORT_MCT_MINUTES: u32 = 180;
const DEFAULT_AIRPORT_MCT_STATUSES: [&str; 4] = ["DD", "DI", "ID", "II"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MctContentIndicator {
    Full,
    UpdatesOnly,
}

impl MctContentIndicator {
    pub fn from_code(value: &str) -> Result<Self> {
        match value {
            "F" => Ok(Self::Full),
            "U" => Ok(Self::UpdatesOnly),
            _ => Err(anyhow!("invalid MCT content indicator: {value}")),
        }
    }

    pub fn as_code(&self) -> &'static str {
        match self {
            Self::Full => "F",
            Self::UpdatesOnly => "U",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MctActionIndicator {
    Add,
    Delete,
}

impl MctActionIndicator {
    pub fn from_code(value: &str) -> Result<Self> {
        match value {
            "A" => Ok(Self::Add),
            "D" => Ok(Self::Delete),
            _ => Err(anyhow!("invalid MCT action indicator: {value}")),
        }
    }

    pub fn as_code(&self) -> &'static str {
        match self {
            Self::Add => "A",
            Self::Delete => "D",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MctHeaderRecord {
    pub title_of_contents: String,
    pub creator_reference: String,
    pub creation_date_utc: String,
    pub creation_time_utc: String,
    pub content_indicator: MctContentIndicator,
    pub record_serial_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MctTrailerRecord {
    pub end_code: String,
    pub serial_number_check_reference: u32,
    pub record_serial_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SurrealValue)]
pub struct ConnectionBuildingFilter {
    pub submitting_carrier: String,
    pub partner_carrier_codes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, SurrealValue)]
pub struct GlobalMctData {
    #[serde(default)]
    pub mct_records: Vec<AirportMctRecord>,
    #[serde(default)]
    pub connection_building_filters: Vec<ConnectionBuildingFilter>,
}

pub type AirportMctData = GlobalMctData;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SurrealValue)]
pub struct AirportMctRecord {
    pub arrival_station: Option<String>,
    pub time: Option<String>,
    pub status: String,
    pub departure_station: Option<String>,
    #[serde(default)]
    pub requires_connection_building_filter: bool,
    pub arrival_carrier: Option<String>,
    #[serde(default)]
    pub arrival_codeshare_indicator: bool,
    pub arrival_codeshare_operating_carrier: Option<String>,
    pub departure_carrier: Option<String>,
    #[serde(default)]
    pub departure_codeshare_indicator: bool,
    pub departure_codeshare_operating_carrier: Option<String>,
    pub arrival_aircraft_type: Option<String>,
    pub arrival_aircraft_body: Option<String>,
    pub departure_aircraft_type: Option<String>,
    pub departure_aircraft_body: Option<String>,
    pub arrival_terminal: Option<String>,
    pub departure_terminal: Option<String>,
    pub previous_country: Option<String>,
    pub previous_station: Option<String>,
    pub next_country: Option<String>,
    pub next_station: Option<String>,
    pub arrival_flight_number_range_start: Option<String>,
    pub arrival_flight_number_range_end: Option<String>,
    pub departure_flight_number_range_start: Option<String>,
    pub departure_flight_number_range_end: Option<String>,
    pub previous_state: Option<String>,
    pub next_state: Option<String>,
    pub previous_region: Option<String>,
    pub next_region: Option<String>,
    pub effective_from_local: Option<String>,
    pub effective_to_local: Option<String>,
    #[serde(default)]
    pub suppression_indicator: bool,
    pub suppression_region: Option<String>,
    pub suppression_country: Option<String>,
    pub suppression_state: Option<String>,
}

impl AirportMctRecord {
    pub fn validate(&self) -> Result<()> {
        validate_status(&self.status)?;
        validate_opt_hhmm(self.time.as_deref(), "time")?;
        validate_opt_iata_location(self.arrival_station.as_deref(), "arrival_station")?;
        validate_opt_iata_location(self.departure_station.as_deref(), "departure_station")?;
        validate_opt_airline(self.arrival_carrier.as_deref(), "arrival_carrier")?;
        validate_opt_airline(
            self.arrival_codeshare_operating_carrier.as_deref(),
            "arrival_codeshare_operating_carrier",
        )?;
        validate_opt_airline(self.departure_carrier.as_deref(), "departure_carrier")?;
        validate_opt_airline(
            self.departure_codeshare_operating_carrier.as_deref(),
            "departure_codeshare_operating_carrier",
        )?;
        validate_opt_alphanumeric(
            self.arrival_aircraft_type.as_deref(),
            3,
            3,
            "arrival_aircraft_type",
        )?;
        validate_opt_body(
            self.arrival_aircraft_body.as_deref(),
            "arrival_aircraft_body",
        )?;
        validate_opt_alphanumeric(
            self.departure_aircraft_type.as_deref(),
            3,
            3,
            "departure_aircraft_type",
        )?;
        validate_opt_body(
            self.departure_aircraft_body.as_deref(),
            "departure_aircraft_body",
        )?;
        validate_opt_alphanumeric(self.arrival_terminal.as_deref(), 1, 2, "arrival_terminal")?;
        validate_opt_alphanumeric(
            self.departure_terminal.as_deref(),
            1,
            2,
            "departure_terminal",
        )?;
        validate_opt_alphanumeric(self.previous_country.as_deref(), 2, 2, "previous_country")?;
        validate_opt_iata_location(self.previous_station.as_deref(), "previous_station")?;
        validate_opt_alphanumeric(self.next_country.as_deref(), 2, 2, "next_country")?;
        validate_opt_iata_location(self.next_station.as_deref(), "next_station")?;
        validate_opt_numeric(
            self.arrival_flight_number_range_start.as_deref(),
            4,
            4,
            "arrival_flight_number_range_start",
        )?;
        validate_opt_numeric(
            self.arrival_flight_number_range_end.as_deref(),
            4,
            4,
            "arrival_flight_number_range_end",
        )?;
        validate_opt_numeric(
            self.departure_flight_number_range_start.as_deref(),
            4,
            4,
            "departure_flight_number_range_start",
        )?;
        validate_opt_numeric(
            self.departure_flight_number_range_end.as_deref(),
            4,
            4,
            "departure_flight_number_range_end",
        )?;
        validate_opt_alphanumeric(self.previous_state.as_deref(), 2, 2, "previous_state")?;
        validate_opt_alphanumeric(self.next_state.as_deref(), 2, 2, "next_state")?;
        validate_opt_alphanumeric(self.previous_region.as_deref(), 3, 3, "previous_region")?;
        validate_opt_alphanumeric(self.next_region.as_deref(), 3, 3, "next_region")?;
        validate_opt_date(self.effective_from_local.as_deref(), "effective_from_local")?;
        validate_opt_date(self.effective_to_local.as_deref(), "effective_to_local")?;
        validate_opt_alphanumeric(
            self.suppression_region.as_deref(),
            3,
            3,
            "suppression_region",
        )?;
        validate_opt_alphanumeric(
            self.suppression_country.as_deref(),
            2,
            2,
            "suppression_country",
        )?;
        validate_opt_alphanumeric(self.suppression_state.as_deref(), 2, 2, "suppression_state")?;

        if self.arrival_flight_number_range_start.is_some()
            != self.arrival_flight_number_range_end.is_some()
        {
            return Err(anyhow!(
                "arrival flight number range start and end must both be present or both be blank"
            ));
        }
        if self.departure_flight_number_range_start.is_some()
            != self.departure_flight_number_range_end.is_some()
        {
            return Err(anyhow!(
                "departure flight number range start and end must both be present or both be blank"
            ));
        }
        if self.previous_state.is_some() && self.previous_country.is_none() {
            return Err(anyhow!(
                "previous_country must be present when previous_state is used"
            ));
        }
        if self.next_state.is_some() && self.next_country.is_none() {
            return Err(anyhow!(
                "next_country must be present when next_state is used"
            ));
        }
        if self.arrival_aircraft_type.is_some() && self.arrival_aircraft_body.is_some() {
            return Err(anyhow!(
                "arrival_aircraft_type and arrival_aircraft_body cannot both be set"
            ));
        }
        if self.departure_aircraft_type.is_some() && self.departure_aircraft_body.is_some() {
            return Err(anyhow!(
                "departure_aircraft_type and departure_aircraft_body cannot both be set"
            ));
        }
        if self.arrival_codeshare_operating_carrier.is_some() && !self.arrival_codeshare_indicator {
            return Err(anyhow!(
                "arrival_codeshare_indicator must be true when arrival_codeshare_operating_carrier is set"
            ));
        }
        if self.departure_codeshare_operating_carrier.is_some()
            && !self.departure_codeshare_indicator
        {
            return Err(anyhow!(
                "departure_codeshare_indicator must be true when departure_codeshare_operating_carrier is set"
            ));
        }
        if !self.suppression_indicator && self.time.is_none() {
            return Err(anyhow!(
                "time must be present for a non-suppression MCT record"
            ));
        }

        Ok(())
    }

    pub fn same_scope_as(&self, other: &Self) -> bool {
        self.arrival_station == other.arrival_station
            && self.status == other.status
            && self.departure_station == other.departure_station
            && self.requires_connection_building_filter == other.requires_connection_building_filter
            && self.arrival_carrier == other.arrival_carrier
            && self.arrival_codeshare_indicator == other.arrival_codeshare_indicator
            && self.arrival_codeshare_operating_carrier == other.arrival_codeshare_operating_carrier
            && self.departure_carrier == other.departure_carrier
            && self.departure_codeshare_indicator == other.departure_codeshare_indicator
            && self.departure_codeshare_operating_carrier
                == other.departure_codeshare_operating_carrier
            && self.arrival_aircraft_type == other.arrival_aircraft_type
            && self.arrival_aircraft_body == other.arrival_aircraft_body
            && self.departure_aircraft_type == other.departure_aircraft_type
            && self.departure_aircraft_body == other.departure_aircraft_body
            && self.arrival_terminal == other.arrival_terminal
            && self.departure_terminal == other.departure_terminal
            && self.previous_country == other.previous_country
            && self.previous_station == other.previous_station
            && self.next_country == other.next_country
            && self.next_station == other.next_station
            && self.arrival_flight_number_range_start == other.arrival_flight_number_range_start
            && self.arrival_flight_number_range_end == other.arrival_flight_number_range_end
            && self.departure_flight_number_range_start == other.departure_flight_number_range_start
            && self.departure_flight_number_range_end == other.departure_flight_number_range_end
            && self.previous_state == other.previous_state
            && self.next_state == other.next_state
            && self.previous_region == other.previous_region
            && self.next_region == other.next_region
            && self.effective_from_local == other.effective_from_local
            && self.effective_to_local == other.effective_to_local
            && self.suppression_indicator == other.suppression_indicator
            && self.suppression_region == other.suppression_region
            && self.suppression_country == other.suppression_country
            && self.suppression_state == other.suppression_state
    }
}

pub fn is_global_mct_record(record: &AirportMctRecord) -> bool {
    record.arrival_station.is_none() && record.departure_station.is_none()
}

pub fn ensure_airport_default_mct_records(
    records: Vec<AirportMctRecord>,
    default_minutes: Option<u32>,
) -> Vec<AirportMctRecord> {
    if !records.is_empty() {
        return records;
    }

    airport_default_mct_records(default_minutes.unwrap_or(DEFAULT_AIRPORT_MCT_MINUTES))
}

pub fn airport_default_mct_records(minutes: u32) -> Vec<AirportMctRecord> {
    let time = format_minutes_as_hhmm(minutes).unwrap_or_else(|| "0300".to_string());

    DEFAULT_AIRPORT_MCT_STATUSES
        .into_iter()
        .map(|status| AirportMctRecord {
            arrival_station: None,
            time: Some(time.clone()),
            status: status.to_string(),
            departure_station: None,
            requires_connection_building_filter: false,
            arrival_carrier: None,
            arrival_codeshare_indicator: false,
            arrival_codeshare_operating_carrier: None,
            departure_carrier: None,
            departure_codeshare_indicator: false,
            departure_codeshare_operating_carrier: None,
            arrival_aircraft_type: None,
            arrival_aircraft_body: None,
            departure_aircraft_type: None,
            departure_aircraft_body: None,
            arrival_terminal: None,
            departure_terminal: None,
            previous_country: None,
            previous_station: None,
            next_country: None,
            next_station: None,
            arrival_flight_number_range_start: None,
            arrival_flight_number_range_end: None,
            departure_flight_number_range_start: None,
            departure_flight_number_range_end: None,
            previous_state: None,
            next_state: None,
            previous_region: None,
            next_region: None,
            effective_from_local: None,
            effective_to_local: None,
            suppression_indicator: false,
            suppression_region: None,
            suppression_country: None,
            suppression_state: None,
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedMctRecord {
    #[serde(flatten)]
    pub data: AirportMctRecord,
    pub submitting_carrier_identifier: Option<String>,
    pub filing_date_local: Option<String>,
    pub action_indicator: Option<MctActionIndicator>,
    pub record_serial_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedConnectionBuildingFilterRecord {
    #[serde(flatten)]
    pub data: ConnectionBuildingFilter,
    pub record_serial_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedMctFile {
    pub header: MctHeaderRecord,
    pub records: Vec<ParsedMctRecord>,
    pub connection_building_filters: Vec<ParsedConnectionBuildingFilterRecord>,
    pub trailer: MctTrailerRecord,
}

fn validate_status(value: &str) -> Result<()> {
    if value.len() != 2 || !value.chars().all(|ch| matches!(ch, 'D' | 'I')) {
        return Err(anyhow!(
            "status must be a 2-character combination of I and D"
        ));
    }
    Ok(())
}

fn validate_opt_hhmm(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.len() != 4 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(anyhow!("{field} must be a 4-digit HHMM value"));
    }
    NaiveTime::parse_from_str(value, "%H%M")
        .map(|_| ())
        .map_err(|_| anyhow!("{field} must be a valid HHMM value"))
}

fn validate_opt_date(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    parse_ssim_date(value)
        .map(|_| ())
        .map_err(|_| anyhow!("{field} must use DDMMMYY format"))
}

fn validate_opt_iata_location(value: Option<&str>, field: &str) -> Result<()> {
    validate_opt_alphanumeric(value, 3, 3, field)
}

fn validate_opt_airline(value: Option<&str>, field: &str) -> Result<()> {
    validate_opt_alphanumeric(value, 2, 2, field)
}

fn validate_opt_body(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    match value {
        "N" | "W" => Ok(()),
        _ => Err(anyhow!("{field} must be N or W")),
    }
}

fn validate_opt_numeric(
    value: Option<&str>,
    min_len: usize,
    max_len: usize,
    field: &str,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.len() < min_len
        || value.len() > max_len
        || !value.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(anyhow!(
            "{field} must be between {min_len} and {max_len} numeric characters"
        ));
    }
    Ok(())
}

fn validate_opt_alphanumeric(
    value: Option<&str>,
    min_len: usize,
    max_len: usize,
    field: &str,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.len() < min_len
        || value.len() > max_len
        || !value.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        return Err(anyhow!(
            "{field} must be between {min_len} and {max_len} ASCII alphanumeric characters"
        ));
    }
    Ok(())
}

fn parse_ssim_date(value: &str) -> Result<NaiveDate> {
    if value.len() != 7 {
        return Err(anyhow!("SSIM dates must be 7 characters long"));
    }

    let month = value[2..5].to_ascii_lowercase();
    let mut normalized = String::with_capacity(7);
    normalized.push_str(&value[0..2]);
    normalized.push_str(&month[..1].to_ascii_uppercase());
    normalized.push_str(&month[1..]);
    normalized.push_str(&value[5..7]);

    NaiveDate::parse_from_str(&normalized, "%d%b%y")
        .map_err(|_| anyhow!("invalid SSIM date: {value}"))
}

fn format_minutes_as_hhmm(minutes: u32) -> Option<String> {
    if minutes > 23 * 60 + 59 {
        return None;
    }

    let hours = minutes / 60;
    let remainder = minutes % 60;
    Some(format!("{hours:02}{remainder:02}"))
}
