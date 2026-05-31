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
        Self::from_plan_for_table(flight_plan, date, "flight")
    }

    pub fn from_plan_for_table(flight_plan: &FlightPlan, date: NaiveDate, table: &str) -> Self {
        let primary_designator = flight_plan.operating_designator.clone();
        let marketing_designator = FlightDesignatorRow {
            company: flight_plan.company.clone(),
            flight_number: flight_plan.flight_no.clone(),
            operational_suffix: None,
        };
        let mut duplicate_designators = flight_plan.duplicate_designators.clone();
        if marketing_designator != primary_designator {
            push_unique_designator(&mut duplicate_designators, marketing_designator.clone());
        }
        normalize_duplicate_designators(&primary_designator, &mut duplicate_designators);

        let type3_legs = flight_plan
            .type3_legs
            .iter()
            .cloned()
            .map(|mut leg| {
                if marketing_designator != leg.operating_designator {
                    push_unique_designator(
                        &mut leg.duplicate_designators,
                        marketing_designator.clone(),
                    );
                }
                normalize_duplicate_designators(
                    &leg.operating_designator,
                    &mut leg.duplicate_designators,
                );
                leg
            })
            .collect::<Vec<_>>();
        let id_str = format!(
            "{}_{}_{}_{}_{}",
            primary_designator.company,
            primary_designator.flight_number,
            flight_plan.origin.as_str(),
            flight_plan.destination.as_str(),
            date.format("%Y-%m-%d")
        );
        let dep_offset = FixedOffset::from_str(flight_plan.dep_tz.as_str()).unwrap();
        let arr_offset = FixedOffset::from_str(flight_plan.arr_tz.as_str()).unwrap();
        FlightRow {
            id: RecordId::new(table, id_str.as_str()),
            company: primary_designator.company.clone(),
            flight_num: primary_designator.flight_number.clone(),
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
            operating_designator: primary_designator,
            duplicate_designators,
            joint_operation_airline_designators: flight_plan
                .joint_operation_airline_designators
                .clone(),
            meal_service_note: flight_plan.meal_service_note.clone(),
            in_flight_service_info: flight_plan.in_flight_service_info.clone(),
            electronic_ticketing_info: flight_plan.electronic_ticketing_info.clone(),
            type3_legs,
        }
    }

    pub fn merge_in_place(&mut self, incoming: FlightRow) {
        self.departure_terminal =
            prefer_richer_option(self.departure_terminal.take(), incoming.departure_terminal);
        self.arrival_terminal =
            prefer_richer_option(self.arrival_terminal.take(), incoming.arrival_terminal);
        merge_designator_lists(
            &self.operating_designator,
            &mut self.duplicate_designators,
            incoming.duplicate_designators,
        );
        merge_string_lists(
            &mut self.joint_operation_airline_designators,
            incoming.joint_operation_airline_designators,
        );
        self.meal_service_note =
            prefer_richer_option(self.meal_service_note.take(), incoming.meal_service_note);
        self.in_flight_service_info = prefer_richer_option(
            self.in_flight_service_info.take(),
            incoming.in_flight_service_info,
        );
        self.electronic_ticketing_info = prefer_richer_option(
            self.electronic_ticketing_info.take(),
            incoming.electronic_ticketing_info,
        );
        merge_type3_legs(&mut self.type3_legs, incoming.type3_legs);
    }
}

impl FlightType3LegRow {
    fn merge_in_place(&mut self, incoming: FlightType3LegRow) {
        self.departure_terminal =
            prefer_richer_option(self.departure_terminal.take(), incoming.departure_terminal);
        self.arrival_terminal =
            prefer_richer_option(self.arrival_terminal.take(), incoming.arrival_terminal);
        self.prbd = prefer_richer_option(self.prbd.take(), incoming.prbd);
        self.prbm = prefer_richer_option(self.prbm.take(), incoming.prbm);
        self.meal_service_note =
            prefer_richer_option(self.meal_service_note.take(), incoming.meal_service_note);
        merge_string_lists(
            &mut self.joint_operation_airline_designators,
            incoming.joint_operation_airline_designators,
        );
        self.secure_flight_indicator = prefer_richer_option(
            self.secure_flight_indicator.take(),
            incoming.secure_flight_indicator,
        );
        self.itinerary_variation_overflow = prefer_richer_option(
            self.itinerary_variation_overflow.take(),
            incoming.itinerary_variation_overflow,
        );
        self.aircraft_owner =
            prefer_richer_option(self.aircraft_owner.take(), incoming.aircraft_owner);
        self.cockpit_crew_employer = prefer_richer_option(
            self.cockpit_crew_employer.take(),
            incoming.cockpit_crew_employer,
        );
        self.cabin_crew_employer = prefer_richer_option(
            self.cabin_crew_employer.take(),
            incoming.cabin_crew_employer,
        );
        self.onward_airline_designator = prefer_richer_option(
            self.onward_airline_designator.take(),
            incoming.onward_airline_designator,
        );
        self.onward_flight_number = prefer_richer_option(
            self.onward_flight_number.take(),
            incoming.onward_flight_number,
        );
        self.onward_aircraft_rotation_layover = prefer_richer_option(
            self.onward_aircraft_rotation_layover.take(),
            incoming.onward_aircraft_rotation_layover,
        );
        self.onward_operational_suffix = prefer_richer_option(
            self.onward_operational_suffix.take(),
            incoming.onward_operational_suffix,
        );
        self.operating_airline_disclosure = prefer_richer_option(
            self.operating_airline_disclosure.take(),
            incoming.operating_airline_disclosure,
        );
        self.traffic_restriction_code = prefer_richer_option(
            self.traffic_restriction_code.take(),
            incoming.traffic_restriction_code,
        );
        self.traffic_restriction_code_leg_overflow_indicator = prefer_richer_option(
            self.traffic_restriction_code_leg_overflow_indicator.take(),
            incoming.traffic_restriction_code_leg_overflow_indicator,
        );
        merge_designator_lists(
            &self.operating_designator,
            &mut self.duplicate_designators,
            incoming.duplicate_designators,
        );
        self.in_flight_service_info = prefer_richer_option(
            self.in_flight_service_info.take(),
            incoming.in_flight_service_info,
        );
        self.electronic_ticketing_info = prefer_richer_option(
            self.electronic_ticketing_info.take(),
            incoming.electronic_ticketing_info,
        );
    }
}

fn merge_type3_legs(existing: &mut Vec<FlightType3LegRow>, incoming: Vec<FlightType3LegRow>) {
    for incoming_leg in incoming {
        if let Some(existing_leg) = existing.iter_mut().find(|leg| {
            leg.leg_sequence == incoming_leg.leg_sequence
                && leg.departure_station == incoming_leg.departure_station
                && leg.arrival_station == incoming_leg.arrival_station
        }) {
            existing_leg.merge_in_place(incoming_leg);
        } else {
            existing.push(incoming_leg);
        }
    }

    existing.sort_by_key(|leg| leg.leg_sequence);
}

fn merge_designator_lists(
    primary: &FlightDesignatorRow,
    existing: &mut Vec<FlightDesignatorRow>,
    incoming: Vec<FlightDesignatorRow>,
) {
    for designator in incoming {
        push_unique_designator(existing, designator);
    }
    normalize_duplicate_designators(primary, existing);
}

fn merge_string_lists(existing: &mut Vec<String>, incoming: Vec<String>) {
    for value in incoming {
        if !existing.contains(&value) {
            existing.push(value);
        }
    }
}

fn push_unique_designator(target: &mut Vec<FlightDesignatorRow>, candidate: FlightDesignatorRow) {
    if !target.contains(&candidate) {
        target.push(candidate);
    }
}

fn normalize_duplicate_designators(
    primary: &FlightDesignatorRow,
    duplicates: &mut Vec<FlightDesignatorRow>,
) {
    let mut normalized = Vec::new();
    for designator in duplicates.drain(..) {
        if designator != *primary && !normalized.contains(&designator) {
            normalized.push(designator);
        }
    }
    *duplicates = normalized;
}

fn prefer_richer_option(current: Option<String>, incoming: Option<String>) -> Option<String> {
    match (current, incoming) {
        (Some(current), Some(incoming)) if incoming.len() > current.len() => Some(incoming),
        (Some(current), Some(_)) => Some(current),
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
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
        assert_eq!(row.company, "UA");
        assert_eq!(row.flight_num, "551");
        assert_eq!(row.in_flight_service_info.as_deref(), Some("9"));
        assert_eq!(row.electronic_ticketing_info.as_deref(), Some("ET"));
    }

    #[test]
    fn marketing_designator_is_preserved_as_duplicate_when_operating_key_differs() {
        let plan = FlightPlan {
            company: "CA".to_string(),
            flight_no: "7312".to_string(),
            origin: AirportCode::new("SFO").unwrap(),
            destination: AirportCode::new("IAD").unwrap(),
            dep_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
            block_time: Duration::hours(5),
            start_date: NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 4, 2).unwrap(),
            weekdays: [false, false, false, true, false, false, false],
            frequency_rate: None,
            dep_tz: "-0700".to_string(),
            arr_tz: "-0400".to_string(),
            arrival_day_offset: 0,
            operating_designator: FlightDesignatorRow {
                company: "UA".to_string(),
                flight_number: "551".to_string(),
                operational_suffix: None,
            },
            duplicate_designators: vec![],
            joint_operation_airline_designators: vec![],
            meal_service_note: None,
            in_flight_service_info: None,
            electronic_ticketing_info: None,
            type3_legs: vec![FlightType3LegRow {
                leg_sequence: 1,
                departure_station: "SFO".to_string(),
                arrival_station: "IAD".to_string(),
                departure_terminal: None,
                arrival_terminal: None,
                prbd: None,
                prbm: None,
                meal_service_note: None,
                joint_operation_airline_designators: vec![],
                secure_flight_indicator: None,
                itinerary_variation_overflow: None,
                aircraft_owner: None,
                cockpit_crew_employer: None,
                cabin_crew_employer: None,
                onward_airline_designator: None,
                onward_flight_number: None,
                onward_aircraft_rotation_layover: None,
                onward_operational_suffix: None,
                operating_airline_disclosure: None,
                traffic_restriction_code: None,
                traffic_restriction_code_leg_overflow_indicator: None,
                operating_designator: FlightDesignatorRow {
                    company: "UA".to_string(),
                    flight_number: "551".to_string(),
                    operational_suffix: None,
                },
                duplicate_designators: vec![],
                in_flight_service_info: None,
                electronic_ticketing_info: None,
            }],
        };

        let row = FlightRow::from_plan(&plan, plan.start_date);

        assert_eq!(row.company, "UA");
        assert_eq!(row.flight_num, "551");
        assert_eq!(
            row.duplicate_designators,
            vec![FlightDesignatorRow {
                company: "CA".to_string(),
                flight_number: "7312".to_string(),
                operational_suffix: None,
            }]
        );
        assert_eq!(
            row.type3_legs[0].duplicate_designators,
            vec![FlightDesignatorRow {
                company: "CA".to_string(),
                flight_number: "7312".to_string(),
                operational_suffix: None,
            }]
        );
    }
}
