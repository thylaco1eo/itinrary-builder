use crate::domain::airport::AirportCode;
use crate::Infrastructure::db::model::flight_row::{
    FlightDesignatorRow, FlightRow, FlightType3LegRow,
};
use crate::Infrastructure::file_loader::dei::Dei;
use crate::Infrastructure::file_loader::oag_parser::{FlightLegRecord, SegmentDataRecord};
use crate::Infrastructure::file_loader::ssim_loader::FlightLegBlock;
use anyhow::{anyhow, Result};
use chrono::Datelike;
use chrono::{Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone};
use std::str::FromStr;

pub struct FlightPlan {
    pub company: String,
    pub flight_no: String,
    pub origin: AirportCode,
    pub destination: AirportCode,
    pub dep_time: NaiveTime,
    pub arr_time: NaiveTime,
    pub block_time: Duration,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub weekdays: [bool; 7],
    pub frequency_rate: Option<u8>,
    pub dep_tz: String,
    pub arr_tz: String,
    pub arrival_day_offset: i64,
    pub operating_designator: FlightDesignatorRow,
    pub duplicate_designators: Vec<FlightDesignatorRow>,
    pub joint_operation_airline_designators: Vec<String>,
    pub meal_service_note: Option<String>,
    pub in_flight_service_info: Option<String>,
    pub electronic_ticketing_info: Option<String>,
    pub type3_legs: Vec<FlightType3LegRow>,
}

pub fn parse_line(line: &str) -> Option<FlightPlan> {
    if !line.starts_with('3') {
        return None;
    }

    let dep = NaiveTime::parse_from_str(&line[43..47], "%H%M").ok()?;
    let arr = NaiveTime::parse_from_str(&line[61..65], "%H%M").ok()?;
    let dep_date_var = line[192..193].chars().next()?;
    let arr_date_var = line[193..194].chars().next()?;
    let arrival_day_offset = relative_arrival_day_offset(dep_date_var, arr_date_var).ok()?;

    let block_time = compute_block_time_with_offsets(
        dep,
        line[47..52].trim(),
        arr,
        line[65..70].trim(),
        arrival_day_offset,
    )
    .ok()?;

    Some(FlightPlan {
        company: line[2..5].trim().to_string(),
        flight_no: line[5..9].trim().to_string(),
        origin: AirportCode::new(line[36..39].trim()).ok()?,
        destination: AirportCode::new(line[54..57].trim()).ok()?,
        dep_time: dep,
        arr_time: arr,
        block_time,
        start_date: NaiveDate::parse_from_str(&line[14..21], "%d%b%y").ok()?,
        end_date: NaiveDate::parse_from_str(&line[21..28], "%d%b%y").ok()?,
        weekdays: build_weekdays(&line[28..35]),
        frequency_rate: parse_frequency_rate(&line[35..36]).ok()?,
        dep_tz: line[47..52].trim().to_string(),
        arr_tz: line[65..70].trim().to_string(),
        arrival_day_offset,
        operating_designator: FlightDesignatorRow {
            company: line[2..5].trim().to_string(),
            flight_number: line[5..9].trim().to_string(),
            operational_suffix: None,
        },
        duplicate_designators: vec![],
        joint_operation_airline_designators: vec![],
        meal_service_note: None,
        in_flight_service_info: None,
        electronic_ticketing_info: None,
        type3_legs: vec![],
    })
}

impl TryFrom<FlightLegRecord> for FlightPlan {
    type Error = anyhow::Error;

    fn try_from(record: FlightLegRecord) -> Result<Self, Self::Error> {
        plan_from_leg_records(std::slice::from_ref(&record))
    }
}

pub fn plans_from_leg_blocks(blocks: &[FlightLegBlock]) -> Result<Vec<FlightPlan>> {
    let mut plans = Vec::new();

    for start in 0..blocks.len() {
        for end in start..blocks.len() {
            plans.push(plan_from_leg_blocks(&blocks[start..=end], blocks)?);
        }
    }

    Ok(plans)
}

pub fn plans_from_leg_records(records: &[FlightLegRecord]) -> Result<Vec<FlightPlan>> {
    let blocks = records
        .iter()
        .cloned()
        .map(|leg| FlightLegBlock {
            leg,
            segments: Vec::new(),
        })
        .collect::<Vec<_>>();
    plans_from_leg_blocks(&blocks)
}

pub fn expand(plan: &FlightPlan) -> Vec<FlightRow> {
    expand_for_table(plan, "flight")
}

pub fn expand_for_table(plan: &FlightPlan, table: &str) -> Vec<FlightRow> {
    let mut rows = Vec::new();
    let mut date = plan.start_date;

    while date <= plan.end_date {
        if operates_on_date(plan, date) {
            rows.push(FlightRow::from_plan_for_table(plan, date, table));
        }
        date = date.succ_opt().unwrap();
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::airport::AirportCode;
    use chrono::{Duration, NaiveDate, NaiveTime};

    #[test]
    fn test_expand_full_date_range_without_import_cap() {
        let plan = FlightPlan {
            company: "AA".to_string(),
            flight_no: "123".to_string(),
            origin: AirportCode::new("JFK").unwrap(),
            destination: AirportCode::new("LAX").unwrap(),
            dep_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            block_time: Duration::hours(3),
            start_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 4, 15).unwrap(),
            weekdays: [true; 7],
            frequency_rate: None,
            dep_tz: "+0000".to_string(),
            arr_tz: "+0000".to_string(),
            arrival_day_offset: 0,
            operating_designator: FlightDesignatorRow {
                company: "AA".to_string(),
                flight_number: "123".to_string(),
                operational_suffix: None,
            },
            duplicate_designators: vec![],
            joint_operation_airline_designators: vec![],
            meal_service_note: None,
            in_flight_service_info: None,
            electronic_ticketing_info: None,
            type3_legs: vec![],
        };

        let rows = expand(&plan);
        assert_eq!(
            rows.len(),
            105,
            "Expansion should cover the full schedule date range"
        );
        assert_eq!(
            rows.first().unwrap().dep_local.date_naive(),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
        );
        assert_eq!(
            rows.last().unwrap().dep_local.date_naive(),
            NaiveDate::from_ymd_opt(2026, 4, 15).unwrap()
        );
    }

    #[test]
    fn test_expand_less_than_30_days() {
        let plan = FlightPlan {
            company: "AA".to_string(),
            flight_no: "123".to_string(),
            origin: AirportCode::new("JFK").unwrap(),
            destination: AirportCode::new("LAX").unwrap(),
            dep_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            block_time: Duration::hours(3),
            start_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 1, 10).unwrap(), // 10 days
            weekdays: [true; 7],
            frequency_rate: None,
            dep_tz: "+0000".to_string(),
            arr_tz: "+0000".to_string(),
            arrival_day_offset: 0,
            operating_designator: FlightDesignatorRow {
                company: "AA".to_string(),
                flight_number: "123".to_string(),
                operational_suffix: None,
            },
            duplicate_designators: vec![],
            joint_operation_airline_designators: vec![],
            meal_service_note: None,
            in_flight_service_info: None,
            electronic_ticketing_info: None,
            type3_legs: vec![],
        };

        let rows = expand(&plan);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn builds_single_leg_and_through_plans_for_multi_segment_flight() {
        let records = vec![
            FlightLegRecord {
                airline_designator: "CA".to_string(),
                flight_number: "865".to_string(),
                itinerary_variation: 1,
                leg_sequence: 1,
                service_type: "J".to_string(),
                valid_from: NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(),
                valid_to: NaiveDate::from_ymd_opt(2026, 3, 4).unwrap(),
                days_of_operation: "3".to_string(),
                frequency_rate: None,
                departure_station: "PEK".to_string(),
                pax_std: NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
                aircraft_std: NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
                time_var_dep: "+0800".to_string(),
                departure_terminal: None,
                arrival_station: "MAD".to_string(),
                aircraft_sta: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                pax_sta: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                time_var_arr: "+0100".to_string(),
                arrival_terminal: None,
                aircraft_type: "789".to_string(),
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
                aircraft_config: None,
                dep_date_var: '0',
                arr_date_var: '0',
                record_serial_number: 1,
            },
            FlightLegRecord {
                airline_designator: "CA".to_string(),
                flight_number: "865".to_string(),
                itinerary_variation: 1,
                leg_sequence: 2,
                service_type: "J".to_string(),
                valid_from: NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(),
                valid_to: NaiveDate::from_ymd_opt(2026, 3, 4).unwrap(),
                days_of_operation: "3".to_string(),
                frequency_rate: None,
                departure_station: "MAD".to_string(),
                pax_std: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                aircraft_std: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                time_var_dep: "+0100".to_string(),
                departure_terminal: None,
                arrival_station: "HAV".to_string(),
                aircraft_sta: NaiveTime::from_hms_opt(18, 15, 0).unwrap(),
                pax_sta: NaiveTime::from_hms_opt(18, 15, 0).unwrap(),
                time_var_arr: "-0500".to_string(),
                arrival_terminal: None,
                aircraft_type: "789".to_string(),
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
                aircraft_config: None,
                dep_date_var: '0',
                arr_date_var: '0',
                record_serial_number: 2,
            },
        ];

        let plans = plans_from_leg_records(&records).unwrap();
        assert_eq!(plans.len(), 3);

        assert_eq!(plans[0].origin.as_str(), "PEK");
        assert_eq!(plans[0].destination.as_str(), "MAD");

        assert_eq!(plans[1].origin.as_str(), "PEK");
        assert_eq!(plans[1].destination.as_str(), "HAV");
        assert_eq!(plans[1].block_time.num_minutes(), 1455);

        assert_eq!(plans[2].origin.as_str(), "MAD");
        assert_eq!(plans[2].destination.as_str(), "HAV");
    }

    #[test]
    fn expand_honors_fortnightly_frequency_rate() {
        let plan = FlightPlan {
            company: "AA".to_string(),
            flight_no: "2468".to_string(),
            origin: AirportCode::new("PEK").unwrap(),
            destination: AirportCode::new("HKG").unwrap(),
            dep_time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            block_time: Duration::hours(4),
            start_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), // Monday
            end_date: NaiveDate::from_ymd_opt(2026, 1, 31).unwrap(),
            weekdays: build_weekdays("1 3    "),
            frequency_rate: Some(2),
            dep_tz: "+0800".to_string(),
            arr_tz: "+0800".to_string(),
            arrival_day_offset: 0,
            operating_designator: FlightDesignatorRow {
                company: "AA".to_string(),
                flight_number: "2468".to_string(),
                operational_suffix: None,
            },
            duplicate_designators: vec![],
            joint_operation_airline_designators: vec![],
            meal_service_note: None,
            in_flight_service_info: None,
            electronic_ticketing_info: None,
            type3_legs: vec![],
        };

        let rows = expand(&plan);
        let operating_dates = rows
            .iter()
            .map(|row| row.dep_local.date_naive())
            .collect::<Vec<_>>();

        assert_eq!(
            operating_dates,
            vec![
                NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
                NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
                NaiveDate::from_ymd_opt(2026, 1, 19).unwrap(),
                NaiveDate::from_ymd_opt(2026, 1, 21).unwrap(),
            ]
        );
    }
}

fn plan_from_leg_records(records: &[FlightLegRecord]) -> Result<FlightPlan> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot build a flight plan from an empty leg list"))?;
    let last = records.last().unwrap();

    for pair in records.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];

        if previous.airline_designator != next.airline_designator
            || previous.flight_number != next.flight_number
            || previous.itinerary_variation != next.itinerary_variation
            || previous.service_type != next.service_type
            || previous.frequency_rate != next.frequency_rate
        {
            return Err(anyhow!(
                "legs do not belong to the same OAG itinerary: {}{} IVI {} leg {} -> {}{} IVI {} leg {}",
                previous.airline_designator,
                previous.flight_number,
                previous.itinerary_variation,
                previous.leg_sequence,
                next.airline_designator,
                next.flight_number,
                next.itinerary_variation,
                next.leg_sequence
            ));
        }

        if next.leg_sequence != previous.leg_sequence.saturating_add(1) {
            return Err(anyhow!(
                "non-contiguous leg sequence for {}{} IVI {}: expected {}, got {}",
                previous.airline_designator,
                previous.flight_number,
                previous.itinerary_variation,
                previous.leg_sequence.saturating_add(1),
                next.leg_sequence
            ));
        }

        if previous.arrival_station != next.departure_station {
            return Err(anyhow!(
                "leg station mismatch for {}{} IVI {}: {} does not connect to {}",
                previous.airline_designator,
                previous.flight_number,
                previous.itinerary_variation,
                previous.arrival_station,
                next.departure_station
            ));
        }
    }

    let dep_tz = first.time_var_dep.clone();
    let arr_tz = last.time_var_arr.clone();
    let arrival_day_offset = relative_arrival_day_offset(first.dep_date_var, last.arr_date_var)?;
    let dep_time = first.aircraft_std;
    let arr_time = last.pax_sta;

    Ok(FlightPlan {
        company: first.airline_designator.clone(),
        flight_no: first.flight_number.clone(),
        origin: AirportCode::new(first.departure_station.clone())
            .map_err(|_| anyhow!("invalid origin airport code {}", first.departure_station))?,
        destination: AirportCode::new(last.arrival_station.clone())
            .map_err(|_| anyhow!("invalid destination airport code {}", last.arrival_station))?,
        dep_time,
        arr_time,
        block_time: compute_block_time_with_offsets(
            dep_time,
            dep_tz.as_str(),
            arr_time,
            arr_tz.as_str(),
            arrival_day_offset,
        )?,
        start_date: first.valid_from,
        end_date: first.valid_to,
        weekdays: build_weekdays(&first.days_of_operation),
        frequency_rate: first.frequency_rate,
        dep_tz,
        arr_tz,
        arrival_day_offset,
        operating_designator: self_designator(first),
        duplicate_designators: vec![],
        joint_operation_airline_designators: common_joint_operation_airlines(records),
        meal_service_note: common_meal_service_note(records),
        in_flight_service_info: None,
        electronic_ticketing_info: None,
        type3_legs: records.iter().map(type3_leg_row_from_record).collect(),
    })
}

fn plan_from_leg_blocks(
    selected: &[FlightLegBlock],
    all_blocks: &[FlightLegBlock],
) -> Result<FlightPlan> {
    let records = selected
        .iter()
        .map(|block| block.leg.clone())
        .collect::<Vec<_>>();
    let mut plan = plan_from_leg_records(&records)?;

    let span_segments = matching_segment_records(
        all_blocks,
        selected.first().unwrap().leg.leg_sequence,
        selected.last().unwrap().leg.leg_sequence,
    );
    let merged_legs = selected.iter().map(merged_leg_row).collect::<Vec<_>>();

    plan.operating_designator =
        operating_designator_for_span(&plan.operating_designator, &merged_legs, &span_segments);
    plan.duplicate_designators = duplicate_designators_for_span(&span_segments);
    plan.joint_operation_airline_designators =
        joint_operation_airlines_for_span(selected, &span_segments);
    plan.meal_service_note = meal_service_note_for_span(selected, &span_segments);
    plan.in_flight_service_info = in_flight_service_info_for_span(selected, &span_segments);
    plan.electronic_ticketing_info = electronic_ticketing_info_for_span(selected, &span_segments);
    plan.type3_legs = merged_legs;

    Ok(plan)
}

fn build_weekdays(days_of_operation: &str) -> [bool; 7] {
    let mut weekdays = [false; 7];
    for i in 0..7 {
        weekdays[i] = days_of_operation.contains(&(i + 1).to_string());
    }
    weekdays
}

fn parse_frequency_rate(value: &str) -> Result<Option<u8>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let frequency_rate = trimmed
        .parse::<u8>()
        .map_err(|_| anyhow!("invalid frequency rate {trimmed}"))?;
    if frequency_rate == 0 {
        return Err(anyhow!("frequency rate must be greater than zero"));
    }

    Ok(Some(frequency_rate))
}

fn self_designator(record: &FlightLegRecord) -> FlightDesignatorRow {
    FlightDesignatorRow {
        company: record.airline_designator.clone(),
        flight_number: record.flight_number.clone(),
        operational_suffix: None,
    }
}

fn merged_leg_row(block: &FlightLegBlock) -> FlightType3LegRow {
    let mut row = type3_leg_row_from_record(&block.leg);
    let matching_segments = matching_segments_for_span(
        &block.segments,
        block.leg.leg_sequence,
        block.leg.leg_sequence,
        block.leg.departure_station.as_str(),
        block.leg.arrival_station.as_str(),
    );

    row.operating_designator =
        operating_designator_from_segments(&row.operating_designator, &matching_segments);
    row.duplicate_designators = duplicate_designators_from_segments(&matching_segments);
    row.joint_operation_airline_designators = joint_operation_airlines_from_segments(
        &block.leg.joint_operation_airline_designators,
        &matching_segments,
    );
    row.meal_service_note =
        meal_service_note_from_segments(block.leg.meal_service_note.as_ref(), &matching_segments);
    row.in_flight_service_info = in_flight_service_info_from_segments(&matching_segments);
    row.electronic_ticketing_info = electronic_ticketing_info_from_segments(&matching_segments);

    row
}

fn type3_leg_row_from_record(record: &FlightLegRecord) -> FlightType3LegRow {
    FlightType3LegRow {
        leg_sequence: record.leg_sequence,
        departure_station: record.departure_station.clone(),
        arrival_station: record.arrival_station.clone(),
        departure_terminal: record.departure_terminal.clone(),
        arrival_terminal: record.arrival_terminal.clone(),
        prbd: record.prbd.clone(),
        prbm: record.prbm.clone(),
        meal_service_note: record.meal_service_note.clone(),
        joint_operation_airline_designators: record.joint_operation_airline_designators.clone(),
        secure_flight_indicator: record.secure_flight_indicator.clone(),
        itinerary_variation_overflow: record.itinerary_variation_overflow.clone(),
        aircraft_owner: record.aircraft_owner.clone(),
        cockpit_crew_employer: record.cockpit_crew_employer.clone(),
        cabin_crew_employer: record.cabin_crew_employer.clone(),
        onward_airline_designator: record.onward_airline_designator.clone(),
        onward_flight_number: record.onward_flight_number.clone(),
        onward_aircraft_rotation_layover: record.onward_aircraft_rotation_layover.clone(),
        onward_operational_suffix: record.onward_operational_suffix.clone(),
        operating_airline_disclosure: record.operating_airline_disclosure.clone(),
        traffic_restriction_code: record.traffic_restriction_code.clone(),
        traffic_restriction_code_leg_overflow_indicator: record
            .traffic_restriction_code_leg_overflow_indicator
            .clone(),
        operating_designator: self_designator(record),
        duplicate_designators: vec![],
        in_flight_service_info: None,
        electronic_ticketing_info: None,
    }
}

fn matching_segment_records<'a>(
    all_blocks: &'a [FlightLegBlock],
    start_leg_sequence: u8,
    end_leg_sequence: u8,
) -> Vec<&'a SegmentDataRecord> {
    let Some(first_block) = all_blocks
        .iter()
        .find(|block| block.leg.leg_sequence == start_leg_sequence)
    else {
        return vec![];
    };
    let Some(last_block) = all_blocks
        .iter()
        .find(|block| block.leg.leg_sequence == end_leg_sequence)
    else {
        return vec![];
    };

    all_blocks
        .iter()
        .flat_map(|block| block.segments.iter())
        .filter(|segment| {
            segment_matches_span(
                segment,
                start_leg_sequence,
                end_leg_sequence,
                first_block.leg.departure_station.as_str(),
                last_block.leg.arrival_station.as_str(),
            )
        })
        .collect()
}

fn matching_segments_for_span<'a>(
    segments: &'a [SegmentDataRecord],
    start_leg_sequence: u8,
    end_leg_sequence: u8,
    departure_station: &str,
    arrival_station: &str,
) -> Vec<&'a SegmentDataRecord> {
    segments
        .iter()
        .filter(|segment| {
            segment_matches_span(
                segment,
                start_leg_sequence,
                end_leg_sequence,
                departure_station,
                arrival_station,
            )
        })
        .collect()
}

fn segment_matches_span(
    segment: &SegmentDataRecord,
    start_leg_sequence: u8,
    end_leg_sequence: u8,
    departure_station: &str,
    arrival_station: &str,
) -> bool {
    board_indicator_for_leg_sequence(start_leg_sequence)
        .zip(off_indicator_for_leg_sequence(end_leg_sequence))
        .map(|(board_indicator, off_indicator)| {
            segment.board_point_indicator == board_indicator
                && segment.off_point_indicator == off_indicator
                && segment.board_point == departure_station
                && segment.off_point == arrival_station
        })
        .unwrap_or(false)
}

fn board_indicator_for_leg_sequence(leg_sequence: u8) -> Option<char> {
    char::from_u32(u32::from(b'A') + u32::from(leg_sequence.saturating_sub(1)))
}

fn off_indicator_for_leg_sequence(leg_sequence: u8) -> Option<char> {
    char::from_u32(u32::from(b'A') + u32::from(leg_sequence))
}

fn operating_designator_for_span(
    default_designator: &FlightDesignatorRow,
    merged_legs: &[FlightType3LegRow],
    matching_segments: &[&SegmentDataRecord],
) -> FlightDesignatorRow {
    if let Some(from_segment) = first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::DupLegCrossRefOpsLegId)
    })
    .and_then(parse_flight_designator)
    {
        return from_segment;
    }

    common_operating_designator(merged_legs).unwrap_or_else(|| default_designator.clone())
}

fn operating_designator_from_segments(
    default_designator: &FlightDesignatorRow,
    matching_segments: &[&SegmentDataRecord],
) -> FlightDesignatorRow {
    first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::DupLegCrossRefOpsLegId)
    })
    .and_then(parse_flight_designator)
    .unwrap_or_else(|| default_designator.clone())
}

fn duplicate_designators_for_span(
    matching_segments: &[&SegmentDataRecord],
) -> Vec<FlightDesignatorRow> {
    duplicate_designators_from_segments(matching_segments)
}

fn duplicate_designators_from_segments(
    matching_segments: &[&SegmentDataRecord],
) -> Vec<FlightDesignatorRow> {
    first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::DupLegCrossRefDupLegId)
    })
    .map(parse_flight_designator_list)
    .unwrap_or_default()
}

fn joint_operation_airlines_for_span(
    selected: &[FlightLegBlock],
    matching_segments: &[&SegmentDataRecord],
) -> Vec<String> {
    if let Some(data) = first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::JointOpAirlineSegOverride)
    }) {
        return parse_joint_operation_airlines(data);
    }

    let leg_values = selected
        .iter()
        .map(|block| block.leg.joint_operation_airline_designators.clone())
        .collect::<Vec<_>>();
    common_string_lists(&leg_values)
}

fn joint_operation_airlines_from_segments(
    base: &[String],
    matching_segments: &[&SegmentDataRecord],
) -> Vec<String> {
    if let Some(data) = first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::JointOpAirlineSegOverride)
    }) {
        return parse_joint_operation_airlines(data);
    }

    base.to_vec()
}

fn meal_service_note_for_span(
    selected: &[FlightLegBlock],
    matching_segments: &[&SegmentDataRecord],
) -> Option<String> {
    if let Some(data) = first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::MealServiceNoteExceedingMaxLength)
    }) {
        return Some(data.trim().to_string());
    }

    let meals = selected
        .iter()
        .map(|block| block.leg.meal_service_note.clone())
        .collect::<Vec<_>>();
    common_optional_string(&meals)
}

fn meal_service_note_from_segments(
    base: Option<&String>,
    matching_segments: &[&SegmentDataRecord],
) -> Option<String> {
    first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::MealServiceNoteExceedingMaxLength)
    })
    .map(|data| data.trim().to_string())
    .or_else(|| base.cloned())
}

fn in_flight_service_info_for_span(
    selected: &[FlightLegBlock],
    matching_segments: &[&SegmentDataRecord],
) -> Option<String> {
    if let Some(data) = in_flight_service_info_from_segments(matching_segments) {
        return Some(data);
    }

    if selected.len() == 1 {
        return None;
    }

    None
}

fn in_flight_service_info_from_segments(
    matching_segments: &[&SegmentDataRecord],
) -> Option<String> {
    first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::InFlightServiceInfo)
    })
    .map(|data| data.trim().to_string())
}

fn electronic_ticketing_info_for_span(
    selected: &[FlightLegBlock],
    matching_segments: &[&SegmentDataRecord],
) -> Option<String> {
    if let Some(data) = electronic_ticketing_info_from_segments(matching_segments) {
        return Some(data);
    }

    let leg_values = selected
        .iter()
        .map(|block| {
            let leg_segments = matching_segments_for_span(
                &block.segments,
                block.leg.leg_sequence,
                block.leg.leg_sequence,
                block.leg.departure_station.as_str(),
                block.leg.arrival_station.as_str(),
            );
            electronic_ticketing_info_from_segments(&leg_segments)
        })
        .collect::<Vec<_>>();

    if !leg_values.is_empty()
        && leg_values
            .iter()
            .all(|value| value.as_deref() == Some("ET"))
    {
        Some("ET".to_string())
    } else {
        None
    }
}

fn electronic_ticketing_info_from_segments(
    matching_segments: &[&SegmentDataRecord],
) -> Option<String> {
    first_matching_segment_data(matching_segments, |dei| {
        matches!(dei, Dei::ElectronicTicketingInfo)
    })
    .map(|data| data.trim().to_string())
}

fn first_matching_segment_data<'a, F>(
    matching_segments: &'a [&'a SegmentDataRecord],
    predicate: F,
) -> Option<&'a str>
where
    F: Fn(&Dei) -> bool,
{
    matching_segments
        .iter()
        .find(|segment| predicate(&segment.dei))
        .map(|segment| segment.data.as_str())
}

fn parse_flight_designator_list(data: &str) -> Vec<FlightDesignatorRow> {
    data.split('/')
        .filter_map(parse_flight_designator)
        .collect()
}

fn parse_flight_designator(value: &str) -> Option<FlightDesignatorRow> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let padded = format!("{trimmed:<8}");
    let company = padded.get(0..3)?.trim();
    let flight_number = padded.get(3..7)?.trim();
    let suffix = padded
        .get(7..8)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    if company.is_empty() || flight_number.is_empty() {
        return None;
    }

    Some(FlightDesignatorRow {
        company: company.to_string(),
        flight_number: flight_number.to_string(),
        operational_suffix: suffix,
    })
}

fn parse_joint_operation_airlines(data: &str) -> Vec<String> {
    data.split('/')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn common_operating_designator(legs: &[FlightType3LegRow]) -> Option<FlightDesignatorRow> {
    let first = legs.first()?.operating_designator.clone();
    if legs.iter().all(|leg| leg.operating_designator == first) {
        Some(first)
    } else {
        None
    }
}

fn common_joint_operation_airlines(records: &[FlightLegRecord]) -> Vec<String> {
    let lists = records
        .iter()
        .map(|record| record.joint_operation_airline_designators.clone())
        .collect::<Vec<_>>();
    common_string_lists(&lists)
}

fn common_string_lists(lists: &[Vec<String>]) -> Vec<String> {
    let Some(first) = lists.first() else {
        return vec![];
    };

    first
        .iter()
        .filter(|candidate| lists.iter().all(|list| list.contains(candidate)))
        .cloned()
        .collect()
}

fn common_meal_service_note(records: &[FlightLegRecord]) -> Option<String> {
    let meals = records
        .iter()
        .map(|record| record.meal_service_note.clone())
        .collect::<Vec<_>>();
    common_optional_string(&meals)
}

fn common_optional_string(values: &[Option<String>]) -> Option<String> {
    let first = values.first()?.clone()?;
    if values.iter().all(|value| value.as_ref() == Some(&first)) {
        Some(first)
    } else {
        None
    }
}

fn operates_on_date(plan: &FlightPlan, date: NaiveDate) -> bool {
    let weekday_index = date.weekday().num_days_from_monday() as usize;
    if !plan.weekdays[weekday_index] {
        return false;
    }

    let interval_weeks = i64::from(plan.frequency_rate.unwrap_or(1));
    let days_since_start = (date - plan.start_date).num_days();
    days_since_start >= 0 && (days_since_start / 7) % interval_weeks == 0
}

fn relative_arrival_day_offset(dep_date_var: char, arr_date_var: char) -> Result<i64> {
    Ok(day_variation_to_offset(arr_date_var)? - day_variation_to_offset(dep_date_var)?)
}

fn day_variation_to_offset(value: char) -> Result<i64> {
    match value {
        ' ' | '0' => Ok(0),
        '1'..='9' => Ok((value as u8 - b'0') as i64),
        'A' | 'J' => Ok(-1),
        _ => Err(anyhow!("unsupported SSIM date variation '{value}'")),
    }
}

fn compute_block_time_with_offsets(
    dep_time: NaiveTime,
    dep_tz: &str,
    arr_time: NaiveTime,
    arr_tz: &str,
    arrival_day_offset: i64,
) -> Result<Duration> {
    let reference_date = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let dep_offset = FixedOffset::from_str(dep_tz)
        .map_err(|_| anyhow!("invalid departure timezone {dep_tz}"))?;
    let arr_offset =
        FixedOffset::from_str(arr_tz).map_err(|_| anyhow!("invalid arrival timezone {arr_tz}"))?;

    let dep_local = dep_offset
        .from_local_datetime(&reference_date.and_time(dep_time))
        .single()
        .ok_or_else(|| anyhow!("invalid departure local datetime"))?;
    let arr_local = arr_offset
        .from_local_datetime(
            &(reference_date + Duration::days(arrival_day_offset)).and_time(arr_time),
        )
        .single()
        .ok_or_else(|| anyhow!("invalid arrival local datetime"))?;

    Ok(arr_local.to_utc() - dep_local.to_utc())
}
