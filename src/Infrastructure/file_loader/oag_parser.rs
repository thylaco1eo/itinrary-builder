use crate::Infrastructure::file_loader::dei::Dei;
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use std::str::FromStr;
// --- Enums & Helper Types ---

#[derive(Debug, PartialEq)]
pub enum TimeMode {
    UTC,
    Local,
}

impl FromStr for TimeMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "U" => Ok(TimeMode::UTC),
            "L" => Ok(TimeMode::Local),
            _ => Err(anyhow!("Invalid Time Mode: {}", s)),
        }
    }
}

// --- Data Structures ---

#[derive(Debug)]
pub struct HeaderRecord {
    pub title: String,
    pub record_serial_number: u32,
}

#[derive(Debug)]
pub struct SeasonRecord {
    pub time_mode: TimeMode,
    pub airline_designator: String,
    pub valid_from: NaiveDate,
    pub valid_to: NaiveDate,
    pub record_serial_number: u32,
}

#[derive(Debug, Clone)]
pub struct FlightLegRecord {
    // Identity
    pub airline_designator: String, // Bytes 3-5
    pub flight_number: String,      // Bytes 6-9
    pub itinerary_variation: u8,    // Bytes 10-11
    pub leg_sequence: u8,           // Bytes 12-13
    pub service_type: String,       // Byte 14

    // Period
    pub valid_from: NaiveDate,      // Bytes 15-21
    pub valid_to: NaiveDate,        // Bytes 22-28
    pub days_of_operation: String,  // Bytes 29-35 (e.g. "1234567")
    pub frequency_rate: Option<u8>, // Byte 36, Chapter 7 currently uses 2 for fortnightly service

    // Route & Times
    pub departure_station: String,          // Bytes 37-39
    pub pax_std: NaiveTime,                 // Bytes 40-43
    pub aircraft_std: NaiveTime,            // Bytes 44-47
    pub time_var_dep: String,               // Bytes 48-52 (UTC Offset)
    pub departure_terminal: Option<String>, // Bytes 53-54

    pub arrival_station: String,          // Bytes 55-57
    pub aircraft_sta: NaiveTime,          // Bytes 58-61
    pub pax_sta: NaiveTime,               // Bytes 62-65
    pub time_var_arr: String,             // Bytes 66-70 (UTC Offset)
    pub arrival_terminal: Option<String>, // Bytes 71-72

    // Details
    pub aircraft_type: String,                            // Bytes 73-75
    pub prbd: Option<String>,                             // Bytes 76-95
    pub prbm: Option<String>,                             // Bytes 96-100
    pub meal_service_note: Option<String>,                // Bytes 101-110
    pub joint_operation_airline_designators: Vec<String>, // Bytes 111-119
    pub secure_flight_indicator: Option<String>,          // Byte 122
    pub itinerary_variation_overflow: Option<String>,     // Byte 128
    pub aircraft_owner: Option<String>,                   // Bytes 129-131
    pub cockpit_crew_employer: Option<String>,            // Bytes 132-134
    pub cabin_crew_employer: Option<String>,              // Bytes 135-137
    pub onward_airline_designator: Option<String>,        // Bytes 138-140
    pub onward_flight_number: Option<String>,             // Bytes 141-144
    pub onward_aircraft_rotation_layover: Option<String>, // Byte 145
    pub onward_operational_suffix: Option<String>,        // Byte 146
    pub operating_airline_disclosure: Option<String>,     // Byte 149
    pub traffic_restriction_code: Option<String>,         // Bytes 150-160
    pub traffic_restriction_code_leg_overflow_indicator: Option<String>, // Byte 161
    pub aircraft_config: Option<String>,                  // Bytes 173-192

    // Date Variations (0-9, J)
    pub dep_date_var: char, // Byte 193
    pub arr_date_var: char, // Byte 194

    pub record_serial_number: u32, // Bytes 195-200
}

#[derive(Debug, Clone)]
pub struct SegmentDataRecord {
    pub board_point_indicator: char,
    pub off_point_indicator: char,
    pub raw_dei: String, // 原始的 "050" 字符串
    pub dei: Dei,        // 新增：解析后的强类型枚举
    pub board_point: String,
    pub off_point: String,
    pub data: String,
}

#[derive(Debug)]
pub struct TrailerRecord {
    pub airline_designator: String,
    pub check_serial_number: u32, // Bytes 188-193 (Should match prev record)
    pub continuation_code: char,  // Byte 194 (C or E)
    pub record_serial_number: u32,
}

// Wrapper to hold any record type
#[derive(Debug)]
pub enum OagRecord {
    Header(HeaderRecord),
    Season(SeasonRecord),
    FlightLeg(FlightLegRecord),
    SegmentData(SegmentDataRecord),
    Trailer(TrailerRecord),
    Unknown(u8, String),
}

// --- Parsing Logic ---

pub struct OagParser;

impl OagParser {
    // Helper: Parse string and trim
    fn parse_str(line: &str, start: usize, end: usize) -> Result<String> {
        if line.len() < end {
            return Err(anyhow!("Line too short"));
        }
        Ok(line[start - 1..end].trim().to_string())
    }

    // Helper: Parse Date DDMMMYY
    fn parse_date(line: &str, start: usize, end: usize) -> Result<NaiveDate> {
        let s = Self::parse_str(line, start, end)?;
        NaiveDate::parse_from_str(&s, "%d%b%y")
            .map_err(|e| anyhow!("Date parse error '{}': {}", s, e))
    }

    // Helper: Parse Time HHMM
    fn parse_time(line: &str, start: usize, end: usize) -> Result<NaiveTime> {
        let s = Self::parse_str(line, start, end)?;
        NaiveTime::parse_from_str(&s, "%H%M")
            .map_err(|e| anyhow!("Time parse error '{}': {}", s, e))
    }

    fn parse_optional_str(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        let s = Self::parse_str(line, start, end)?;
        if s.is_empty() {
            Ok(None)
        } else {
            Ok(Some(s))
        }
    }

    fn parse_designator_chunks(line: &str, start: usize, end: usize) -> Result<Vec<String>> {
        if line.len() < end {
            return Err(anyhow!("Line too short"));
        }

        Ok(line[start - 1..end]
            .as_bytes()
            .chunks(3)
            .filter_map(|chunk| {
                let value = std::str::from_utf8(chunk).ok()?.trim();
                if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                }
            })
            .collect())
    }

    // Helper: Parse u8/u32 safely
    fn parse_num<T: FromStr>(line: &str, start: usize, end: usize) -> Result<T>
    where
        T::Err: std::fmt::Display,
    {
        let s = Self::parse_str(line, start, end)?;
        s.parse::<T>()
            .map_err(|e| anyhow!("Number parse error '{}': {}", s, e))
    }

    fn parse_optional_num<T: FromStr>(line: &str, start: usize, end: usize) -> Result<Option<T>>
    where
        T::Err: std::fmt::Display,
    {
        let s = Self::parse_str(line, start, end)?;
        if s.is_empty() {
            return Ok(None);
        }

        s.parse::<T>()
            .map(Some)
            .map_err(|e| anyhow!("Number parse error '{}': {}", s, e))
    }

    pub fn parse_line(line: &str) -> Result<OagRecord> {
        let clean_line = line.trim_end();
        if clean_line.is_empty() {
            return Err(anyhow!("Empty line"));
        }

        let record_type_char = clean_line.chars().next().unwrap();

        match record_type_char {
            '1' => Ok(OagRecord::Header(HeaderRecord {
                title: Self::parse_str(clean_line, 2, 35)?,
                record_serial_number: Self::parse_num(clean_line, 195, 200)?,
            })),
            '2' => Ok(OagRecord::Season(SeasonRecord {
                time_mode: Self::parse_str(clean_line, 2, 2)?.parse()?,
                airline_designator: Self::parse_str(clean_line, 3, 5)?,
                valid_from: Self::parse_date(clean_line, 15, 21)?,
                valid_to: Self::parse_date(clean_line, 22, 28)?,
                record_serial_number: Self::parse_num(clean_line, 195, 200)?,
            })),
            '3' => {
                Ok(OagRecord::FlightLeg(FlightLegRecord {
                    airline_designator: Self::parse_str(clean_line, 3, 5)?,
                    flight_number: Self::parse_str(clean_line, 6, 9)?,
                    itinerary_variation: Self::parse_num(clean_line, 10, 11)?,
                    leg_sequence: Self::parse_num(clean_line, 12, 13)?,
                    service_type: Self::parse_str(clean_line, 14, 14)?,
                    valid_from: Self::parse_date(clean_line, 15, 21)?,
                    valid_to: Self::parse_date(clean_line, 22, 28)?,
                    days_of_operation: Self::parse_str(clean_line, 29, 35)?, // raw "1234567"
                    frequency_rate: Self::parse_optional_num(clean_line, 36, 36)?,
                    departure_station: Self::parse_str(clean_line, 37, 39)?,
                    pax_std: Self::parse_time(clean_line, 40, 43)?,
                    aircraft_std: Self::parse_time(clean_line, 44, 47)?,
                    time_var_dep: Self::parse_str(clean_line, 48, 52)?,
                    departure_terminal: Self::parse_optional_str(clean_line, 53, 54)?,
                    arrival_station: Self::parse_str(clean_line, 55, 57)?,
                    aircraft_sta: Self::parse_time(clean_line, 58, 61)?,
                    pax_sta: Self::parse_time(clean_line, 62, 65)?,
                    time_var_arr: Self::parse_str(clean_line, 66, 70)?,
                    arrival_terminal: Self::parse_optional_str(clean_line, 71, 72)?,
                    aircraft_type: Self::parse_str(clean_line, 73, 75)?,
                    prbd: Self::parse_optional_str(clean_line, 76, 95)?,
                    prbm: Self::parse_optional_str(clean_line, 96, 100)?,
                    meal_service_note: Self::parse_optional_str(clean_line, 101, 110)?,
                    joint_operation_airline_designators: Self::parse_designator_chunks(
                        clean_line, 111, 119,
                    )?,
                    secure_flight_indicator: Self::parse_optional_str(clean_line, 122, 122)?,
                    itinerary_variation_overflow: Self::parse_optional_str(clean_line, 128, 128)?,
                    aircraft_owner: Self::parse_optional_str(clean_line, 129, 131)?,
                    cockpit_crew_employer: Self::parse_optional_str(clean_line, 132, 134)?,
                    cabin_crew_employer: Self::parse_optional_str(clean_line, 135, 137)?,
                    onward_airline_designator: Self::parse_optional_str(clean_line, 138, 140)?,
                    onward_flight_number: Self::parse_optional_str(clean_line, 141, 144)?,
                    onward_aircraft_rotation_layover: Self::parse_optional_str(
                        clean_line, 145, 145,
                    )?,
                    onward_operational_suffix: Self::parse_optional_str(clean_line, 146, 146)?,
                    operating_airline_disclosure: Self::parse_optional_str(clean_line, 149, 149)?,
                    traffic_restriction_code: Self::parse_optional_str(clean_line, 150, 160)?,
                    traffic_restriction_code_leg_overflow_indicator: Self::parse_optional_str(
                        clean_line, 161, 161,
                    )?,
                    aircraft_config: {
                        let s = Self::parse_str(clean_line, 173, 192)?;
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    },
                    dep_date_var: Self::parse_str(clean_line, 193, 193)?
                        .chars()
                        .next()
                        .unwrap_or('0'),
                    arr_date_var: Self::parse_str(clean_line, 194, 194)?
                        .chars()
                        .next()
                        .unwrap_or('0'),
                    record_serial_number: Self::parse_num(clean_line, 195, 200)?,
                }))
            }
            '4' => {
                let dei_enum = Dei::from_code(Self::parse_str(clean_line, 31, 33)?.as_str());
                Ok(OagRecord::SegmentData(SegmentDataRecord {
                    board_point_indicator: Self::parse_str(clean_line, 29, 29)?
                        .chars()
                        .next()
                        .unwrap_or(' '),
                    off_point_indicator: Self::parse_str(clean_line, 30, 30)?
                        .chars()
                        .next()
                        .unwrap_or(' '),
                    raw_dei: Self::parse_str(clean_line, 31, 33)?,
                    dei: dei_enum,
                    board_point: Self::parse_str(clean_line, 34, 36)?,
                    off_point: Self::parse_str(line, 37, 39)?,
                    data: Self::parse_str(line, 40, 194)?,
                }))
            }
            '5' => Ok(OagRecord::Trailer(TrailerRecord {
                airline_designator: Self::parse_str(clean_line, 3, 5)?,
                check_serial_number: Self::parse_num(clean_line, 188, 193)?,
                continuation_code: Self::parse_str(clean_line, 194, 194)?
                    .chars()
                    .next()
                    .unwrap_or('E'),
                record_serial_number: Self::parse_num(clean_line, 195, 200)?,
            })),
            _ => Ok(OagRecord::Unknown(
                record_type_char.to_digit(10).unwrap_or(0) as u8,
                clean_line.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_range(buffer: &mut [u8], start: usize, end: usize, value: &str) {
        let width = end - start + 1;
        let bytes = value.as_bytes();
        buffer[start - 1..end].fill(b' ');
        buffer[start - 1..start - 1 + bytes.len().min(width)]
            .copy_from_slice(&bytes[..bytes.len().min(width)]);
    }

    #[test]
    fn parses_type3_optional_leg_fields() {
        let mut record = vec![b' '; 200];
        set_range(&mut record, 1, 1, "3");
        set_range(&mut record, 3, 5, "CA");
        set_range(&mut record, 6, 9, "897");
        set_range(&mut record, 10, 11, "01");
        set_range(&mut record, 12, 13, "01");
        set_range(&mut record, 14, 14, "J");
        set_range(&mut record, 15, 21, "05FEB26");
        set_range(&mut record, 22, 28, "26MAR26");
        set_range(&mut record, 29, 35, "1  4  7");
        set_range(&mut record, 36, 36, "2");
        set_range(&mut record, 37, 39, "PEK");
        set_range(&mut record, 40, 43, "1500");
        set_range(&mut record, 44, 47, "1500");
        set_range(&mut record, 48, 52, "+0800");
        set_range(&mut record, 53, 54, "T3");
        set_range(&mut record, 55, 57, "MAD");
        set_range(&mut record, 58, 61, "2010");
        set_range(&mut record, 62, 65, "2010");
        set_range(&mut record, 66, 70, "+0100");
        set_range(&mut record, 71, 72, "T1");
        set_range(&mut record, 73, 75, "789");
        set_range(&mut record, 76, 95, "JCDZRGEYBMUHQVWSTLPN");
        set_range(&mut record, 96, 100, "ABCDE");
        set_range(&mut record, 122, 122, "S");
        set_range(&mut record, 128, 128, "A");
        set_range(&mut record, 129, 131, "CA");
        set_range(&mut record, 132, 134, "CCA");
        set_range(&mut record, 135, 137, "CCB");
        set_range(&mut record, 138, 140, "LH");
        set_range(&mut record, 141, 144, "1234");
        set_range(&mut record, 145, 145, "1");
        set_range(&mut record, 146, 146, "Z");
        set_range(&mut record, 149, 149, "L");
        set_range(&mut record, 150, 160, "A  C   K  Z");
        set_range(&mut record, 161, 161, "Z");
        set_range(&mut record, 173, 192, "J008Y148");
        set_range(&mut record, 193, 193, "0");
        set_range(&mut record, 194, 194, "1");
        set_range(&mut record, 195, 200, "000123");

        let line = String::from_utf8(record).unwrap();
        let parsed = OagParser::parse_line(&line).unwrap();

        let OagRecord::FlightLeg(leg) = parsed else {
            panic!("expected flight leg record");
        };

        assert_eq!(leg.frequency_rate, Some(2));
        assert_eq!(leg.departure_terminal.as_deref(), Some("T3"));
        assert_eq!(leg.arrival_terminal.as_deref(), Some("T1"));
        assert_eq!(leg.prbd.as_deref(), Some("JCDZRGEYBMUHQVWSTLPN"));
        assert_eq!(leg.prbm.as_deref(), Some("ABCDE"));
        assert_eq!(leg.meal_service_note.as_deref(), None);
        assert!(leg.joint_operation_airline_designators.is_empty());
        assert_eq!(leg.secure_flight_indicator.as_deref(), Some("S"));
        assert_eq!(leg.itinerary_variation_overflow.as_deref(), Some("A"));
        assert_eq!(leg.aircraft_owner.as_deref(), Some("CA"));
        assert_eq!(leg.cockpit_crew_employer.as_deref(), Some("CCA"));
        assert_eq!(leg.cabin_crew_employer.as_deref(), Some("CCB"));
        assert_eq!(leg.onward_airline_designator.as_deref(), Some("LH"));
        assert_eq!(leg.onward_flight_number.as_deref(), Some("1234"));
        assert_eq!(leg.onward_aircraft_rotation_layover.as_deref(), Some("1"));
        assert_eq!(leg.onward_operational_suffix.as_deref(), Some("Z"));
        assert_eq!(leg.operating_airline_disclosure.as_deref(), Some("L"));
        assert_eq!(leg.traffic_restriction_code.as_deref(), Some("A  C   K  Z"));
        assert_eq!(
            leg.traffic_restriction_code_leg_overflow_indicator
                .as_deref(),
            Some("Z")
        );
    }

    #[test]
    fn parses_segment_data_indicators() {
        let line = "4 CA  4690101J              AB010CKGTSABR 2756                                                                                                                                                    004037";
        let parsed = OagParser::parse_line(line).unwrap();

        let OagRecord::SegmentData(segment) = parsed else {
            panic!("expected segment data record");
        };

        assert_eq!(segment.board_point_indicator, 'A');
        assert_eq!(segment.off_point_indicator, 'B');
        assert_eq!(segment.raw_dei, "010");
        assert_eq!(segment.board_point, "CKG");
        assert_eq!(segment.off_point, "TSA");
        assert_eq!(segment.data.trim(), "BR 2756");
    }
}

// // --- Main for Testing ---
//
// fn main() {
//     // 构造模拟数据 (注意：真实数据会有精确的空格填充，这里为了演示做了简化)
//     // 假设每行都正好是 200 字节 (这里只填关键字段，空格用 ... 代替)
//
//     // Type 3: AA 100, Var 01, Seq 01, JFK-LHR, STD 1800, STA 0600
//     // Date Vars: 0 (Dep same day), 1 (Arr next day)
//     let type3 = "3 AA 01000101J13FEB2620OCT261234567  JFK18001800-0500  LHR06000600+0000  777                                                                                                        01000003";
//
//     // Type 4: Linked to AA 100 Var 01 Seq 01, DEI 050 (Meal), JFK-LHR, Data: M/D (Meal/Dinner)
//     let type4 = "4 AA 01000101J                D A050JFKLHRM/D                                                                                                                                         000004";
//
//     // Type 5: Trailer
//     let type5 = "5 AA                                                                                                                                                                       000004E000005";
//
//     let records = vec![type3, type4, type5];
//
//     for line in records {
//         // 在真实场景中，你需要处理 line 的长度，确保 padding 正确
//         // 这里我们假设输入是合法的
//         match OagParser::parse_line(line) {
//             Ok(rec) => match rec {
//                 OagRecord::FlightLeg(leg) => {
//                     println!("✈️  Flight: {}{}", leg.airline_designator, leg.flight_number);
//                     println!("    Route: {} -> {}", leg.departure_station, leg.arrival_station);
//                     println!("    Time: {} (Offset {}) -> {} (Offset {})",
//                              leg.aircraft_std, leg.time_var_dep, leg.aircraft_sta, leg.time_var_arr);
//                     println!("    Arrives Next Day?: {}", if leg.arr_date_var == '1' { "Yes" } else { "No" });
//                 },
//                 OagRecord::SegmentData(seg) => {
//                     println!("ℹ️  Segment Info (DEI {}): {} -> {}", seg.data_element_id, seg.board_point, seg.off_point);
//                     println!("    Data: {}", seg.data);
//                 },
//                 OagRecord::Trailer(t) => {
//                     println!("🏁 End of Block. Check Serial: {}", t.check_serial_number);
//                 },
//                 _ => println!("Other record..."),
//             },
//             Err(e) => println!("Error: {}", e),
//         }
//     }
//}
