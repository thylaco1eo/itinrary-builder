use chrono::{NaiveDate, NaiveTime};
use std::str::FromStr;
use anyhow::{anyhow, Result};
use crate::Infrastructure::file_loader::dei::Dei;
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

#[derive(Debug)]
pub struct FlightLegRecord {
    // Identity
    pub airline_designator: String,   // Bytes 3-5
    pub flight_number: String,        // Bytes 6-9
    pub itinerary_variation: u8,      // Bytes 10-11
    pub leg_sequence: u8,             // Bytes 12-13
    pub service_type: String,         // Byte 14

    // Period
    pub valid_from: NaiveDate,        // Bytes 15-21
    pub valid_to: NaiveDate,          // Bytes 22-28
    pub days_of_operation: String,    // Bytes 29-35 (e.g. "1234567")

    // Route & Times
    pub departure_station: String,    // Bytes 37-39
    pub pax_std: NaiveTime,           // Bytes 40-43
    pub aircraft_std: NaiveTime,      // Bytes 44-47
    pub time_var_dep: String,         // Bytes 48-52 (UTC Offset)

    pub arrival_station: String,      // Bytes 55-57
    pub aircraft_sta: NaiveTime,      // Bytes 58-61
    pub pax_sta: NaiveTime,           // Bytes 62-65
    pub time_var_arr: String,         // Bytes 66-70 (UTC Offset)

    // Details
    pub aircraft_type: String,        // Bytes 73-75
    pub aircraft_config: Option<String>, // Bytes 173-192 OR 76-95 (Simplified logic)

    // Date Variations (0-9, J)
    pub dep_date_var: char,           // Byte 193
    pub arr_date_var: char,           // Byte 194

    pub record_serial_number: u32,    // Bytes 195-200
}

#[derive(Debug, Clone)]
pub struct SegmentDataRecord {
    pub raw_dei: String, // 原始的 "050" 字符串
    pub dei: Dei,        // 新增：解析后的强类型枚举
    pub board_point: String,
    pub off_point: String,
    pub data: String,
}

#[derive(Debug)]
pub struct TrailerRecord {
    pub airline_designator: String,
    pub check_serial_number: u32,     // Bytes 188-193 (Should match prev record)
    pub continuation_code: char,      // Byte 194 (C or E)
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

    // Helper: Parse u8/u32 safely
    fn parse_num<T: FromStr>(line: &str, start: usize, end: usize) -> Result<T>
    where T::Err: std::fmt::Display {
        let s = Self::parse_str(line, start, end)?;
        s.parse::<T>().map_err(|e| anyhow!("Number parse error '{}': {}", s, e))
    }

    pub fn parse_line(line: &str) -> Result<OagRecord> {
        let clean_line = line.trim_end();
        if clean_line.is_empty() { return Err(anyhow!("Empty line")); }

        let record_type_char = clean_line.chars().next().unwrap();

        match record_type_char {
            '1' => {
                Ok(OagRecord::Header(HeaderRecord {
                    title: Self::parse_str(clean_line, 2, 35)?,
                    record_serial_number: Self::parse_num(clean_line, 195, 200)?,
                }))
            }
            '2' => {
                Ok(OagRecord::Season(SeasonRecord {
                    time_mode: Self::parse_str(clean_line, 2, 2)?.parse()?,
                    airline_designator: Self::parse_str(clean_line, 3, 5)?,
                    valid_from: Self::parse_date(clean_line, 15, 21)?,
                    valid_to: Self::parse_date(clean_line, 22, 28)?,
                    record_serial_number: Self::parse_num(clean_line, 195, 200)?,
                }))
            }
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
                    departure_station: Self::parse_str(clean_line, 37, 39)?,
                    pax_std: Self::parse_time(clean_line, 40, 43)?,
                    aircraft_std: Self::parse_time(clean_line, 44, 47)?,
                    time_var_dep: Self::parse_str(clean_line, 48, 52)?,
                    arrival_station: Self::parse_str(clean_line, 55, 57)?,
                    aircraft_sta: Self::parse_time(clean_line, 58, 61)?,
                    pax_sta: Self::parse_time(clean_line, 62, 65)?,
                    time_var_arr: Self::parse_str(clean_line, 66, 70)?,
                    aircraft_type: Self::parse_str(clean_line, 73, 75)?,
                    // 逻辑：如果 173-192 非空则读之，否则可以尝试读 76-95 (PRBD)
                    // 这里简化处理，只读 Config Area
                    aircraft_config: {
                        let s = Self::parse_str(clean_line, 173, 192)?;
                        if s.is_empty() { None } else { Some(s) }
                    },
                    dep_date_var: Self::parse_str(clean_line, 193, 193)?.chars().next().unwrap_or('0'),
                    arr_date_var: Self::parse_str(clean_line, 194, 194)?.chars().next().unwrap_or('0'),
                    record_serial_number: Self::parse_num(clean_line, 195, 200)?,
                }))
            }
            '4' => {
                let dei_enum = Dei::from_code(Self::parse_str(clean_line,31,33)?.as_str());
                Ok(OagRecord::SegmentData(SegmentDataRecord{
                    raw_dei: Self::parse_str(clean_line,31,33)?,
                    dei: dei_enum,
                    board_point: Self::parse_str(clean_line, 34, 36)?,
                    off_point: Self::parse_str(line, 37, 39)?,
                    data: Self::parse_str(line, 40, 194)?,
                }))
            }
            '5' => {
                Ok(OagRecord::Trailer(TrailerRecord {
                    airline_designator: Self::parse_str(clean_line, 3, 5)?,
                    check_serial_number: Self::parse_num(clean_line, 188, 193)?,
                    continuation_code: Self::parse_str(clean_line, 194, 194)?.chars().next().unwrap_or('E'),
                    record_serial_number: Self::parse_num(clean_line, 195, 200)?,
                }))
            }
            _ => {
                Ok(OagRecord::Unknown(
                    record_type_char.to_digit(10).unwrap_or(0) as u8,
                    clean_line.to_string()
                ))
            }
        }
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