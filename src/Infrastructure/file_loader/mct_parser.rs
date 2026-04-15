use crate::domain::mct::{
    AirportMctRecord, ConnectionBuildingFilter, MctActionIndicator, MctContentIndicator,
    MctHeaderRecord, MctTrailerRecord, ParsedConnectionBuildingFilterRecord, ParsedMctFile,
    ParsedMctRecord,
};
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use std::io::{BufRead, BufReader, Read};

#[derive(Debug)]
pub enum MctFileRecord {
    Header(MctHeaderRecord),
    Record(ParsedMctRecord),
    ConnectionBuildingFilter(ParsedConnectionBuildingFilterRecord),
    Trailer(MctTrailerRecord),
}

pub struct MctParser;

impl MctParser {
    pub fn parse_reader<R: Read>(reader: R) -> Result<ParsedMctFile> {
        let mut header: Option<MctHeaderRecord> = None;
        let mut records = Vec::new();
        let mut connection_building_filters = Vec::new();
        let mut trailer: Option<MctTrailerRecord> = None;

        for (line_index, line_result) in BufReader::new(reader).lines().enumerate() {
            let line_number = line_index + 1;
            let line = line_result
                .map_err(|error| anyhow!("failed to read line {line_number}: {error}"))?;

            match Self::parse_line(&line)? {
                MctFileRecord::Header(parsed) => {
                    if header.is_some() {
                        return Err(anyhow!("multiple MCT header records found"));
                    }
                    if !records.is_empty()
                        || !connection_building_filters.is_empty()
                        || trailer.is_some()
                    {
                        return Err(anyhow!(
                            "MCT header record must be the first record in the file"
                        ));
                    }
                    header = Some(parsed);
                }
                MctFileRecord::Record(parsed) => {
                    if header.is_none() {
                        return Err(anyhow!("MCT file is missing a header record"));
                    }
                    if trailer.is_some() {
                        return Err(anyhow!("MCT record found after trailer record"));
                    }
                    records.push(parsed);
                }
                MctFileRecord::ConnectionBuildingFilter(parsed) => {
                    if header.is_none() {
                        return Err(anyhow!("MCT file is missing a header record"));
                    }
                    if trailer.is_some() {
                        return Err(anyhow!(
                            "connection-building filter record found after trailer record"
                        ));
                    }
                    connection_building_filters.push(parsed);
                }
                MctFileRecord::Trailer(parsed) => {
                    if header.is_none() {
                        return Err(anyhow!("MCT trailer record found before header record"));
                    }
                    if trailer.is_some() {
                        return Err(anyhow!("multiple MCT trailer records found"));
                    }
                    trailer = Some(parsed);
                }
            }
        }

        let parsed = ParsedMctFile {
            header: header.ok_or_else(|| anyhow!("MCT file is missing a header record"))?,
            records,
            connection_building_filters,
            trailer: trailer.ok_or_else(|| anyhow!("MCT file is missing a trailer record"))?,
        };
        Self::validate_file(&parsed)?;
        Ok(parsed)
    }

    pub fn parse_line(raw_line: &str) -> Result<MctFileRecord> {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.as_bytes().len() != 200 {
            return Err(anyhow!(
                "MCT records must be exactly 200 bytes long, got {}",
                line.as_bytes().len()
            ));
        }

        match Self::field(line, 1, 1)? {
            "1" => Ok(MctFileRecord::Header(Self::parse_header(line)?)),
            "2" => Ok(MctFileRecord::Record(Self::parse_record(line)?)),
            "3" => {
                if Self::field(line, 194, 194)?.trim() == "E" {
                    Ok(MctFileRecord::Trailer(Self::parse_trailer(line)?))
                } else {
                    Ok(MctFileRecord::ConnectionBuildingFilter(
                        Self::parse_connection_building_filter(line)?,
                    ))
                }
            }
            "4" => Ok(MctFileRecord::Trailer(Self::parse_trailer(line)?)),
            value => Err(anyhow!("unsupported MCT record type: {value}")),
        }
    }

    fn parse_header(line: &str) -> Result<MctHeaderRecord> {
        let creation_date_utc = Self::required_str(line, 67, 73)?;
        Self::parse_ssim_date(&creation_date_utc)
            .map_err(|_| anyhow!("invalid MCT header creation date: {creation_date_utc}"))?;

        let creation_time_utc = Self::required_str(line, 74, 77)?;
        NaiveTime::parse_from_str(&creation_time_utc, "%H%M")
            .map_err(|_| anyhow!("invalid MCT header creation time: {creation_time_utc}"))?;

        Ok(MctHeaderRecord {
            title_of_contents: Self::required_str(line, 2, 31)?,
            creator_reference: Self::required_str(line, 32, 66)?,
            creation_date_utc,
            creation_time_utc,
            content_indicator: MctContentIndicator::from_code(&Self::required_str(line, 78, 78)?)?,
            record_serial_number: Self::required_u32(line, 195, 200)?,
        })
    }

    fn parse_record(line: &str) -> Result<ParsedMctRecord> {
        let raw_arrival_station = Self::optional_raw_field(line, 2, 4)?;
        let raw_departure_station = Self::optional_raw_field(line, 11, 13)?;

        let data = AirportMctRecord {
            arrival_station: Self::normalize_station(raw_arrival_station.as_deref()),
            time: Self::optional_hhmm(line, 5, 8)?,
            status: Self::required_str(line, 9, 10)?,
            departure_station: Self::normalize_station(raw_departure_station.as_deref()),
            requires_connection_building_filter: raw_departure_station.as_deref() == Some("***"),
            arrival_carrier: Self::optional_str(line, 14, 15)?,
            arrival_codeshare_indicator: Self::optional_flag(
                line,
                16,
                "arrival_codeshare_indicator",
            )?,
            arrival_codeshare_operating_carrier: Self::optional_str(line, 17, 18)?,
            departure_carrier: Self::optional_str(line, 19, 20)?,
            departure_codeshare_indicator: Self::optional_flag(
                line,
                21,
                "departure_codeshare_indicator",
            )?,
            departure_codeshare_operating_carrier: Self::optional_str(line, 22, 23)?,
            arrival_aircraft_type: Self::optional_str(line, 24, 26)?,
            arrival_aircraft_body: Self::optional_str(line, 27, 27)?,
            departure_aircraft_type: Self::optional_str(line, 28, 30)?,
            departure_aircraft_body: Self::optional_str(line, 31, 31)?,
            arrival_terminal: Self::optional_str(line, 32, 33)?,
            departure_terminal: Self::optional_str(line, 34, 35)?,
            previous_country: Self::optional_str(line, 36, 37)?,
            previous_station: Self::optional_station(line, 38, 40)?,
            next_country: Self::optional_str(line, 41, 42)?,
            next_station: Self::optional_station(line, 43, 45)?,
            arrival_flight_number_range_start: Self::optional_numeric(line, 46, 49)?,
            arrival_flight_number_range_end: Self::optional_numeric(line, 50, 53)?,
            departure_flight_number_range_start: Self::optional_numeric(line, 54, 57)?,
            departure_flight_number_range_end: Self::optional_numeric(line, 58, 61)?,
            previous_state: Self::optional_str(line, 62, 63)?,
            next_state: Self::optional_str(line, 64, 65)?,
            previous_region: Self::optional_str(line, 66, 68)?,
            next_region: Self::optional_str(line, 69, 71)?,
            effective_from_local: Self::optional_date(line, 72, 78)?,
            effective_to_local: Self::optional_date(line, 79, 85)?,
            suppression_indicator: Self::optional_flag_with_n(line, 87, "suppression_indicator")?,
            suppression_region: Self::optional_str(line, 88, 90)?,
            suppression_country: Self::optional_str(line, 91, 92)?,
            suppression_state: Self::optional_str(line, 93, 94)?,
        };
        data.validate()?;

        Ok(ParsedMctRecord {
            data,
            submitting_carrier_identifier: Self::optional_str(line, 95, 96)?,
            filing_date_local: Self::optional_date(line, 97, 103)?,
            action_indicator: Self::optional_action(line, 104, 104)?,
            record_serial_number: Self::required_u32(line, 195, 200)?,
        })
    }

    fn parse_connection_building_filter(
        line: &str,
    ) -> Result<ParsedConnectionBuildingFilterRecord> {
        Ok(ParsedConnectionBuildingFilterRecord {
            data: ConnectionBuildingFilter {
                submitting_carrier: Self::required_str(line, 2, 3)?,
                partner_carrier_codes: Self::parse_partner_carrier_codes(line, 5, 194)?,
            },
            record_serial_number: Self::required_u32(line, 195, 200)?,
        })
    }

    fn parse_trailer(line: &str) -> Result<MctTrailerRecord> {
        let end_code = Self::required_str(line, 194, 194)?;
        if end_code != "E" {
            return Err(anyhow!("MCT trailer end code must be E"));
        }

        Ok(MctTrailerRecord {
            end_code,
            serial_number_check_reference: Self::required_u32(line, 195, 200)?,
            record_serial_number: Self::required_u32(line, 195, 200)? + 1,
        })
    }

    fn validate_file(parsed: &ParsedMctFile) -> Result<()> {
        if parsed.header.record_serial_number != 1 {
            return Err(anyhow!("MCT header record serial number must be 000001"));
        }

        let mut previous_serial = parsed.header.record_serial_number;
        for record in &parsed.records {
            if record.record_serial_number != previous_serial + 1 {
                return Err(anyhow!(
                    "MCT record serial numbers must increase sequentially"
                ));
            }
            previous_serial = record.record_serial_number;
        }

        for filter in &parsed.connection_building_filters {
            if filter.record_serial_number != previous_serial + 1 {
                return Err(anyhow!(
                    "connection-building filter serial numbers must increase sequentially"
                ));
            }
            previous_serial = filter.record_serial_number;
        }

        if parsed.trailer.serial_number_check_reference != previous_serial {
            return Err(anyhow!(
                "MCT trailer check reference must match the previous record serial number"
            ));
        }

        if parsed
            .records
            .iter()
            .any(|record| record.data.requires_connection_building_filter)
            && parsed.connection_building_filters.is_empty()
        {
            return Err(anyhow!(
                "records marked with the connection-building filter require at least one type 3 record"
            ));
        }

        match parsed.header.content_indicator {
            MctContentIndicator::Full => {
                if parsed
                    .records
                    .iter()
                    .any(|record| record.action_indicator.is_some())
                {
                    return Err(anyhow!(
                        "full replacement MCT files must not contain action indicators"
                    ));
                }
            }
            MctContentIndicator::UpdatesOnly => {
                if parsed
                    .records
                    .iter()
                    .any(|record| record.action_indicator.is_none())
                {
                    return Err(anyhow!(
                        "update-only MCT files must include an action indicator on every MCT record"
                    ));
                }
            }
        }

        Ok(())
    }

    fn parse_partner_carrier_codes(line: &str, start: usize, end: usize) -> Result<Vec<String>> {
        let value = Self::field(line, start, end)?;
        Ok(value
            .as_bytes()
            .chunks(2)
            .filter_map(|chunk| {
                let code = std::str::from_utf8(chunk).ok()?.trim();
                if code.is_empty() {
                    None
                } else {
                    Some(code.to_string())
                }
            })
            .collect())
    }

    fn normalize_station(value: Option<&str>) -> Option<String> {
        match value {
            Some("***") | None => None,
            Some(value) => Some(value.to_string()),
        }
    }

    fn field<'a>(line: &'a str, start: usize, end: usize) -> Result<&'a str> {
        std::str::from_utf8(
            line.as_bytes()
                .get(start - 1..end)
                .ok_or_else(|| anyhow!("record is too short for bytes {start}-{end}"))?,
        )
        .map_err(|error| anyhow!("invalid UTF-8 in bytes {start}-{end}: {error}"))
    }

    fn required_str(line: &str, start: usize, end: usize) -> Result<String> {
        let value = Self::field(line, start, end)?.trim().to_string();
        if value.is_empty() {
            return Err(anyhow!("bytes {start}-{end} are required"));
        }
        Ok(value)
    }

    fn optional_raw_field(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        let value = Self::field(line, start, end)?.trim().to_string();
        if value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    fn optional_str(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        Self::optional_raw_field(line, start, end)
    }

    fn optional_station(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        let raw = Self::optional_raw_field(line, start, end)?;
        Ok(Self::normalize_station(raw.as_deref()))
    }

    fn required_u32(line: &str, start: usize, end: usize) -> Result<u32> {
        let value = Self::required_str(line, start, end)?;
        value
            .parse()
            .map_err(|_| anyhow!("bytes {start}-{end} must contain a numeric value"))
    }

    fn optional_hhmm(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        let value = Self::optional_str(line, start, end)?;
        if let Some(value) = &value {
            NaiveTime::parse_from_str(value, "%H%M")
                .map_err(|_| anyhow!("bytes {start}-{end} must contain a valid HHMM value"))?;
        }
        Ok(value)
    }

    fn optional_date(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        let value = Self::optional_str(line, start, end)?;
        if let Some(value) = &value {
            Self::parse_ssim_date(value)
                .map_err(|_| anyhow!("bytes {start}-{end} must contain a DDMMMYY date"))?;
        }
        Ok(value)
    }

    fn optional_numeric(line: &str, start: usize, end: usize) -> Result<Option<String>> {
        let value = Self::optional_str(line, start, end)?;
        if let Some(value) = &value {
            if !value.chars().all(|character| character.is_ascii_digit()) {
                return Err(anyhow!("bytes {start}-{end} must contain only digits"));
            }
        }
        Ok(value)
    }

    fn optional_flag(line: &str, position: usize, field_name: &str) -> Result<bool> {
        match Self::field(line, position, position)?.trim() {
            "" => Ok(false),
            "Y" => Ok(true),
            other => Err(anyhow!("{field_name} must be Y or blank, got {other}")),
        }
    }

    fn optional_flag_with_n(line: &str, position: usize, field_name: &str) -> Result<bool> {
        match Self::field(line, position, position)?.trim() {
            "" | "N" => Ok(false),
            "Y" => Ok(true),
            other => Err(anyhow!("{field_name} must be Y, N, or blank, got {other}")),
        }
    }

    fn optional_action(line: &str, start: usize, end: usize) -> Result<Option<MctActionIndicator>> {
        let value = Self::optional_str(line, start, end)?;
        match value {
            Some(value) => Ok(Some(MctActionIndicator::from_code(&value)?)),
            None => Ok(None),
        }
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
}

#[cfg(test)]
mod tests {
    use super::MctParser;
    use crate::domain::mct::MctContentIndicator;
    use std::io::Cursor;

    #[test]
    fn parses_standalone_global_default_record() {
        let data = format!(
            "{}\n{}\n{}\n",
            build_header("F", "000001"),
            build_record2(&[
                (2, 4, "***"),
                (5, 8, "0030"),
                (9, 10, "DD"),
                (195, 200, "000002")
            ]),
            build_trailer('4', "000002")
        );

        let parsed = MctParser::parse_reader(Cursor::new(data)).expect("file should parse");
        assert_eq!(parsed.header.content_indicator, MctContentIndicator::Full);
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.records[0].data.arrival_station, None);
        assert_eq!(parsed.records[0].data.departure_station, None);
        assert!(!parsed.records[0].data.requires_connection_building_filter);
        assert!(parsed.connection_building_filters.is_empty());
    }

    #[test]
    fn parses_connection_building_filter_block() {
        let data = format!(
            "{}\n{}\n{}\n{}\n",
            build_header("F", "000001"),
            build_record2(&[
                (2, 4, "***"),
                (5, 8, "0400"),
                (9, 10, "DD"),
                (11, 13, "***"),
                (195, 200, "000002"),
            ]),
            build_record3("AA", &["3M", "4B", "UA"], "000003"),
            build_trailer('4', "000003")
        );

        let parsed = MctParser::parse_reader(Cursor::new(data)).expect("file should parse");
        assert_eq!(parsed.records.len(), 1);
        assert!(parsed.records[0].data.requires_connection_building_filter);
        assert_eq!(parsed.connection_building_filters.len(), 1);
        assert_eq!(
            parsed.connection_building_filters[0]
                .data
                .partner_carrier_codes,
            vec!["3M".to_string(), "4B".to_string(), "UA".to_string()]
        );
    }

    #[test]
    fn rejects_filter_marked_record_without_type3_followup() {
        let data = format!(
            "{}\n{}\n{}\n",
            build_header("F", "000001"),
            build_record2(&[
                (2, 4, "***"),
                (5, 8, "0400"),
                (9, 10, "II"),
                (11, 13, "***"),
                (195, 200, "000002"),
            ]),
            build_trailer('4', "000002")
        );

        let error = MctParser::parse_reader(Cursor::new(data)).expect_err("file should fail");
        assert!(error.to_string().contains(
            "records marked with the connection-building filter require at least one type 3 record"
        ));
    }

    #[test]
    fn still_parses_older_type3_trailer_layout() {
        let data = format!(
            "{}\n{}\n{}\n",
            build_header("F", "000001"),
            build_record2(&[
                (2, 4, "LHR"),
                (5, 8, "0130"),
                (9, 10, "II"),
                (11, 13, "LHR"),
                (195, 200, "000002"),
            ]),
            build_trailer('3', "000002")
        );

        let parsed = MctParser::parse_reader(Cursor::new(data)).expect("file should parse");
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.connection_building_filters.len(), 0);
    }

    fn build_header(content_indicator: &str, serial: &str) -> String {
        fixed_width_line(
            '1',
            &[
                (2, 31, "MINIMUM CONNECT TIME DATA SET"),
                (32, 66, "TEST REFERENCE"),
                (67, 73, "13APR26"),
                (74, 77, "1200"),
                (78, 78, content_indicator),
                (195, 200, serial),
            ],
        )
    }

    fn build_record2(fields: &[(usize, usize, &str)]) -> String {
        fixed_width_line('2', fields)
    }

    fn build_record3(submitting_carrier: &str, partners: &[&str], serial: &str) -> String {
        let partner_text = partners.join("");
        fixed_width_line(
            '3',
            &[
                (2, 3, submitting_carrier),
                (5, 194, &partner_text),
                (195, 200, serial),
            ],
        )
    }

    fn build_trailer(record_type: char, check_reference: &str) -> String {
        fixed_width_line(record_type, &[(194, 194, "E"), (195, 200, check_reference)])
    }

    fn fixed_width_line(record_type: char, fields: &[(usize, usize, &str)]) -> String {
        let mut bytes = vec![b' '; 200];
        bytes[0] = record_type as u8;

        for (start, end, value) in fields {
            let width = end - start + 1;
            let raw = value.as_bytes();
            assert!(raw.len() <= width, "field value exceeds allotted width");
            bytes[start - 1..start - 1 + raw.len()].copy_from_slice(raw);
        }

        String::from_utf8(bytes).expect("ASCII fixed-width line")
    }
}
