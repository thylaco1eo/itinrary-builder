use anyhow::Context;
use crate::domain::airport::Airport;
use crate::domain::mct::{
    AirportMctData, AirportMctRecord, ConnectionBuildingFilter, MctActionIndicator,
    MctContentIndicator, ParsedMctFile, GlobalMctData, is_global_mct_record,
};
use crate::memory::core::WebData;
use crate::Infrastructure::db;
use crate::Infrastructure::db::model::airport_row::AirportRow;
use crate::Infrastructure::db::model::airport_row::AirportRowError;
use crate::Infrastructure::file_loader::mct_parser::MctParser;
use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::{get, post, put, web, HttpResponse};
use serde::Serialize;
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

#[derive(Debug, MultipartForm)]
struct UploadForm {
    #[multipart(rename = "file")]
    file: TempFile,
}

#[derive(Debug, Serialize)]
struct AirportMctResponse {
    airport: String,
    record_count: usize,
    mct_records: Vec<AirportMctRecord>,
    connection_building_filter_count: usize,
    connection_building_filters: Vec<ConnectionBuildingFilter>,
}

#[derive(Debug, Serialize)]
struct BulkMctApplySummary {
    content_indicator: String,
    file_record_count: usize,
    airports_updated: usize,
    added_records: usize,
    replaced_records: usize,
    deleted_records: usize,
    global_records_applied: usize,
    filter_airports_updated: usize,
    connection_building_filter_count: usize,
    missing_airports: Vec<String>,
}

enum UpsertRecordResult {
    Added,
    Replaced,
    Unchanged,
}

#[put("/airport")]
pub async fn add_airport(
    data: web::Data<WebData>,
    form: web::Form<AirportRow>,
) -> Result<HttpResponse, actix_web::Error> {
    let row = form.into_inner();
    if let Err(e) = Airport::try_from(row.clone()) {
        return match e {
            AirportRowError::InvalidCode(_) => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid airport code"})))
            }
            AirportRowError::InvalidTimezone(_) => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid timezone"})))
            }
            AirportRowError::InvalidLatitude => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid latitude"})))
            }
            AirportRowError::InvalidLongitude => {
                Ok(HttpResponse::BadRequest().json(json!({"status": "invalid longitude"})))
            }
        };
    }

    match db::repository::airport_repo::add_airport(data.database(), row.clone()).await {
        Ok(true) => {
            data.upsert_airport(row)
                .map_err(actix_web::error::ErrorInternalServerError)?;
            Ok(HttpResponse::Created().json(json!({"status": "ok"})))
        }
        Ok(false) => Ok(HttpResponse::Conflict().json(json!({"status": "conflict"}))),
        Err(e) => {
            log::error!("Error adding airport: {}", e);
            Ok(HttpResponse::InternalServerError()
                .json(json!({"status": "error", "message": e.to_string()})))
        }
    }
}

#[post("/airport/mct")]
pub async fn upload_airport_mct(
    data: web::Data<WebData>,
    MultipartForm(form): MultipartForm<UploadForm>,
) -> Result<HttpResponse, actix_web::Error> {
    let file = form.file.file.into_file();
    let parsed = match MctParser::parse_reader(file) {
        Ok(parsed) => parsed,
        Err(error) => {
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid_mct_file",
                "message": error.to_string()
            })));
        }
    };

    match apply_mct_file(data.database(), parsed).await {
        Ok((summary, _updated_airports)) => {
            data.reload_airport_mct().await;
            data.reload_global_mct().await;
            Ok(HttpResponse::Ok().json(json!({
                "status": "ok",
                "summary": summary
            })))
        }
        Err(error) => {
            log::error!("Error applying MCT file: {}", error);
            Ok(HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": error.to_string()
            })))
        }
    }
}

#[put("/airport/{code}/mct")]
pub async fn put_airport_mct(
    data: web::Data<WebData>,
    path: web::Path<String>,
    body: web::Json<AirportMctRecord>,
) -> Result<HttpResponse, actix_web::Error> {
    let airport_code = path.into_inner().trim().to_uppercase();
    let Some(_airport) = db::repository::airport_repo::get_airport(data.database(), &airport_code)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?
    else {
        return Ok(HttpResponse::NotFound().json(json!({
            "status": "not_found",
            "message": format!("airport {} not found", airport_code)
        })));
    };

    let record = match normalize_airport_mct_record(body.into_inner(), &airport_code) {
        Ok(record) => record,
        Err(error) => {
            return Ok(HttpResponse::BadRequest().json(json!({
                "status": "invalid_mct_record",
                "message": error.to_string()
            })));
        }
    };

    let current_airport_mct = db::repository::mct_repo::get_airport_mct(data.database(), &airport_code)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?
        .unwrap_or_default();
    let mut mct_records = current_airport_mct.mct_records;
    let upsert_result = upsert_airport_record(&mut mct_records, record);
    let airport_mct = AirportMctData {
        mct_records: mct_records.clone(),
        connection_building_filters: Vec::new(),
    };

    db::repository::mct_repo::set_airport_mct(
        data.database(),
        &airport_code,
        &airport_mct,
    )
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;
    db::repository::airport_repo::clear_legacy_airport_mct_fields_for_airport(
        data.database(),
        &airport_code,
    )
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;
    data.set_airport_mct(airport_code.clone(), airport_mct);
    let global_filters = data.global_mct().connection_building_filters.clone();

    Ok(HttpResponse::Ok().json(json!({
        "status": "ok",
        "airport": airport_code,
        "record_count": mct_records.len(),
        "result": match upsert_result {
            UpsertRecordResult::Added => "added",
            UpsertRecordResult::Replaced => "replaced",
            UpsertRecordResult::Unchanged => "unchanged"
        },
        "mct_records": mct_records,
        "connection_building_filters": global_filters
    })))
}

#[get("/airport/{code}/mct")]
pub async fn get_airport_mct(
    data: web::Data<WebData>,
    path: web::Path<String>,
) -> Result<HttpResponse, actix_web::Error> {
    let airport_code = path.into_inner().trim().to_uppercase();
    let airport = db::repository::airport_repo::get_airport(data.database(), &airport_code)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let airport_mct = db::repository::mct_repo::get_airport_mct(data.database(), &airport_code)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?
        .unwrap_or_default();
    let global_filters = data.global_mct().connection_building_filters.clone();

    match airport {
        Some(_) => Ok(HttpResponse::Ok().json(AirportMctResponse {
            airport: airport_code,
            record_count: airport_mct.mct_records.len(),
            mct_records: airport_mct.mct_records,
            connection_building_filter_count: global_filters.len(),
            connection_building_filters: global_filters,
        })),
        None => Ok(HttpResponse::NotFound().json(json!({
            "status": "not_found",
            "message": format!("airport {} not found", airport_code)
        }))),
    }
}

async fn apply_mct_file(
    db: &Surreal<Any>,
    parsed: ParsedMctFile,
) -> anyhow::Result<(BulkMctApplySummary, Vec<String>)> {
    let content_indicator = parsed.header.content_indicator.clone();
    let file_record_count = parsed.records.len();
    log::info!(
        "Applying MCT file: content_indicator={}, records={}, connection_building_filters={}",
        content_indicator.as_code(),
        file_record_count,
        parsed.connection_building_filters.len()
    );
    let existing_airports: HashSet<String> =
        db::repository::airport_repo::get_all_airport_codes(db)
            .await
            .context("failed to load airport codes before applying MCT file")?
            .into_iter()
            .collect();
    let mut missing_airports = BTreeSet::new();
    let mut adds_by_airport: HashMap<String, Vec<AirportMctRecord>> = HashMap::new();
    let mut deletes_by_airport: HashMap<String, Vec<AirportMctRecord>> = HashMap::new();
    let mut global_records_applied = 0usize;
    let mut global_add_records = Vec::new();
    let mut global_delete_records = Vec::new();
    let merged_filters = merge_connection_building_filters(&parsed);

    for record in parsed.records {
        if is_global_mct_record(&record.data) {
            global_records_applied += 1;
            match &content_indicator {
                MctContentIndicator::Full => {
                    let _ = upsert_airport_record(&mut global_add_records, record.data.clone());
                }
                MctContentIndicator::UpdatesOnly => match record.action_indicator {
                    Some(MctActionIndicator::Add) => global_add_records.push(record.data.clone()),
                    Some(MctActionIndicator::Delete) => {
                        global_delete_records.push(record.data.clone())
                    }
                    None => {}
                },
            }
            continue;
        }

        let (targets, missing_targets, is_global) =
            resolve_target_airports(&record.data, &existing_airports);
        missing_airports.extend(missing_targets);
        if is_global {
            global_records_applied += 1;
        }

        match &content_indicator {
            MctContentIndicator::Full => {
                for airport_code in targets {
                    let airport_records = adds_by_airport.entry(airport_code).or_default();
                    let _ = upsert_airport_record(airport_records, record.data.clone());
                }
            }
            MctContentIndicator::UpdatesOnly => match record.action_indicator {
                Some(MctActionIndicator::Add) => {
                    for airport_code in targets {
                        adds_by_airport
                            .entry(airport_code)
                            .or_default()
                            .push(record.data.clone());
                    }
                }
                Some(MctActionIndicator::Delete) => {
                    for airport_code in targets {
                        deletes_by_airport
                            .entry(airport_code)
                            .or_default()
                            .push(record.data.clone());
                    }
                }
                None => {}
            },
        }
    }

    if matches!(&content_indicator, MctContentIndicator::Full) {
        log::info!(
            "Applying full MCT import across {} known airports; {} airports have explicit records, {} global records, and {} merged connection-building filters.",
            existing_airports.len(),
            adds_by_airport.len(),
            global_add_records.len(),
            merged_filters.len()
        );
        db::repository::airport_repo::clear_legacy_airport_mct_fields(db)
            .await
            .context("failed to clear legacy airport-side MCT fields before full import")?;
        db::repository::mct_repo::clear_all_airport_mct(db)
            .await
            .context("failed to clear airport MCT payloads before full import")?;
        db::repository::mct_repo::clear_global_mct(db)
            .await
            .context("failed to clear global MCT payload before full import")?;

        let mut airports_updated = 0usize;
        let mut added_records = 0usize;
        let filter_airports_updated = 0usize;
        let mut updated_airports = adds_by_airport.keys().cloned().collect::<Vec<_>>();
        updated_airports.sort();
        for (airport_code, airport_records) in adds_by_airport {
            added_records += airport_records.len();
            let record_count = airport_records.len();
            let airport_mct = AirportMctData {
                mct_records: airport_records,
                connection_building_filters: Vec::new(),
            };
            db::repository::mct_repo::set_airport_mct(db, &airport_code, &airport_mct)
                .await
                .with_context(|| {
                    format!(
                        "failed to set full-import MCT payload for airport {} (records={})",
                        airport_code, record_count
                    )
                })?;
            airports_updated += 1;
        }

        let global_mct = GlobalMctData {
            mct_records: global_add_records.clone(),
            connection_building_filters: merged_filters.clone(),
        };
        db::repository::mct_repo::set_global_mct(db, &global_mct)
            .await
            .with_context(|| {
                format!(
                    "failed to persist global MCT payload (records={}, filters={})",
                    global_mct.mct_records.len(),
                    global_mct.connection_building_filters.len()
                )
            })?;

        return Ok((
            BulkMctApplySummary {
                content_indicator: content_indicator.as_code().to_string(),
                file_record_count,
                airports_updated,
                added_records,
                replaced_records: 0,
                deleted_records: 0,
                global_records_applied,
                filter_airports_updated,
                connection_building_filter_count: merged_filters.len(),
                missing_airports: missing_airports.into_iter().collect(),
            },
            updated_airports,
        ));
    }

    let affected_airports: BTreeSet<String> = adds_by_airport
        .keys()
        .chain(deletes_by_airport.keys())
        .cloned()
        .collect();
    let mut airports_updated = 0usize;
    let mut added_records = 0usize;
    let mut replaced_records = 0usize;
    let mut deleted_records = 0usize;
    let filter_airports_updated = 0usize;
    let mut updated_airports = Vec::new();

    let current_global_mct = db::repository::mct_repo::get_global_mct(db)
        .await
        .context("failed to load global MCT payload before updates-only apply")?;
    let mut current_global_records = current_global_mct.mct_records.clone();

    for record in &global_delete_records {
        deleted_records += remove_airport_record(&mut current_global_records, record);
    }

    for record in &global_add_records {
        match upsert_airport_record(&mut current_global_records, record.clone()) {
            UpsertRecordResult::Added => added_records += 1,
            UpsertRecordResult::Replaced => replaced_records += 1,
            UpsertRecordResult::Unchanged => {}
        }
    }

    for airport_code in affected_airports {
        let mut current_records = db::repository::mct_repo::get_airport_mct(db, &airport_code)
            .await
            .with_context(|| {
                format!(
                    "failed to load airport MCT payload for {} before updates-only apply",
                    airport_code
                )
            })?
            .unwrap_or_default()
            .mct_records;

        if let Some(records_to_delete) = deletes_by_airport.get(&airport_code) {
            for record in records_to_delete {
                deleted_records += remove_airport_record(&mut current_records, record);
            }
        }

        if let Some(records_to_add) = adds_by_airport.get(&airport_code) {
            for record in records_to_add {
                match upsert_airport_record(&mut current_records, record.clone()) {
                    UpsertRecordResult::Added => added_records += 1,
                    UpsertRecordResult::Replaced => replaced_records += 1,
                    UpsertRecordResult::Unchanged => {}
                }
            }
        }

        let record_count = current_records.len();
        let airport_mct = AirportMctData {
            mct_records: current_records,
            connection_building_filters: Vec::new(),
        };
        db::repository::mct_repo::set_airport_mct(db, &airport_code, &airport_mct)
            .await
            .with_context(|| {
                format!(
                    "failed to set updates-only MCT payload for airport {} (records={})",
                    airport_code, record_count
                )
            })?;
        airports_updated += 1;
        updated_airports.push(airport_code.clone());
    }

    let global_connection_building_filters = if merged_filters.is_empty() {
        current_global_mct.connection_building_filters
    } else {
        merged_filters.clone()
    };

    if !global_add_records.is_empty()
        || !global_delete_records.is_empty()
        || !merged_filters.is_empty()
    {
        let global_mct = GlobalMctData {
            mct_records: current_global_records,
            connection_building_filters: global_connection_building_filters,
        };
        db::repository::mct_repo::set_global_mct(db, &global_mct)
            .await
            .with_context(|| {
                format!(
                    "failed to persist updates-only global MCT payload (records={}, filters={})",
                    global_mct.mct_records.len(),
                    global_mct.connection_building_filters.len()
                )
            })?;
    }

    Ok((
        BulkMctApplySummary {
            content_indicator: content_indicator.as_code().to_string(),
            file_record_count,
            airports_updated,
            added_records,
            replaced_records,
            deleted_records,
            global_records_applied,
            filter_airports_updated,
            connection_building_filter_count: merged_filters.len(),
            missing_airports: missing_airports.into_iter().collect(),
        },
        updated_airports,
    ))
}

fn normalize_airport_mct_record(
    mut record: AirportMctRecord,
    airport_code: &str,
) -> anyhow::Result<AirportMctRecord> {
    let airport_code = airport_code.trim().to_uppercase();

    record.arrival_station =
        normalize_optional(record.arrival_station).or(Some(airport_code.clone()));
    record.time = normalize_optional(record.time);
    record.status = record.status.trim().to_uppercase();
    record.departure_station = normalize_optional(record.departure_station).or(Some(airport_code));
    record.arrival_carrier = normalize_optional(record.arrival_carrier);
    record.arrival_codeshare_operating_carrier =
        normalize_optional(record.arrival_codeshare_operating_carrier);
    record.departure_carrier = normalize_optional(record.departure_carrier);
    record.departure_codeshare_operating_carrier =
        normalize_optional(record.departure_codeshare_operating_carrier);
    record.arrival_aircraft_type = normalize_optional(record.arrival_aircraft_type);
    record.arrival_aircraft_body = normalize_optional(record.arrival_aircraft_body);
    record.departure_aircraft_type = normalize_optional(record.departure_aircraft_type);
    record.departure_aircraft_body = normalize_optional(record.departure_aircraft_body);
    record.arrival_terminal = normalize_optional(record.arrival_terminal);
    record.departure_terminal = normalize_optional(record.departure_terminal);
    record.previous_country = normalize_optional(record.previous_country);
    record.previous_station = normalize_optional(record.previous_station);
    record.next_country = normalize_optional(record.next_country);
    record.next_station = normalize_optional(record.next_station);
    record.arrival_flight_number_range_start =
        normalize_optional(record.arrival_flight_number_range_start);
    record.arrival_flight_number_range_end =
        normalize_optional(record.arrival_flight_number_range_end);
    record.departure_flight_number_range_start =
        normalize_optional(record.departure_flight_number_range_start);
    record.departure_flight_number_range_end =
        normalize_optional(record.departure_flight_number_range_end);
    record.previous_state = normalize_optional(record.previous_state);
    record.next_state = normalize_optional(record.next_state);
    record.previous_region = normalize_optional(record.previous_region);
    record.next_region = normalize_optional(record.next_region);
    record.effective_from_local = normalize_optional(record.effective_from_local);
    record.effective_to_local = normalize_optional(record.effective_to_local);
    record.suppression_region = normalize_optional(record.suppression_region);
    record.suppression_country = normalize_optional(record.suppression_country);
    record.suppression_state = normalize_optional(record.suppression_state);
    record.requires_connection_building_filter = false;
    record.validate()?;

    Ok(record)
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_uppercase();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn resolve_target_airports(
    record: &AirportMctRecord,
    existing_airports: &HashSet<String>,
) -> (Vec<String>, Vec<String>, bool) {
    let mut targets = BTreeSet::new();
    let mut missing = BTreeSet::new();

    for airport_code in [&record.arrival_station, &record.departure_station] {
        if let Some(airport_code) = airport_code {
            if existing_airports.contains(airport_code) {
                targets.insert(airport_code.clone());
            } else {
                missing.insert(airport_code.clone());
            }
        }
    }

    if record.arrival_station.is_none() && record.departure_station.is_none() {
        targets.extend(existing_airports.iter().cloned());
        return (
            targets.into_iter().collect(),
            missing.into_iter().collect(),
            true,
        );
    }

    (
        targets.into_iter().collect(),
        missing.into_iter().collect(),
        false,
    )
}

fn upsert_airport_record(
    records: &mut Vec<AirportMctRecord>,
    record: AirportMctRecord,
) -> UpsertRecordResult {
    if let Some(index) = records
        .iter()
        .position(|existing| existing.same_scope_as(&record))
    {
        if records[index] == record {
            return UpsertRecordResult::Unchanged;
        }
        records[index] = record;
        return UpsertRecordResult::Replaced;
    }

    records.push(record);
    UpsertRecordResult::Added
}

fn merge_connection_building_filters(parsed: &ParsedMctFile) -> Vec<ConnectionBuildingFilter> {
    let mut merged: HashMap<String, BTreeSet<String>> = HashMap::new();

    for filter in &parsed.connection_building_filters {
        let entry = merged
            .entry(filter.data.submitting_carrier.clone())
            .or_default();
        for partner in &filter.data.partner_carrier_codes {
            entry.insert(partner.clone());
        }
    }

    let mut result = merged
        .into_iter()
        .map(
            |(submitting_carrier, partner_carrier_codes)| ConnectionBuildingFilter {
                submitting_carrier,
                partner_carrier_codes: partner_carrier_codes.into_iter().collect(),
            },
        )
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.submitting_carrier.cmp(&right.submitting_carrier));
    result
}

fn remove_airport_record(records: &mut Vec<AirportMctRecord>, record: &AirportMctRecord) -> usize {
    let original_len = records.len();
    records.retain(|existing| existing != record);
    original_len - records.len()
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_airport_mct_record, remove_airport_record, resolve_target_airports,
        upsert_airport_record, UpsertRecordResult,
    };
    use crate::domain::mct::AirportMctRecord;
    use std::collections::HashSet;

    #[test]
    fn put_style_upsert_replaces_same_scope() {
        let mut records = vec![sample_record("PEK", "0100")];
        let result = upsert_airport_record(&mut records, sample_record("PEK", "0130"));

        assert!(matches!(result, UpsertRecordResult::Replaced));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].time.as_deref(), Some("0130"));
    }

    #[test]
    fn delete_removes_exact_match() {
        let mut records = vec![sample_record("PVG", "0100"), sample_record("PVG", "0130")];
        let deleted = remove_airport_record(&mut records, &sample_record("PVG", "0100"));

        assert_eq!(deleted, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].time.as_deref(), Some("0130"));
    }

    #[test]
    fn global_suppression_targets_all_airports() {
        let mut existing = HashSet::new();
        existing.insert("PEK".to_string());
        existing.insert("SHA".to_string());

        let record = AirportMctRecord {
            arrival_station: None,
            time: None,
            status: "II".to_string(),
            departure_station: None,
            requires_connection_building_filter: false,
            arrival_carrier: Some("CA".to_string()),
            arrival_codeshare_indicator: false,
            arrival_codeshare_operating_carrier: None,
            departure_carrier: Some("CA".to_string()),
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
            suppression_indicator: true,
            suppression_region: None,
            suppression_country: None,
            suppression_state: None,
        };

        let (targets, missing, is_global) = resolve_target_airports(&record, &existing);
        assert!(is_global);
        assert!(missing.is_empty());
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn put_normalization_defaults_station_to_path_airport() {
        let normalized = normalize_airport_mct_record(sample_record("", "0100"), "hkg")
            .expect("record should normalize");

        assert_eq!(normalized.arrival_station.as_deref(), Some("HKG"));
        assert_eq!(normalized.departure_station.as_deref(), Some("HKG"));
    }

    fn sample_record(station: &str, time: &str) -> AirportMctRecord {
        AirportMctRecord {
            arrival_station: if station.is_empty() {
                None
            } else {
                Some(station.to_string())
            },
            time: Some(time.to_string()),
            status: "II".to_string(),
            departure_station: if station.is_empty() {
                None
            } else {
                Some(station.to_string())
            },
            requires_connection_building_filter: false,
            arrival_carrier: Some("CA".to_string()),
            arrival_codeshare_indicator: false,
            arrival_codeshare_operating_carrier: None,
            departure_carrier: Some("CA".to_string()),
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
        }
    }
}
