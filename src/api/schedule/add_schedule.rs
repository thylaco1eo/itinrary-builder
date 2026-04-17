use crate::domain::flightplan;
use crate::memory::core::WebData;
use crate::Infrastructure::db::repository::flight_repo;
use crate::Infrastructure::file_loader::ssim_loader::{OagStreamIterator, ParseItem};
use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::{put, web, HttpResponse};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::time::Instant;

#[derive(Debug, MultipartForm)]
struct UploadForm {
    #[multipart(rename = "file")]
    file: TempFile,
}

#[derive(Debug)]
struct StagedScheduleImport {
    flight_count: usize,
    segment_count: usize,
    plan_count: usize,
    duplicate_flight_rows_skipped: usize,
    build_duration: std::time::Duration,
    flight_rows: Vec<crate::Infrastructure::db::model::flight_row::FlightRow>,
    route_updates: Vec<flight_repo::RouteUpsert>,
}

#[derive(Debug, Default)]
struct RouteAccumulator {
    flights: BTreeSet<String>,
    companies: BTreeSet<String>,
}

#[put("/schedule")]
pub async fn add_schedule(
    data: web::Data<WebData>,
    MultipartForm(form): MultipartForm<UploadForm>,
) -> Result<HttpResponse, actix_web::Error> {
    let file = form.file.file.into_file();
    let stage_started = Instant::now();
    let staged = match stage_schedule_import(file) {
        Ok(staged) => staged,
        Err(error) => return Ok(HttpResponse::BadRequest().body(error)),
    };
    let stage_duration = stage_started.elapsed();
    println!(
        "Staged schedule import ready: {} OAG flights, {} physical legs, {} plan variants, {} unique flight rows, {} duplicate flight rows skipped, {} route updates.",
        staged.flight_count,
        staged.segment_count,
        staged.plan_count,
        staged.flight_rows.len(),
        staged.duplicate_flight_rows_skipped,
        staged.route_updates.len()
    );

    let db_started = Instant::now();
    if let Err(error) = flight_repo::import_schedule_atomically(
        data.database(),
        &staged.flight_rows,
        &staged.route_updates,
    )
    .await
    {
        return Ok(HttpResponse::InternalServerError().body(format!(
            "Atomic schedule import failed: {}",
            error
        )));
    }
    let db_duration = db_started.elapsed();

    let cache_started = Instant::now();
    let cache_summary = data.upsert_flights(staged.flight_rows);
    let cache_duration = cache_started.elapsed();

    println!(
        "Total: {} OAG flights with {} physical legs and {} imported plan variants.",
        staged.flight_count, staged.segment_count, staged.plan_count
    );
    println!(
        "Skipped {} duplicate flight rows while staging.",
        staged.duplicate_flight_rows_skipped
    );
    println!("⏱️ Stage schedule import: {:?}", stage_duration);
    println!("⏱️ Build flightplan: {:?}", staged.build_duration);
    println!("⏱️ DB atomic import: {:?}", db_duration);
    println!("⏱️ Memory cache refresh: {:?}", cache_duration);
    println!(
        "Updated in-memory flights atomically: {} inserted, {} overwritten, {} skipped (missing airports).",
        cache_summary.upserted,
        cache_summary.overwritten,
        cache_summary.skipped_missing_airports
    );

    Ok(HttpResponse::Ok().finish())
}

fn stage_schedule_import(file: File) -> Result<StagedScheduleImport, String> {
    let iterator = OagStreamIterator::new(file);

    let mut flight_count = 0usize;
    let mut segment_count = 0usize;
    let mut plan_count = 0usize;
    let mut duplicate_flight_rows_skipped = 0usize;
    let mut build_duration = std::time::Duration::default();
    let mut flight_rows = Vec::new();
    let mut seen_flight_row_ids = HashSet::new();
    let mut route_accumulators: HashMap<(String, String), RouteAccumulator> = HashMap::new();

    for item in iterator {
        match item {
            ParseItem::Flight(block) => {
                flight_count += 1;
                segment_count += block.legs.len();

                let start_build = Instant::now();
                let plans = flightplan::plans_from_leg_blocks(&block.legs)
                    .map_err(|error| format!("Error building flight plans: {}", error))?;
                plan_count += plans.len();
                append_unique_flight_rows(
                    &mut flight_rows,
                    &mut seen_flight_row_ids,
                    &mut duplicate_flight_rows_skipped,
                    plans.iter().flat_map(flightplan::expand),
                );
                accumulate_route_updates(&mut route_accumulators, &plans);
                build_duration += start_build.elapsed();

                if flight_count % 10000 == 0 {
                    println!("Staged {} flights...", flight_count);
                }
            }
            ParseItem::Trailer(t) => {
                println!(
                    "✅ Finished block with serial check: {}",
                    t.check_serial_number
                );
            }
            ParseItem::Error(error) => {
                return Err(format!("Error parsing file: {}", error));
            }
            _ => {}
        }
    }

    Ok(StagedScheduleImport {
        flight_count,
        segment_count,
        plan_count,
        duplicate_flight_rows_skipped,
        build_duration,
        flight_rows,
        route_updates: finalize_route_updates(route_accumulators),
    })
}

fn append_unique_flight_rows(
    staged_rows: &mut Vec<crate::Infrastructure::db::model::flight_row::FlightRow>,
    seen_ids: &mut HashSet<surrealdb_types::RecordId>,
    duplicate_count: &mut usize,
    rows: impl IntoIterator<Item = crate::Infrastructure::db::model::flight_row::FlightRow>,
) {
    for row in rows {
        if seen_ids.insert(row.id.clone()) {
            staged_rows.push(row);
        } else {
            *duplicate_count += 1;
        }
    }
}

fn accumulate_route_updates(
    accumulators: &mut HashMap<(String, String), RouteAccumulator>,
    plans: &[crate::domain::flightplan::FlightPlan],
) {
    for plan in plans {
        let key = (
            plan.origin.as_str().to_string(),
            plan.destination.as_str().to_string(),
        );
        let accumulator = accumulators.entry(key).or_default();
        accumulator
            .flights
            .insert(format!("{}_{}", plan.company, plan.flight_no));
        accumulator.companies.insert(plan.company.clone());
    }
}

fn finalize_route_updates(
    accumulators: HashMap<(String, String), RouteAccumulator>,
) -> Vec<flight_repo::RouteUpsert> {
    let mut route_updates = accumulators
        .into_iter()
        .map(|((origin, destination), accumulator)| flight_repo::RouteUpsert {
            origin,
            destination,
            flights: accumulator.flights.into_iter().collect(),
            companies: accumulator.companies.into_iter().collect(),
        })
        .collect::<Vec<_>>();

    route_updates.sort_by(|left, right| {
        left.origin
            .cmp(&right.origin)
            .then_with(|| left.destination.cmp(&right.destination))
    });

    route_updates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::airport::AirportCode;
    use chrono::{Duration, NaiveDate, NaiveTime};

    #[test]
    fn route_updates_are_deduplicated_per_airport_pair() {
        let plans = vec![
            sample_plan("CA", "897", "PEK", "GRU"),
            sample_plan("CA", "897", "PEK", "GRU"),
            sample_plan("UA", "551", "PEK", "GRU"),
        ];
        let mut accumulators = HashMap::new();

        accumulate_route_updates(&mut accumulators, &plans);
        let route_updates = finalize_route_updates(accumulators);

        assert_eq!(route_updates.len(), 1);
        assert_eq!(route_updates[0].origin, "PEK");
        assert_eq!(route_updates[0].destination, "GRU");
        assert_eq!(
            route_updates[0].flights,
            vec!["CA_897".to_string(), "UA_551".to_string()]
        );
        assert_eq!(
            route_updates[0].companies,
            vec!["CA".to_string(), "UA".to_string()]
        );
    }

    #[test]
    fn duplicate_flight_rows_are_skipped_before_db_import() {
        let plan = sample_plan("CA", "897", "PEK", "GRU");
        let first = crate::Infrastructure::db::model::flight_row::FlightRow::from_plan(
            &plan,
            NaiveDate::from_ymd_opt(2026, 4, 16).unwrap(),
        );
        let duplicate = crate::Infrastructure::db::model::flight_row::FlightRow::from_plan(
            &plan,
            NaiveDate::from_ymd_opt(2026, 4, 16).unwrap(),
        );
        let mut staged_rows = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut duplicate_count = 0usize;

        append_unique_flight_rows(
            &mut staged_rows,
            &mut seen_ids,
            &mut duplicate_count,
            vec![first, duplicate],
        );

        assert_eq!(staged_rows.len(), 1);
        assert_eq!(duplicate_count, 1);
    }

    fn sample_plan(company: &str, flight_no: &str, origin: &str, destination: &str) -> crate::domain::flightplan::FlightPlan {
        crate::domain::flightplan::FlightPlan {
            company: company.to_string(),
            flight_no: flight_no.to_string(),
            origin: AirportCode::new(origin).unwrap(),
            destination: AirportCode::new(destination).unwrap(),
            dep_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            arr_time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            block_time: Duration::hours(2),
            start_date: NaiveDate::from_ymd_opt(2026, 4, 16).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 4, 16).unwrap(),
            weekdays: [false, false, false, false, true, false, false],
            frequency_rate: None,
            dep_tz: "+0800".to_string(),
            arr_tz: "+0800".to_string(),
            arrival_day_offset: 0,
            operating_designator: crate::Infrastructure::db::model::flight_row::FlightDesignatorRow {
                company: company.to_string(),
                flight_number: flight_no.to_string(),
                operational_suffix: None,
            },
            duplicate_designators: vec![],
            joint_operation_airline_designators: vec![],
            meal_service_note: None,
            in_flight_service_info: None,
            electronic_ticketing_info: None,
            type3_legs: vec![],
        }
    }
}
