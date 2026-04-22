use crate::domain::flightplan;
use crate::domain::route::Route;
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
    route_rows: Vec<Route>,
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
        "Staged schedule import ready for tmp tables: {} OAG flights, {} physical legs, {} plan variants, {} unique flight rows, {} duplicate flight rows skipped, {} route rows.",
        staged.flight_count,
        staged.segment_count,
        staged.plan_count,
        staged.flight_rows.len(),
        staged.duplicate_flight_rows_skipped,
        staged.route_rows.len()
    );

    let db_started = Instant::now();
    if let Err(error) =
        flight_repo::load_schedule_tmp(data.database(), &staged.flight_rows, &staged.route_rows)
            .await
    {
        return Ok(HttpResponse::InternalServerError()
            .body(format!("Tmp schedule load failed: {}", error)));
    }
    let db_duration = db_started.elapsed();

    let promote_started = Instant::now();
    if let Err(error) = flight_repo::promote_tmp_to_production(data.database()).await {
        return Ok(HttpResponse::InternalServerError().body(format!(
            "Failed to promote tmp schedule into production tables: {}",
            error
        )));
    }
    let promote_duration = promote_started.elapsed();

    let cache_started = Instant::now();
    let cache_summary = data.replace_flights(staged.flight_rows);
    let cache_duration = cache_started.elapsed();

    println!(
        "Total: {} OAG flights with {} physical legs and {} imported plan variants into production tables.",
        staged.flight_count,
        staged.segment_count,
        staged.plan_count
    );
    println!(
        "Skipped {} duplicate flight rows while staging.",
        staged.duplicate_flight_rows_skipped
    );
    println!("⏱️ Stage schedule import: {:?}", stage_duration);
    println!("⏱️ Build flightplan: {:?}", staged.build_duration);
    println!("⏱️ DB tmp load: {:?}", db_duration);
    println!("⏱️ Production promotion: {:?}", promote_duration);
    println!("⏱️ Memory cache replace: {:?}", cache_duration);
    println!(
        "Replaced in-memory schedule snapshot: {} active flights, {} duplicate keys inside snapshot, {} skipped (missing airports).",
        cache_summary.active_flights,
        cache_summary.duplicate_keys_within_snapshot,
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
                    plans.iter().flat_map(|plan| {
                        flightplan::expand_for_table(plan, flight_repo::temp_flight_table())
                    }),
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
        route_rows: finalize_route_rows(route_accumulators, flight_repo::temp_route_table()),
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

fn finalize_route_rows(
    accumulators: HashMap<(String, String), RouteAccumulator>,
    route_table: &str,
) -> Vec<Route> {
    let mut route_rows = accumulators
        .into_iter()
        .map(|((origin, destination), accumulator)| {
            let route_id = format!("{}_{}", origin, destination);
            Route::new(
                surrealdb_types::RecordId::new("airport", origin.as_str()),
                surrealdb_types::RecordId::new("airport", destination.as_str()),
                surrealdb_types::RecordId::new(route_table, route_id.as_str()),
                accumulator.flights.into_iter().collect(),
                accumulator.companies.into_iter().collect(),
            )
        })
        .collect::<Vec<_>>();

    route_rows.sort_by(|left, right| left.id.cmp(&right.id));

    route_rows
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
        let route_rows = finalize_route_rows(accumulators, "route_tmp");

        assert_eq!(route_rows.len(), 1);
        assert_eq!(
            route_rows[0].flights,
            vec!["CA_897".to_string(), "UA_551".to_string()]
        );
        assert_eq!(
            route_rows[0].companies,
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

    fn sample_plan(
        company: &str,
        flight_no: &str,
        origin: &str,
        destination: &str,
    ) -> crate::domain::flightplan::FlightPlan {
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
            operating_designator:
                crate::Infrastructure::db::model::flight_row::FlightDesignatorRow {
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
