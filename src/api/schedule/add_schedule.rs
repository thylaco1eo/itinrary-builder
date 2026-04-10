use crate::domain::flightplan;
use crate::memory::core::WebData;
use crate::Infrastructure::db::repository::flight_repo;
use crate::Infrastructure::file_loader::ssim_loader::{OagStreamIterator, ParseItem};
use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::{put, web, HttpResponse};
use std::time::Instant;

#[derive(Debug, MultipartForm)]
struct UploadForm {
    #[multipart(rename = "file")]
    file: TempFile,
}

#[put("/schedule")]
pub async fn add_schedule(
    data: web::Data<WebData>,
    MultipartForm(form): MultipartForm<UploadForm>,
) -> Result<HttpResponse, actix_web::Error> {
    let file = form.file.file.into_file();
    let iterator = OagStreamIterator::new(file);

    let mut flight_count = 0;
    let mut segment_count = 0;
    let mut plan_count = 0;
    let mut db_error_count = 0usize;
    let mut first_db_error: Option<String> = None;

    let mut build_duration = std::time::Duration::default();
    let mut add_flight_duration = std::time::Duration::default();
    let mut add_route_duration = std::time::Duration::default();

    for item in iterator {
        match item {
            ParseItem::Flight(block) => {
                flight_count += 1;
                segment_count += block.legs.len();

                let start_build = Instant::now();
                let plans = match flightplan::plans_from_leg_blocks(&block.legs) {
                    Ok(plans) => plans,
                    Err(e) => {
                        eprintln!("❌ Error building flight plans: {}", e);
                        continue;
                    }
                };
                plan_count += plans.len();
                let expanded_flights = plans
                    .iter()
                    .flat_map(flightplan::expand)
                    .collect::<Vec<_>>();
                build_duration += start_build.elapsed();

                let start_add_flight = Instant::now();
                match flight_repo::add_flights_batch(data.database(), &expanded_flights).await {
                    Ok(_) => {
                        let cache_summary = data.upsert_flights(expanded_flights);
                        println!(
                            "Updated in-memory flights: {} inserted, {} overwritten, {} skipped (missing airports).",
                            cache_summary.upserted,
                            cache_summary.overwritten,
                            cache_summary.skipped_missing_airports
                        );
                    }
                    Err(e) => {
                        let msg = format!("❌ Error adding flights to DB: {}", e);
                        eprintln!("{}", msg);
                        if first_db_error.is_none() {
                            first_db_error = Some(msg);
                        }
                        db_error_count += 1;
                    }
                }
                add_flight_duration += start_add_flight.elapsed();

                let start_add_route = Instant::now();
                for plan in &plans {
                    match flight_repo::add_route(data.database(), plan).await {
                        Ok(_) => (),
                        Err(e) => {
                            eprintln!("❌ Error adding route to DB: {}", e);
                        }
                    }
                }
                add_route_duration += start_add_route.elapsed();

                if flight_count % 10000 == 0 {
                    println!("Processed {} flights...", flight_count);
                }
            }
            ParseItem::Trailer(t) => {
                println!(
                    "✅ Finished block with serial check: {}",
                    t.check_serial_number
                );
            }
            ParseItem::Error(e) => {
                eprintln!("❌ Error parsing file: {}", e);
                // 根据需求决定是 break 还是 continue
            }
            _ => {} // 忽略 Header/Season
        }
    }

    println!(
        "Total: {} OAG flights with {} physical legs and {} imported plan variants.",
        flight_count, segment_count, plan_count
    );
    println!("⏱️ Build flightplan: {:?}", build_duration);
    println!("⏱️ DB add_flight: {:?}", add_flight_duration);
    println!("⏱️ DB add_route: {:?}", add_route_duration);

    if db_error_count > 0 {
        let detail = first_db_error.unwrap_or_else(|| "Unknown DB error".to_string());
        return Ok(HttpResponse::InternalServerError().body(format!(
            "DB errors: {}. Example: {}",
            db_error_count, detail
        )));
    }
    Ok(HttpResponse::Ok().finish())
}
