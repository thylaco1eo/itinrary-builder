//extern crate chrono;
mod flight_info;
pub mod services;
pub mod db;
pub mod structure;
pub mod other;
use other::load_configuration;
use structure::{Configuration,SSIM};
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use actix_multipart::form::MultipartForm;

#[get("/search")]
async fn search(data: web::Data<Configuration>,req_body:String) -> impl Responder {
    if let Some(flights) = data.flights().lock().ok(){
        if flights.is_empty() {
            return HttpResponse::NotFound().body("No flight data available");
        }
    } else {
        return HttpResponse::InternalServerError().body("Failed to lock flight data");
    }
    let flights = data.flights().lock().unwrap();
    let result = services::search_service::search_flight::search_flight(&flights, &req_body);
    let result_string = if result.is_empty() {
        "No flights found".to_string()
    } else {
        let mut output = String::new();
        for path in result {
            for (flt_id, dep_time, arr_station, flight_time) in path {
                output.push_str(&format!("Flight ID: {}, Departure Time: {}, Arrival Station: {}, Flight Time: {} minutes\n", flt_id, dep_time, arr_station, flight_time));
            }
            output.push_str("-------------------------\n");
        }
        output
    };
    HttpResponse::Ok().body(result_string)
}

#[post("/import_ssim")]
async fn import_ssim(data: web::Data<Configuration>, mut multipart_form: MultipartForm<SSIM>) -> impl Responder {
    {
        let mut data_new = data.flights().lock().unwrap();
        data_new.clear(); // Clear existing data before importing new one
        *data_new  = services::data_service::import_schedule_file(multipart_form.file().file.as_file_mut());
    }
    HttpResponse::Ok().body("File imported successfully")
}



#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let conf = load_configuration();
    let mut db_client = db::connect_db(conf.db_info());
    db::check_db_status(&mut db_client);
    let app_state = web::Data::new(conf);
    HttpServer::new(move || {
    App::new()
        .app_data(app_state.clone())
        .service(search)
        .service(import_ssim)
})
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}