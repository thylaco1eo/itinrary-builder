//extern crate chrono;
mod flight_info;
pub mod services;
pub mod db;
pub mod structure;
pub mod utils;

use crate::structure::FlightInfo;
use actix_multipart::form::MultipartForm;
use actix_web::middleware::Logger;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use log::{error, info};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
};
use neo4rs::{query, Graph};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::fs::File;
use std::io::prelude::*;
use structure::*;

#[get("/search")]
// async fn search(data: web::Data<WebData>,req_body:String) -> impl Responder {
//     if let Some(flights) = data.flights().lock().ok(){
//         if flights.is_empty() {
//             return HttpResponse::NotFound().body("No flight data available");
//         }
//     } else {
//         return HttpResponse::InternalServerError().body("Failed to lock flight data");
//     }
//     let flights = data.flights().lock().unwrap();
//     let result = services::search_service::search_flight::search_flight(&flights, &req_body);
//     let result_string = if result.is_empty() {
//         "No flights found".to_string()
//     } else {
//         let mut output = String::new();
//         for path in result {
//             for (flt_id, dep_time, arr_station, flight_time) in path {
//                 output.push_str(&format!("Flight ID: {}, Departure Time: {}, Arrival Station: {}, Flight Time: {} minutes\n", flt_id, dep_time, arr_station, flight_time));
//             }
//             output.push_str("-------------------------\n");
//         }
//         output
//     };
//     HttpResponse::Ok().body(result_string)
// }

async fn search(data: web::Data<WebData>, reqbody: String) -> impl Responder {
    if !utils::check_ib_reqbody(reqbody.clone()) {
        return HttpResponse::BadRequest().body("Invalid request body");
    }
    let mut result = data
        .database()
        .execute(utils::make_request(reqbody))
        .await
        .unwrap();
    let mut collect = String::new();
    while let Ok(Some(row)) = result.next().await {
        let node: neo4rs::Node = row.get("n").unwrap();
        collect.push_str(&format!(
            "Node ID: {}, Labels: {:?}\n",
            node.id(),
            node.labels()
        ));
        //println!("Node ID: {}, Labels: {:?}", node.id(), node.labels());
    }
    HttpResponse::Ok().body(collect)
}

#[post("/import_ssim")]
async fn import_ssim(data: web::Data<WebData>, mut multipart_form: MultipartForm<SSIM>) -> impl Responder {
    let flights = services::data_service::import_schedule_file(multipart_form.file().file.as_file_mut());
    //db::import_ssim(&data.database, &flights).await;
    HttpResponse::Ok().body("File imported successfully")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut file = File::open("./src/itinbuilder.json").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Failed to read config file");
    let config: structure::Configuration =
        serde_json::from_str(&contents).expect("Failed to parse config file");
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(config.log().pattern())))
        .build(config.log().file())
        .expect("Failed to create file appender");
    let log_config = log4rs::Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(
            Root::builder()
                .appender("logfile")
                .build(log::LevelFilter::Trace),
        )
        .expect("Failed to build Log config");
    let _handler = log4rs::init_config(log_config).expect("Failed to initialize logger");
    let graph = Graph::new(
        config.neo4j().uri(),
        config.neo4j().username(),
        config.neo4j().password(),
    )
    .await
    .expect("Failed to connect to Neo4j");
    //let connection = utils::make_db_connection(config.database());
    // let pool = match PgPoolOptions::new()
    //     .max_connections(10)
    //     .connect(&connection).await
    // {
    //     Ok(pool) => {
    //         info!("Connection to the database is successful!");
    //         pool
    //     }
    //     Err(err) => {
    //         error!("Failed to connect to the database: {:?}", err);
    //         std::process::exit(1);
    //     }
    // };
    //db::check_db_status(&pool).await;

    let app_state = web::Data::new(WebData::new(graph.clone()));
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(search)
            .service(web::scope("/schedule").route("/airport", web::post()))
            //.service(import_ssim)
            //.service(services::health_check)
            .wrap(Logger::default())
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
