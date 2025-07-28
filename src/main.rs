//extern crate chrono;
mod flight_info;
pub mod services;
use actix_multipart::form::tempfile::TempFile;
use postgres::{Client};
use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;
use serde_json;
use postgres_openssl::MakeTlsConnector;
use openssl::ssl::{SslConnector, SslMethod,SslVerifyMode};
use std::sync::Mutex;
use std::collections::HashMap;
use flight_info::FlightInfo;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use actix_multipart::{form::bytes, Multipart};
use actix_multipart::form::MultipartForm;


#[derive(Deserialize)]
struct DataBase{
    host: String,
    port: String,
    username: String,
    password: String,
    dbname: String,
}

struct Configuration {
    flights: Mutex<HashMap<String, Vec<FlightInfo>>>,
    db_info: DataBase,
}

#[derive(MultipartForm)]
struct SSIM{
    file : TempFile,
}

#[get("/search")]
async fn search(data: web::Data<Configuration>,req_body:String) -> impl Responder {
    let flights = data.flights.lock().unwrap();
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
        let mut data_new = data.flights.lock().unwrap();
        data_new.clear(); // Clear existing data before importing new one
        *data_new  = services::data_service::import_schedule_file(multipart_form.file.file.as_file_mut());
    }
    HttpResponse::Ok().body("File imported successfully")
}

//fn main() {
    
    //let mut path = String::new();
    //println!("Please enter the path to the flight data file:");
    //io::stdin().read_line(&mut path).expect("Failed to read line");
    // let dpt_apt: std::collections::HashMap<String, Vec<flight_info::FlightInfo>> = services::data_service::import_schedule_file("./data/cassim0401");
    // let request = "PEKFRA01MAY25+0800";
    // let path_list = services::search_service::search_flight::search_flight(&dpt_apt, request);
    // if path_list.is_empty() {
    //     println!("No flights found");
    // } else {
    //     for path in path_list {
    //         println!("Found path:");
    //         for (flt_id, dep_time, arr_station, flight_time) in path {
    //             println!("Flight ID: {}, Departure Time: {}, Arrival Station: {}, Flight Time: {} minutes", flt_id, dep_time, arr_station, flight_time);
    //         }
    //     }
    // }
    // let mut file = File::open("src/initbuilder.json").unwrap();
    // let mut contents = String::new();
    // file.read_to_string(&mut contents).unwrap();
    // let db_info:DataBase = serde_json::from_str(&contents).unwrap();
    // //let connection_string = format!("postgresql://{}:{}@{}:{}/{}",
    // //    db_info.username, db_info.password, db_info.host, db_info.port, db_info.dbname);
    // let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    // builder.set_verify(SslVerifyMode::NONE);
    // let connector = MakeTlsConnector::new(builder.build());
    // let connection_string = format!(
    //     "host={} port={} dbname={} user={} password={}",
    //     db_info.host, db_info.port, db_info.dbname, db_info.username, db_info.password
    // );
    // let mut client = Client::connect(&connection_string, connector)
    //     .expect("Failed to connect to the database");
    // let query = client.query(
    //     "if (to_regclass('ITINBUILDER') is null) then
    //         create table ITINBUILDER (
    //             id serial primary key,
    //             dpt_station varchar(3) not null,
    //             arr_station varchar(3) not null,
    //             dpt_date date not null,
    //             dpt_time time not null,
    //             arr_time time not null,
    //             flight_id varchar(10) not null,
    //             flight_time int not null
    //         );
    //     end if;
    // ", &[])
    //     .expect("Failed to execute query");
    // println!("{:?}", query);
//}

#[actix_web::main]
    async fn main() -> std::io::Result<()> {
        let mut file = File::open("./src/initbuilder.json").unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        let db_info:DataBase = serde_json::from_str(&contents).unwrap();
        let dpt_apt = HashMap::new();
        let app_state = web::Data::new(Configuration {
            flights: Mutex::new(dpt_apt),
            db_info: db_info,
        });
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