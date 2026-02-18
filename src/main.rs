pub mod services;
pub mod domain;
pub mod structure;
mod api;
mod Infrastructure;
//mod config;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
};
use std::fs::File;
use std::io::prelude::*;
use structure::*;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root as DBRoot;
use Infrastructure::db;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut file = File::open("./src/itinbuilder.json")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Failed to read config file");
    let config: Configuration =
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

    let database = connect("wss://elegant-lotus-06e6vsqajhrt79s0hd50vftavs.aws-euw1.surreal.cloud/").await.expect("Failed to connect to SurrealDB");
    database.signin(DBRoot {
        username: config.database().username().to_string(),
        password: config.database().password().to_string(),
    }).await.expect("Failed to authenticate to SurrealDB");

    db::surrealDB::connection::check_db_status(&database, config.database().namespace(), config.database().dbname()).await;
    database.use_ns(config.database().namespace()).use_db(config.database().dbname()).await.expect("Failed to select namespace and database");

    let app_state = web::Data::new(WebData::new(database.clone()));
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(api::schedule::add_schedule::add_schedule)
            .service(api::utils::health_check::health_check)
            .service(api::airport::add_airport::add_airport)
            .wrap(Logger::default())
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
