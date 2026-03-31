mod Infrastructure;
mod api;
mod config;
pub mod domain;
pub mod services;
mod memory;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
};
use memory::core::WebData;
use std::fs::File;
use std::io::prelude::*;
use crate::config::Configuration;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root as DBRoot;
use Infrastructure::db;
//use Infrastructure::db::repository::flight_repo::get_flights;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut file = File::open("./src/itinbuilder.json")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Failed to read config file");
    let config: Configuration =
        serde_json::from_str(&contents).expect("Failed to parse config file");
    let application_port = config.application().port();
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

    println!(
        "Connecting to SurrealDB at ws://{}:{}...",
        config.database().host(),
        config.database().port()
    );
    let database = connect(format!(
        "ws://{}:{}",
        config.database().host(),
        config.database().port()
    ))
    .await
    .expect("Failed to connect to SurrealDB");
    println!("Connected to SurrealDB.");
    database
        .signin(DBRoot {
            username: config.database().username().to_string(),
            password: config.database().password().to_string(),
        })
        .await
        .expect("Failed to authenticate to SurrealDB");
    println!("Authenticated to SurrealDB.");

    db::surrealDB::connection::check_db_status(
        &database,
        config.database().namespace(),
        config.database().dbname(),
    )
    .await;
    println!(
        "Selecting namespace '{}' and database '{}'...",
        config.database().namespace(),
        config.database().dbname()
    );
    database
        .use_ns(config.database().namespace())
        .use_db(config.database().dbname())
        .await
        .expect("Failed to select namespace and database");
    println!("Selected namespace and database.");
    let app_state: web::Data<WebData> = web::Data::new(WebData::new(database.clone()).await);
    println!(
        "Startup complete. {} airports and {} flights are ready in memory.",
        app_state.airports().len(),
        app_state.flights().len()
    );
    println!(
        "HTTP server listening on http://127.0.0.1:{}",
        application_port
    );
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(api::schedule::add_schedule::add_schedule)
            .service(api::utils::health_check::health_check)
            .service(api::airport::add_airport)
            .service(api::ib::get_ib)
            .wrap(Logger::default())
    })
    .bind(("127.0.0.1", application_port))?
    .run()
    .await
}
