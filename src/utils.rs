use crate::structure::{DataBase};
use std::fs::File;
use std::io::prelude::*;
use serde_json;
use std::collections::HashMap;
use std::sync::Mutex;


// pub fn load_configuration() -> WebData {
//     let mut file = File::open("./src/initbuilder.json").unwrap();
//     let mut contents = String::new();
//     file.read_to_string(&mut contents).expect("Failed to read config file");
//     let db_info: DataBase = serde_json::from_str(&contents).expect("Failed to parse config file");
//     let dpt_apt = HashMap::new();
//     WebData::new(Mutex::new(dpt_apt), db_info)
// }

pub fn make_db_connection(db_config:&DataBase) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}?schema=public",
        db_config.username(),
        db_config.password(),
        db_config.host(),
        db_config.port(),
        db_config.dbname()
    )
}