use crate::structure::DataBase;
use serde_json;
use std::collections::HashMap;
use neo4rs::{query, Query};

// pub fn load_configuration() -> WebData {
//     let mut file = File::open("./src/initbuilder.json").unwrap();
//     let mut contents = String::new();
//     file.read_to_string(&mut contents).expect("Failed to read config file");
//     let db_info: DataBase = serde_json::from_str(&contents).expect("Failed to parse config file");
//     let dpt_apt = HashMap::new();
//     WebData::new(Mutex::new(dpt_apt), db_info)
// }

pub fn make_db_connection(db_config: &DataBase) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}?schema=public",
        db_config.username(),
        db_config.password(),
        db_config.host(),
        db_config.port(),
        db_config.dbname()
    )
}

pub fn check_ib_reqbody(reqbody: String) -> bool {
    // This function checks if the request body for ITINBUILDER search contains required fields
    if reqbody.len() != 17{
        return false;
    }else if &reqbody[0..2] != "AV" {
        return false;
    }
    true
}


pub fn make_request(reqbody: String) -> Query{
    let dep_station = &reqbody[2..5];
    let arr_station = &reqbody[5..8];
    let dep_date = &reqbody[8..15];
    let cql = include_str!("./sql/search_IB.cql");
    query(cql).param("dep_airport",dep_station).param("arr_airport", arr_station)
}