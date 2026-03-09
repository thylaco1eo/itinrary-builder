use serde::{Deserialize, Serialize};
use surrealdb::types::{SurrealValue,RecordId};

#[derive(Serialize,Deserialize,Clone,SurrealValue)]
pub struct Route {
    #[surreal(rename = "in")]
    dep_station: RecordId,
    #[surreal(rename = "out")]
    arr_station: RecordId,
    pub id: RecordId,
    pub flights: Vec<String>,
    pub companies: Vec<String>,
}

impl Route {
    pub fn new(dep_station: RecordId,arr_station:RecordId,id: RecordId, flights: Vec<String>,companies: Vec<String>) -> Self {
        Self { dep_station,arr_station, id, flights, companies}
    }
    pub fn flights(&self) -> &[String] {
        &self.flights
    }
}