use crate::domain::mct::{AirportMctRecord, ConnectionBuildingFilter};
use serde::{Deserialize, Serialize};
use surrealdb_types::{RecordId, SurrealValue};

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
pub struct MctRow {
    pub id: RecordId,
    #[serde(default)]
    pub mct_records: Vec<AirportMctRecord>,
    #[serde(default)]
    pub connection_building_filters: Vec<ConnectionBuildingFilter>,
}
