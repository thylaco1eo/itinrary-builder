use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotOD {
    pub origin: String,
    pub destination: String,
}
