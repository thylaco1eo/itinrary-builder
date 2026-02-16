use serde::{Deserialize, Serialize};
use surrealdb::types::{Kind, SurrealValue, Value};

#[derive(Serialize,Deserialize,Clone)]
pub struct Route {
    pub id: String,
    pub flights: Vec<String>,
}

impl SurrealValue for Route {
    fn kind_of() -> Kind {
        Kind::Object
    }

    fn is_value(value: &Value) -> bool {
        matches!(value, Value::Object(_))
    }

    fn into_value(self) -> Value {
        serde_json::from_value(serde_json::to_value(self).unwrap_or_default()).unwrap_or(Value::None)
    }

    fn from_value(value: Value) -> surrealdb::types::anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(serde_json::from_value(serde_json::to_value(value)?)?)
    }
}

impl Route {
    pub fn new(id: String, flights: Vec<String>) -> Self {
        Self { id, flights }
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn flights(&self) -> &[String] {
        &self.flights
    }
}