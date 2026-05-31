use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Itinerary {
    origin: String,
    destination: String,
    dep_date: String,
    transport: Option<String>,
    #[serde(alias = "operating_company")]
    operation_company: Option<String>,
    #[serde(alias = "travel_time")]
    #[serde(alias = "longest_travel_time")]
    #[serde(alias = "max_travel_time_days")]
    max_travel_time: Option<String>,
}
impl Itinerary {
    pub fn new() -> Itinerary {
        Itinerary {
            origin: String::new(),
            destination: String::new(),
            dep_date: String::new(),
            transport: None,
            operation_company: None,
            max_travel_time: None,
        }
    }
    pub fn get_origin(&self) -> String {
        self.origin.clone()
    }
    pub fn get_destination(&self) -> String {
        self.destination.clone()
    }
    pub fn get_dep_date(&self) -> String {
        self.dep_date.clone()
    }
    pub fn get_transport(&self) -> Option<String> {
        self.transport.clone()
    }
    pub fn get_operation_company(&self) -> Option<String> {
        self.operation_company.clone()
    }
    pub fn get_max_travel_time(&self) -> Option<String> {
        self.max_travel_time.clone()
    }
}
