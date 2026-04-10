use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Itinerary {
    origin: String,
    destination: String,
    dep_date: String,
    transport: Option<String>,
}
impl Itinerary {
    pub fn new() -> Itinerary {
        Itinerary {
            origin: String::new(),
            destination: String::new(),
            dep_date: String::new(),
            transport: None,
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
}
