use serde::Deserialize;

#[derive(Deserialize)]
pub struct Application {
    port: u16,
}

impl Application {
    pub fn new(port: u16) -> Self {
        Application { port }
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}
