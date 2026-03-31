use serde::Deserialize;

use crate::config::{Application, DataBase, Log};

#[derive(Deserialize)]
pub struct Configuration {
    database: DataBase,
    log: Log,
    application: Application,
}

impl Configuration {
    pub fn new(database: DataBase, log: Log, application: Application) -> Self {
        Configuration {
            database,
            log,
            application,
        }
    }
    pub fn database(&self) -> &DataBase {
        &self.database
    }
    pub fn log(&self) -> &Log {
        &self.log
    }
    pub fn application(&self) -> &Application {
        &self.application
    }
}
