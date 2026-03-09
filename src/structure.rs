use actix_multipart::form::tempfile::TempFile;
use actix_multipart::form::MultipartForm;
use serde::Deserialize;
use surrealdb::{Surreal, engine::any};

pub struct WebData {
    database: Surreal<any::Any>,
}

impl WebData {
    pub fn new(data_base: Surreal<any::Any>) -> Self {
        WebData {
            database: data_base,
        }
    }

    pub fn database(&self) -> &Surreal<any::Any> {
        &self.database
    }
}


#[derive(MultipartForm)]
pub struct SSIM {
    file: TempFile,
}

impl SSIM {
    pub fn file(&mut self) -> &mut TempFile {
        &mut self.file
    }
}

#[derive(Deserialize, Clone)]
pub struct DataBase {
    host: String,
    port: String,
    username: String,
    password: String,
    namespace: String,
    dbname: String,
}

impl DataBase {
    pub fn new(
        host: String,
        port: String,
        username: String,
        password: String,
        namespace: String,
        dbname: String,
    ) -> Self {
        DataBase {
            host,
            port,
            username,
            password,
            namespace,
            dbname,
        }
    }
    pub fn host(&self) -> &String {
        &self.host
    }
    pub fn port(&self) -> &String {
        &self.port
    }
    pub fn username(&self) -> &String {
        &self.username
    }
    pub fn password(&self) -> &String {
        &self.password
    }
    pub fn dbname(&self) -> &String {
        &self.dbname
    }
    pub fn namespace(&self) -> &String {
        &self.namespace
    }
}

#[derive(Deserialize)]
pub struct Log {
    level: String,
    file: String,
    pattern: String,
}

impl Log {
    pub fn new(level: String, file: String, pattern: String) -> Self {
        Log {
            level,
            file,
            pattern,
        }
    }
    pub fn level(&self) -> &String {
        &self.level
    }
    pub fn file(&self) -> &String {
        &self.file
    }
    pub fn pattern(&self) -> &String {
        &self.pattern
    }
}

#[derive(Deserialize)]
pub struct Configuration {
    database: DataBase,
    log: Log,
}

impl Configuration {
    pub fn new(database: DataBase,log: Log) -> Self {
        Configuration {
            database,
            log,
        }
    }
    pub fn database(&self) -> &DataBase {
        &self.database
    }
    pub fn log(&self) -> &Log {
        &self.log
    }
}
