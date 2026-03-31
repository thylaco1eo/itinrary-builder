use serde::Deserialize;

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
