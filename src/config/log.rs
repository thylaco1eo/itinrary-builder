use serde::Deserialize;

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
