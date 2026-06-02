use serde::Deserialize;

#[derive(Deserialize)]
pub struct Log {
    level: String,
    file: String,
    pattern: String,
    #[serde(default)]
    request_trace: bool,
}

impl Log {
    pub fn new(level: String, file: String, pattern: String, request_trace: bool) -> Self {
        Log {
            level,
            file,
            pattern,
            request_trace,
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
    pub fn request_trace(&self) -> bool {
        self.request_trace
    }
}
