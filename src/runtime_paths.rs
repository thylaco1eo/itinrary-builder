use std::env;
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};

const CONFIG_DIR_NAME: &str = "etc";
const CONFIG_FILE_NAME: &str = "itinbuilder.json";
const LEGACY_CONFIG_PATH: &str = "src/itinbuilder.json";
const LOG_DIR_NAME: &str = "log";
const REQUEST_LOG_FILE_NAME: &str = "requests.log";

pub fn configuration_file() -> io::Result<PathBuf> {
    let deployed_config = executable_dir()?
        .join(CONFIG_DIR_NAME)
        .join(CONFIG_FILE_NAME);
    if deployed_config.exists() {
        return Ok(deployed_config);
    }

    let cwd_config = env::current_dir()?
        .join(CONFIG_DIR_NAME)
        .join(CONFIG_FILE_NAME);
    if cwd_config.exists() {
        return Ok(cwd_config);
    }

    let legacy_config = env::current_dir()?.join(LEGACY_CONFIG_PATH);
    if legacy_config.exists() {
        return Ok(legacy_config);
    }

    Ok(deployed_config)
}

pub fn resolve_executable_relative<P: AsRef<Path>>(path: P) -> io::Result<PathBuf> {
    let path = path.as_ref();
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    Ok(executable_dir()?.join(path))
}

pub fn request_log_file() -> io::Result<PathBuf> {
    Ok(executable_dir()?
        .join(LOG_DIR_NAME)
        .join(REQUEST_LOG_FILE_NAME))
}

pub fn create_parent_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => create_dir_all(parent),
        None => Ok(()),
    }
}

fn executable_dir() -> io::Result<PathBuf> {
    let executable_path = env::current_exe()?;
    executable_path
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            io::Error::other(format!(
                "failed to resolve executable directory from {}",
                executable_path.display()
            ))
        })
}
