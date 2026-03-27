use chrono::{Duration, NaiveTime};

pub fn compute_block_time(dep: NaiveTime, arr: NaiveTime) -> Duration {
    let minutes = arr.signed_duration_since(dep).num_minutes();
    if minutes >= 0 {
        Duration::minutes(minutes)
    } else {
        Duration::minutes(minutes + 1440)
    }
}
