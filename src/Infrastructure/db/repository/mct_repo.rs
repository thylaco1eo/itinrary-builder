use std::env;
use std::time::Duration;

use actix_web::rt::time::timeout;
use crate::domain::mct::{AirportMctData, GlobalMctData};
use crate::Infrastructure::db::model::mct_row::MctRow;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb::Error as SurrealError;
use surrealdb_types::RecordIdKey;

const GLOBAL_MCT_RECORD_ID: &str = "global";
const DEFAULT_AIRPORT_MCT_PRELOAD_BATCH_SIZE: usize = 25;
const DEFAULT_AIRPORT_MCT_PRELOAD_MIN_BATCH_SIZE: usize = 5;
const DEFAULT_AIRPORT_MCT_PRELOAD_TIMEOUT_SECS: u64 = 30;

pub async fn get_airport_mct(
    db: &Surreal<Any>,
    code: &str,
) -> surrealdb::Result<Option<AirportMctData>> {
    match db.select(("mct", code.to_string())).await {
        Ok(record) => Ok(record.map(mct_row_payload)),
        Err(error) if is_missing_mct_table_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

pub async fn get_all_airport_mct(
    db: &Surreal<Any>,
) -> surrealdb::Result<Vec<(String, AirportMctData)>> {
    let mut airport_mct = Vec::new();
    let mut start = 0usize;
    let mut batch_size = airport_mct_preload_batch_size();
    let min_batch_size = airport_mct_preload_min_batch_size(batch_size);
    let timeout_secs = airport_mct_preload_timeout_secs();

    loop {
        let sql = format!(
            "SELECT id, mct_records, connection_building_filters \
             FROM mct \
             WHERE id != type::record('mct','{global_id}') \
             START {start} LIMIT {batch_size}",
            global_id = GLOBAL_MCT_RECORD_ID,
            start = start,
            batch_size = batch_size
        );

        let mut response = match timeout(Duration::from_secs(timeout_secs), db.query(&sql)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) if is_missing_mct_table_error(&error) => return Ok(Vec::new()),
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                if batch_size > min_batch_size {
                    batch_size = (batch_size / 2).max(min_batch_size);
                    continue;
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "airport MCT batch query timed out after {}s at offset {}",
                        timeout_secs, start
                    ),
                )
                .into());
            }
        };

        let rows: Vec<MctRow> = response.take(0)?;
        let row_count = rows.len();

        airport_mct.extend(rows.into_iter().filter_map(|row| {
            let RecordIdKey::String(code) = &row.id.key else {
                return None;
            };
            Some((code.clone(), mct_row_payload(row)))
        }));

        if row_count < batch_size {
            break;
        }

        start += row_count;
    }

    Ok(airport_mct)
}

pub async fn set_airport_mct(
    db: &Surreal<Any>,
    code: &str,
    airport_mct: &AirportMctData,
) -> surrealdb::Result<()> {
    if airport_mct.mct_records.is_empty() && airport_mct.connection_building_filters.is_empty() {
        match db.delete(("mct", code.to_string())).await {
            Ok::<Option<MctRow>, _>(_) => {}
            Err(error) if is_missing_mct_table_error(&error) => {}
            Err(error) => return Err(error),
        }
        return Ok(());
    }

    let exists: Option<MctRow> = match db.select(("mct", code.to_string())).await {
        Ok(record) => record,
        Err(error) if is_missing_mct_table_error(&error) => None,
        Err(error) => return Err(error),
    };
    let query = if exists.is_some() {
        "UPDATE type::record('mct',$id) SET mct_records = $mct_records, connection_building_filters = $connection_building_filters"
    } else {
        "CREATE type::record('mct',$id) SET mct_records = $mct_records, connection_building_filters = $connection_building_filters"
    };

    db.query(query)
        .bind(("id", code.to_string()))
        .bind(("mct_records", airport_mct.mct_records.clone()))
        .bind((
            "connection_building_filters",
            airport_mct.connection_building_filters.clone(),
        ))
        .await?;
    Ok(())
}

pub async fn clear_all_airport_mct(db: &Surreal<Any>) -> surrealdb::Result<()> {
    match db
        .query(format!(
            "DELETE mct WHERE id != type::record('mct','{}')",
            GLOBAL_MCT_RECORD_ID
        ))
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if is_missing_mct_table_error(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

pub async fn get_global_mct(db: &Surreal<Any>) -> surrealdb::Result<GlobalMctData> {
    match db.select(("mct", GLOBAL_MCT_RECORD_ID.to_string())).await {
        Ok(record) => Ok(record.map(mct_row_payload).unwrap_or_default()),
        Err(error) if is_missing_mct_table_error(&error) => Ok(GlobalMctData::default()),
        Err(error) => Err(error),
    }
}

pub async fn set_global_mct(
    db: &Surreal<Any>,
    global_mct: &GlobalMctData,
) -> surrealdb::Result<()> {
    if global_mct.mct_records.is_empty() && global_mct.connection_building_filters.is_empty() {
        return clear_global_mct(db).await;
    }

    let exists: Option<MctRow> = match db.select(("mct", GLOBAL_MCT_RECORD_ID.to_string())).await {
        Ok(record) => record,
        Err(error) if is_missing_mct_table_error(&error) => None,
        Err(error) => return Err(error),
    };
    let query = if exists.is_some() {
        "UPDATE type::record('mct',$id) SET mct_records = $mct_records, connection_building_filters = $connection_building_filters"
    } else {
        "CREATE type::record('mct',$id) SET mct_records = $mct_records, connection_building_filters = $connection_building_filters"
    };

    db.query(query)
        .bind(("id", GLOBAL_MCT_RECORD_ID.to_string()))
        .bind(("mct_records", global_mct.mct_records.clone()))
        .bind((
            "connection_building_filters",
            global_mct.connection_building_filters.clone(),
        ))
        .await?;
    Ok(())
}

pub async fn clear_global_mct(db: &Surreal<Any>) -> surrealdb::Result<()> {
    match db.delete(("mct", GLOBAL_MCT_RECORD_ID.to_string())).await {
        Ok::<Option<MctRow>, _>(_) => {}
        Err(error) if is_missing_mct_table_error(&error) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn mct_row_payload(row: MctRow) -> AirportMctData {
    AirportMctData {
        mct_records: row.mct_records,
        connection_building_filters: row.connection_building_filters,
    }
}

fn is_missing_mct_table_error(error: &SurrealError) -> bool {
    let message = error.to_string();
    message.contains("table 'mct' does not exist")
        || message.contains("table \"mct\" does not exist")
        || message.contains("The table 'mct' does not exist")
        || message.contains("The table \"mct\" does not exist")
}

fn airport_mct_preload_batch_size() -> usize {
    env::var("IB_AIRPORT_MCT_PRELOAD_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AIRPORT_MCT_PRELOAD_BATCH_SIZE)
}

fn airport_mct_preload_min_batch_size(initial_batch_size: usize) -> usize {
    env::var("IB_AIRPORT_MCT_PRELOAD_MIN_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0 && *value <= initial_batch_size)
        .unwrap_or(DEFAULT_AIRPORT_MCT_PRELOAD_MIN_BATCH_SIZE.min(initial_batch_size))
}

fn airport_mct_preload_timeout_secs() -> u64 {
    env::var("IB_AIRPORT_MCT_PRELOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AIRPORT_MCT_PRELOAD_TIMEOUT_SECS)
}
