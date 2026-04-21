use crate::domain::mct::{AirportMctData, GlobalMctData};
use crate::Infrastructure::db::model::mct_row::MctRow;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::RecordIdKey;

const GLOBAL_MCT_RECORD_ID: &str = "global";

pub async fn get_airport_mct(
    db: &Surreal<Any>,
    code: &str,
) -> surrealdb::Result<Option<AirportMctData>> {
    let record: Option<MctRow> = db.select(("mct", code.to_string())).await?;
    Ok(record.map(mct_row_payload))
}

pub async fn get_all_airport_mct(
    db: &Surreal<Any>,
) -> surrealdb::Result<Vec<(String, AirportMctData)>> {
    let mut response = db.query("SELECT * FROM mct").await?;
    let rows: Vec<MctRow> = response.take(0)?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let RecordIdKey::String(code) = &row.id.key else {
                return None;
            };
            if code == GLOBAL_MCT_RECORD_ID {
                return None;
            }
            Some((code.clone(), mct_row_payload(row)))
        })
        .collect())
}

pub async fn set_airport_mct(
    db: &Surreal<Any>,
    code: &str,
    airport_mct: &AirportMctData,
) -> surrealdb::Result<()> {
    if airport_mct.mct_records.is_empty() && airport_mct.connection_building_filters.is_empty() {
        let _: Option<MctRow> = db.delete(("mct", code.to_string())).await?;
        return Ok(());
    }

    let exists: Option<MctRow> = db.select(("mct", code.to_string())).await?;
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
    let rows = get_all_airport_mct(db).await?;
    for (code, _) in rows {
        let _: Option<MctRow> = db.delete(("mct", code)).await?;
    }
    Ok(())
}

pub async fn get_global_mct(db: &Surreal<Any>) -> surrealdb::Result<GlobalMctData> {
    let record: Option<MctRow> = db.select(("mct", GLOBAL_MCT_RECORD_ID.to_string())).await?;
    Ok(record.map(mct_row_payload).unwrap_or_default())
}

pub async fn set_global_mct(
    db: &Surreal<Any>,
    global_mct: &GlobalMctData,
) -> surrealdb::Result<()> {
    if global_mct.mct_records.is_empty() && global_mct.connection_building_filters.is_empty() {
        return clear_global_mct(db).await;
    }

    let exists: Option<MctRow> = db.select(("mct", GLOBAL_MCT_RECORD_ID.to_string())).await?;
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
    let _: Option<MctRow> = db.delete(("mct", GLOBAL_MCT_RECORD_ID.to_string())).await?;
    Ok(())
}

fn mct_row_payload(row: MctRow) -> AirportMctData {
    AirportMctData {
        mct_records: row.mct_records,
        connection_building_filters: row.connection_building_filters,
    }
}
