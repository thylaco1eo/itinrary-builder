use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use serde::Deserialize;
use surrealdb_types::SurrealValue;

#[derive(Debug, Deserialize, SurrealValue)]
pub struct HotODRecord {
    pub origin: String,
    pub destination: String,
}

pub async fn list_hot_ods(db: &Surreal<Any>) -> surrealdb::Result<Vec<HotODRecord>> {
    db.select("hot_od").await
}

pub async fn add_hot_od(db: &Surreal<Any>, origin: &str, destination: &str) -> surrealdb::Result<()> {
    let exists: Option<HotODRecord> = db
        .query("SELECT * FROM hot_od WHERE origin = $origin AND destination = $dest LIMIT 1")
        .bind(("origin", origin.to_string()))
        .bind(("dest", destination.to_string()))
        .await?
        .take(0)?;

    if exists.is_some() {
        return Ok(());
    }

    db.query("CREATE hot_od SET origin = $origin, destination = $dest")
        .bind(("origin", origin.to_string()))
        .bind(("dest", destination.to_string()))
        .await?;

    Ok(())
}

pub async fn remove_hot_od(
    db: &Surreal<Any>,
    origin: &str,
    destination: &str,
) -> surrealdb::Result<usize> {
    let mut result = db
        .query("DELETE hot_od WHERE origin = $origin AND destination = $dest")
        .bind(("origin", origin.to_string()))
        .bind(("dest", destination.to_string()))
        .await?;

    let deleted: Vec<HotODRecord> = result.take(0)?;
    Ok(deleted.len())
}
