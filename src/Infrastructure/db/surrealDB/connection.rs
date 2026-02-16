use surrealdb::engine::any::Any;
use surrealdb::Surreal;

pub async fn check_db_status(db:&Surreal<Any>, ns: &str, database: &str){
    let sql = format!("DEFINE NAMESPACE IF NOT EXISTS {};", ns);
    let mut response = db.query(sql).await.expect("Failed to execute define query");
    db.use_ns(ns).await.expect("Failed to select namespace");
    response = db.query(format!("DEFINE DATABASE IF NOT EXISTS {};", database)).await.expect("Failed to execute define query");
    response.check().expect("Failed to define namespace or database");
}