use surrealdb::engine::any::Any;
use surrealdb::Surreal;

pub async fn check_db_status(db: &Surreal<Any>, ns: &str, database: &str) {
    let sql = format!("DEFINE NAMESPACE IF NOT EXISTS {};", ns);
    let response = db.query(sql).await.expect("Failed to execute define query");
    response.check().expect("Failed to define namespace");
    db.use_ns(ns).await.expect("Failed to select namespace");
    let response = db
        .query(format!("DEFINE DATABASE IF NOT EXISTS {};", database))
        .await
        .expect("Failed to execute define query");
    response
        .check()
        .expect("Failed to define namespace or database");
    db.use_ns(ns)
        .use_db(database)
        .await
        .expect("Failed to select namespace and database");
    let response = db
        .query(
            "DEFINE TABLE IF NOT EXISTS airport;DEFINE TABLE IF NOT EXISTS flight;DEFINE TABLE IF NOT EXISTS route TYPE RELATION;",
        )
        .await
        .expect("Failed to execute empty query");
    response.check().expect("Failed to define required tables");
}
