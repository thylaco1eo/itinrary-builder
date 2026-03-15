use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::RecordId;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::domain::{flightplan::FlightPlan,route::Route};

pub async fn add_flights_batch(
    db: &Surreal<Any>,
    rows: Vec<FlightRow>,
) -> surrealdb::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // 使用 SDK 的 insert 一次性插入多条记录
    let _created: Vec<FlightRow> = db
        .insert("flight")
        .content(rows)
        .await?;
    Ok(())
}

pub async fn add_route(db: &Surreal<Any>, plan: &FlightPlan) -> surrealdb::Result<()> {
    let origin = plan.origin.as_str();
    let destination = plan.destination.as_str();
    let flight_id = format!("{}_{}", plan.company, plan.flight_no);
    let company = plan.company.clone();
    let route_id = format!("{}_{}", origin, destination);

    match db.select::<Option<Route>>(("route", route_id.as_str())).await? {
        Some(mut route) => {
            let mut dirty = false;

            if !route.flights.contains(&flight_id) {
                route.flights.push(flight_id);
                dirty = true;
            }
            if !route.companies.contains(&company) {
                route.companies.push(company);
                dirty = true;
            }

            if dirty {
                let _: Option<Route> = db
                    .update(("route", route_id.as_str()))
                    .merge(route)
                    .await?;
            }
        }
        None => {
            let route = Route::new(
                RecordId::new("airport", origin),
                RecordId::new("airport", destination),
                RecordId::new("route",route_id.as_str()),
                vec![flight_id],
                vec![company]
            );
            let _: Vec<Route> = db.insert("route").relation(route).await?;
        }
    }

    Ok(())
}

pub async fn get_flights(db: &Surreal<Any>) -> Vec<FlightRow> {
    db.select::<Vec<FlightRow>>("flight").await.unwrap_or(vec![])
}
