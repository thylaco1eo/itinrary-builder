use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use crate::Infrastructure::db::model::flight_row::FlightRow;
use crate::domain::{flightplan::FlightPlan,route::Route};

pub async fn add_flights_batch(
    db: &Surreal<Any>,
    rows: Vec<FlightRow>,
) -> surrealdb::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    
    db.query("BEGIN").await?;
    for row in rows {
        db.query("CREATE flight CONTENT $data")
            .bind(("data", row))
            .await?;
    }
    db.query("COMMIT").await?;
    Ok(())
}

pub async fn add_route(db: &Surreal<Any>, plan: FlightPlan) -> surrealdb::Result<()> {
    let origin = plan.origin.as_str();
    let destination = plan.destination.as_str();
    let flight_id = format!("{}{}", plan.company, plan.flight_no);
    
    let route_id = format!("{}_{}", origin, destination);
    let _route: Option<Route> = db.select(("route", route_id.as_str())).await?;

    if _route.is_some() {
        db.query("UPDATE type::thing('route', $route_id) SET flights += $flight_id")
            .bind(("route_id", route_id))
            .bind(("flight_id", flight_id))
            .await?;
    } else {
        db.query("RELATE type::thing('airport', $origin) -> route -> type::thing('airport', $destination) 
                  SET id = type::thing('route', $route_id), flights = [$flight_id]")
            .bind(("origin", origin.to_string()))
            .bind(("destination", destination.to_string()))
            .bind(("route_id", route_id))
            .bind(("flight_id", flight_id))
            .await?;
    }
    Ok(())
}