use crate::structure::{self, Airport};
use sqlx::Pool;
use neo4rs::{BoltList, BoltMap, BoltString, BoltType, Graph, query,Row};

pub async fn check_db_status(pool: &Pool<sqlx::Postgres>) {
    //This function checks if the ITINBUILDER schema exists in the database
    //and creates it if it does not exist.
    let result = sqlx::query(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'ITINBUILDER'
        )",
    )
    .execute(pool)
    .await
    .expect("Failed to check ITINBUILDER schema status");
    if result.rows_affected() == 0 {
        sqlx::query("CREATE SCHEMA ITINBUILDER")
            .execute(pool)
            .await
            .expect("Failed to create ITINBUILDER schema");
        println!("ITINBUILDER schema created");
    } else {
        println!("ITINBUILDER schema already exists");
    }
}

pub async fn init_table(pool: &Pool<sqlx::Postgres>) {
    // This function initializes the necessary tables in the ITINBUILDER schema.
    // You can add your table creation logic here.
    // For example:
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ITINBUILDER.flights (
            id SERIAL PRIMARY KEY,
            carrier VARCHAR(2) NOT NULL,
            flight_id VARCHAR(10) NOT NULL,
            departure_time TIMESTAMP WITH TIME ZONE NOT NULL,
            arrival_time TIMESTAMP WITH TIME ZONE NOT NULL,
            departure_station VARCHAR(3) NOT NULL,
            arrival_station VARCHAR(3) NOT NULL,
            frequency VARCHAR(7) NOT NULL,
            flight_time INT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .expect("Failed to create flights table");
}

pub async fn import_ssim(pool: &Pool<sqlx::Postgres>, flights: &Vec<structure::FlightInfo>) {
    // This function imports SSIM data into the database, It drop existing data and re-imports it.
    sqlx::query("TRUNCATE TABLE ITINBUILDER.flights")
        .execute(pool)
        .await
        .expect("Failed to truncate flights table");
    for flight in flights {
        sqlx::query(
            "INSERT INTO ITINBUILDER.flights (carrier, flight_id, departure_time, arrival_time, departure_station, arrival_station, frequency, flight_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        )
            .bind(&flight.carrier())
            .bind(&flight.flt_id())
            .bind(&flight.dpt_start_utc())
            .bind(&flight.dpt_end_utc())
            .bind(&flight.dpt_station())
            .bind(&flight.arr_station())
            .bind(&flight.frequency())
            .bind(flight.flight_time())
            .execute(pool).await
            .expect("Failed to insert flight data");
    }
}

pub async fn import_ssim_neo4j(graph:Graph,flights: &Vec<structure::FlightInfo>){
    let flight_maps: Vec<BoltType> = flights.iter().map(|f| {
        let mut map = BoltMap::new();
        map.put(BoltString::from("flt_id"), f.flt_id().clone().into());
        map.put(BoltString::from("carrier"), f.carrier().clone().into());
        map.put(BoltString::from("dpt_station"), f.dpt_station().clone().into());
        map.put(BoltString::from("arr_station"), f.arr_station().clone().into());
        map.put(BoltString::from("dpt_start_utc"), f.dpt_start_utc().to_rfc3339().into());
        map.put(BoltString::from("dpt_end_utc"), f.dpt_end_utc().to_rfc3339().into());
        map.put(BoltString::from("frequency"), f.frequency().clone().into());
        map.put(BoltString::from("flight_time"), f.flight_time().into());
        BoltType::Map(map)
    }).collect();
    
    let q = query(
        "UNWIND $flights AS flight
         MERGE (dpt:Airport {code: flight.dpt_station})
         MERGE (arr:Airport {code: flight.arr_station})
         CREATE (dpt)-[:FLIGHT {
             flt_id: flight.flt_id,
             carrier: flight.carrier,
             dpt_start_utc: datetime(flight.dpt_start_utc),
             dpt_end_utc: datetime(flight.dpt_end_utc),
             frequency: flight.frequency,
             flight_time: flight.flight_time
         }]->(arr)"
    )
    .param("flights", BoltList::from(flight_maps));
    
    graph.run(q).await?;
}

pub async fn create_airport_neo4j(graph: Graph, airport: Airport) -> Result<bool, neo4rs::Error> {
    // Check if airport with given id exists
    let q = query("MATCH (a:Airport {id: $id}) RETURN a IS NOT NULL AS exists")
        .param("id", airport.id().clone());
    let mut result: DetachedRowStream= graph.run(q).await.unwrap();
    if let Some(row_res) = result.next().await {
        let row = row_res?;
        let exists = row.get("exists").and_then(|v| v.as_bool()).unwrap_or(false);
        if exists {
            return Ok(false);
        }
    }

    // Not found -> create and return true
    let q = query(
        "CREATE (a:Airport {id: $id, name: $name, city: $city, country: $country})",
    )
    .param("id", airport.id().clone())
    .param("name", airport.name().clone())
    .param("city", airport.city().clone())
    .param("country", airport.country().clone());
    graph.run(q).await?;
    Ok(true)
}