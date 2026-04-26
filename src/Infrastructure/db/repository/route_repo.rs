use std::collections::{HashMap, HashSet};

use crate::domain::airport::Airport;
use serde::Deserialize;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::{RecordId, RecordIdKey, SurrealValue};

const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

#[derive(Debug, Deserialize, SurrealValue)]
pub struct Segment {
    pub from: RecordId,
    pub to: RecordId,
    pub companies: Vec<String>,
    pub flights: Vec<String>,
    pub distance: f64,
}

#[derive(Debug, Deserialize, SurrealValue)]
pub struct PathResult {
    pub airports: Vec<RecordId>,
    pub segments: Vec<Segment>,
    pub total_dist: f64,
    pub circuity: f64,
}

#[derive(Clone, Debug, Deserialize, SurrealValue)]
struct RouteEdgeRecord {
    #[serde(rename = "in")]
    #[surreal(rename = "in")]
    from: RecordId,
    #[serde(rename = "out")]
    #[surreal(rename = "out")]
    to: RecordId,
    companies: Vec<String>,
    flights: Vec<String>,
}

#[derive(Clone, Debug)]
struct RouteEdge {
    from: String,
    to: String,
    companies: Vec<String>,
    flights: Vec<String>,
    distance: f64,
}

pub async fn find_paths(
    db: &Surreal<Any>,
    dep: &str,
    arr: &str,
    airports: &HashMap<String, Airport>,
    max_hops: u8,
    max_circuity: f64,
) -> surrealdb::Result<Vec<PathResult>> {
    let route_records: Vec<RouteEdgeRecord> = db.select("route").await?;
    let route_edges = route_records
        .into_iter()
        .filter_map(|record| route_edge_from_record(record, airports))
        .collect::<Vec<_>>();

    Ok(find_paths_from_route_edges(
        &route_edges,
        airports,
        dep,
        arr,
        max_hops,
        max_circuity,
    ))
}

fn find_paths_from_route_edges(
    route_edges: &[RouteEdge],
    airports: &HashMap<String, Airport>,
    dep: &str,
    arr: &str,
    max_hops: u8,
    max_circuity: f64,
) -> Vec<PathResult> {
    if max_hops == 0 {
        return Vec::new();
    }

    let city_index = build_city_index(airports);
    let origin_airports = related_airport_codes(dep, airports, &city_index);
    let destination_airports = related_airport_codes(arr, airports, &city_index)
        .into_iter()
        .collect::<HashSet<_>>();
    let adjacency = build_route_adjacency(route_edges);
    let mut path_edges = Vec::new();
    let mut visited_airports = HashSet::new();
    let mut seen_paths = HashSet::new();
    let mut results = Vec::new();

    for origin in origin_airports {
        let Some(edge_indices) = adjacency.get(&origin) else {
            continue;
        };
        for &edge_index in edge_indices {
            let edge = &route_edges[edge_index];
            visited_airports.clear();
            visited_airports.insert(edge.from.clone());
            visited_airports.insert(edge.to.clone());
            path_edges.clear();
            path_edges.push(edge_index);
            collect_route_paths(
                route_edges,
                airports,
                &city_index,
                &adjacency,
                &destination_airports,
                max_hops,
                max_circuity,
                &mut path_edges,
                &mut visited_airports,
                &mut seen_paths,
                &mut results,
            );
        }
    }

    results
}

fn collect_route_paths(
    route_edges: &[RouteEdge],
    airports: &HashMap<String, Airport>,
    city_index: &HashMap<String, Vec<String>>,
    adjacency: &HashMap<String, Vec<usize>>,
    destination_airports: &HashSet<String>,
    max_hops: u8,
    max_circuity: f64,
    path_edges: &mut Vec<usize>,
    visited_airports: &mut HashSet<String>,
    seen_paths: &mut HashSet<String>,
    results: &mut Vec<PathResult>,
) {
    let last_edge = &route_edges[*path_edges.last().unwrap()];
    if destination_airports.contains(&last_edge.to) {
        if let Some(path) = build_path_result(route_edges, airports, path_edges, max_circuity) {
            let signature = path_signature(&path);
            if seen_paths.insert(signature) {
                results.push(path);
            }
        }
    }

    if path_edges.len() >= usize::from(max_hops) {
        return;
    }

    for departure in related_airport_codes(&last_edge.to, airports, city_index) {
        let Some(edge_indices) = adjacency.get(&departure) else {
            continue;
        };
        for &edge_index in edge_indices {
            let next_edge = &route_edges[edge_index];
            if next_edge.to == last_edge.to || visited_airports.contains(&next_edge.to) {
                continue;
            }
            if next_edge.from != last_edge.to && visited_airports.contains(&next_edge.from) {
                continue;
            }

            let inserted_from = visited_airports.insert(next_edge.from.clone());
            let inserted_to = visited_airports.insert(next_edge.to.clone());
            path_edges.push(edge_index);
            collect_route_paths(
                route_edges,
                airports,
                city_index,
                adjacency,
                destination_airports,
                max_hops,
                max_circuity,
                path_edges,
                visited_airports,
                seen_paths,
                results,
            );
            path_edges.pop();
            if inserted_to {
                visited_airports.remove(&next_edge.to);
            }
            if inserted_from {
                visited_airports.remove(&next_edge.from);
            }
        }
    }
}

fn route_edge_from_record(
    record: RouteEdgeRecord,
    airports: &HashMap<String, Airport>,
) -> Option<RouteEdge> {
    let from = record_id_code(&record.from)?;
    let to = record_id_code(&record.to)?;
    let distance = airport_distance_meters(airports.get(&from)?, airports.get(&to)?);

    Some(RouteEdge {
        from,
        to,
        companies: record.companies,
        flights: record.flights,
        distance,
    })
}

fn build_path_result(
    route_edges: &[RouteEdge],
    airports: &HashMap<String, Airport>,
    path_edges: &[usize],
    max_circuity: f64,
) -> Option<PathResult> {
    let first_edge = &route_edges[*path_edges.first()?];
    let last_edge = &route_edges[*path_edges.last()?];
    let direct_dist = airport_distance_meters(
        airports.get(&first_edge.from)?,
        airports.get(&last_edge.to)?,
    );
    if direct_dist <= f64::EPSILON {
        return None;
    }

    let mut total_dist = 0.0;
    let mut segments = Vec::with_capacity(path_edges.len());
    let mut airports_in_path = vec![RecordId::new("airport", first_edge.from.as_str())];
    let mut previous_to: Option<&str> = None;

    for edge_index in path_edges {
        let edge = &route_edges[*edge_index];
        if let Some(previous_to) = previous_to {
            if previous_to != edge.from {
                total_dist +=
                    airport_distance_meters(airports.get(previous_to)?, airports.get(&edge.from)?);
                airports_in_path.push(RecordId::new("airport", edge.from.as_str()));
            }
        }
        total_dist += edge.distance;
        airports_in_path.push(RecordId::new("airport", edge.to.as_str()));
        previous_to = Some(edge.to.as_str());
        segments.push(Segment {
            from: RecordId::new("airport", edge.from.as_str()),
            to: RecordId::new("airport", edge.to.as_str()),
            companies: edge.companies.clone(),
            flights: edge.flights.clone(),
            distance: edge.distance,
        });
    }

    let circuity = total_dist / direct_dist;
    if circuity > max_circuity {
        return None;
    }

    Some(PathResult {
        airports: airports_in_path,
        segments,
        total_dist,
        circuity,
    })
}

fn build_route_adjacency(route_edges: &[RouteEdge]) -> HashMap<String, Vec<usize>> {
    let mut adjacency: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, edge) in route_edges.iter().enumerate() {
        adjacency.entry(edge.from.clone()).or_default().push(index);
    }
    adjacency
}

fn build_city_index(airports: &HashMap<String, Airport>) -> HashMap<String, Vec<String>> {
    let mut city_index: HashMap<String, Vec<String>> = HashMap::new();
    for (code, airport) in airports {
        if let Some(city_key) = airport_city_key(airport) {
            city_index.entry(city_key).or_default().push(code.clone());
        }
    }
    for codes in city_index.values_mut() {
        codes.sort();
        codes.dedup();
    }
    city_index
}

fn related_airport_codes(
    code: &str,
    airports: &HashMap<String, Airport>,
    city_index: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut codes = airports
        .get(code)
        .and_then(airport_city_key)
        .and_then(|city_key| city_index.get(&city_key).cloned())
        .unwrap_or_else(|| vec![code.to_string()]);

    if !codes.iter().any(|candidate| candidate == code) {
        codes.push(code.to_string());
        codes.sort();
    }
    codes
}

fn airport_city_key(airport: &Airport) -> Option<String> {
    let country = normalize_city_key_part(airport.country()?)?;
    let city = normalize_city_key_part(airport.city()?)?;
    Some(format!("{country}|{city}"))
}

fn normalize_city_key_part(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn airport_distance_meters(left: &Airport, right: &Airport) -> f64 {
    let lat1 = left.latitude().to_radians();
    let lat2 = right.latitude().to_radians();
    let delta_lat = (right.latitude() - left.latitude()).to_radians();
    let delta_lon = (right.longitude() - left.longitude()).to_radians();
    let a =
        (delta_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_METERS * c
}

fn path_signature(path: &PathResult) -> String {
    path.segments
        .iter()
        .map(|segment| {
            format!(
                "{}>{}",
                record_id_code(&segment.from).unwrap_or_default(),
                record_id_code(&segment.to).unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn record_id_code(record_id: &RecordId) -> Option<String> {
    match &record_id.key {
        RecordIdKey::String(code) => Some(code.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::airport::AirportCode;

    #[test]
    fn connects_route_edges_across_same_city_airports() {
        let airports = sample_airports();
        let route_edges = vec![
            sample_edge(&airports, "AAA", "PEK", "CA", "100"),
            sample_edge(&airports, "PKX", "CCC", "CA", "200"),
        ];

        let paths = find_paths_from_route_edges(&route_edges, &airports, "AAA", "CCC", 2, 10.0);

        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0]
                .segments
                .iter()
                .map(|segment| (
                    record_id_code(&segment.from).unwrap(),
                    record_id_code(&segment.to).unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("AAA".to_string(), "PEK".to_string()),
                ("PKX".to_string(), "CCC".to_string())
            ]
        );
        assert_eq!(
            paths[0]
                .airports
                .iter()
                .map(|airport| record_id_code(airport).unwrap())
                .collect::<Vec<_>>(),
            vec!["AAA", "PEK", "PKX", "CCC"]
        );
    }

    #[test]
    fn accepts_same_city_destination_airport() {
        let airports = sample_airports();
        let route_edges = vec![sample_edge(&airports, "AAA", "PKX", "CA", "100")];

        let paths = find_paths_from_route_edges(&route_edges, &airports, "AAA", "PEK", 1, 10.0);

        assert_eq!(paths.len(), 1);
        assert_eq!(
            record_id_code(&paths[0].segments[0].to).unwrap(),
            "PKX".to_string()
        );
    }

    fn sample_airports() -> HashMap<String, Airport> {
        [
            sample_airport("AAA", "Alpha", "CN", 116.0, 39.0),
            sample_airport("PEK", "Beijing", "CN", 116.6, 40.1),
            sample_airport("PKX", "Beijing", "CN", 116.4, 39.5),
            sample_airport("CCC", "Gamma", "CN", 121.0, 31.0),
        ]
        .into_iter()
        .map(|airport| (airport.id().as_str().to_string(), airport))
        .collect()
    }

    fn sample_airport(
        code: &str,
        city: &str,
        country: &str,
        longitude: f64,
        latitude: f64,
    ) -> Airport {
        Airport::new_full(
            AirportCode::new(code).unwrap(),
            chrono_tz::UTC,
            None,
            Some(city.to_string()),
            Some(country.to_string()),
            None,
            longitude,
            latitude,
        )
    }

    fn sample_edge(
        airports: &HashMap<String, Airport>,
        from: &str,
        to: &str,
        company: &str,
        flight_id: &str,
    ) -> RouteEdge {
        RouteEdge {
            from: from.to_string(),
            to: to.to_string(),
            companies: vec![company.to_string()],
            flights: vec![format!("{company}_{flight_id}")],
            distance: airport_distance_meters(
                airports.get(from).unwrap(),
                airports.get(to).unwrap(),
            ),
        }
    }
}
