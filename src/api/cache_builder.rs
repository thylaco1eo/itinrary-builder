use std::collections::{HashMap, HashSet};

use crate::domain::flight::Flightcore;
use crate::domain::mct::{AirportMctData, GlobalMctData};
use crate::memory::core::{flight_storage_key, WebData};
use crate::Infrastructure::db::repository::route_repo::find_paths_from_route_edges;

use super::ib::{build_itineraries_for_path, EffectiveMct, MctCacheKey};

const MAX_CIRCUITY: f64 = 2.5;

pub fn build_itin_cache(data: &WebData) {
    let hot_ods = data.hot_ods();
    if hot_ods.is_empty() {
        println!("No hot ODs configured, skipping itinerary cache build.");
        return;
    }

    let flights = data.flights();
    let airports = data.airports();
    let airport_mct = data.airport_mct();
    let global_mct = data.global_mct();
    let route_edges = data.route_edges();

    let dep_dates = collect_dep_dates(&flights);
    println!(
        "Building itinerary cache for {} hot ODs across {} departure dates...",
        hot_ods.len(),
        dep_dates.len()
    );

    let mut cache: HashMap<(String, String, String), Vec<Vec<String>>> = HashMap::new();
    let hot_ods_vec: Vec<_> = hot_ods.iter().cloned().collect();

    for (od_index, (ref origin, ref dest)) in hot_ods_vec.iter().enumerate() {
        for dep_date in &dep_dates {
            let paths = find_paths_from_route_edges(
                &route_edges,
                &airports,
                origin,
                dest,
                3, // max_hops for ≤2 stops
                MAX_CIRCUITY,
            );

            let mut od_combinations: Vec<Vec<String>> = Vec::new();
            for (path_index, path) in paths.iter().enumerate() {
                let mut mct_cache: HashMap<MctCacheKey, EffectiveMct> = HashMap::new();
                let results = build_itineraries_for_path(
                    path,
                    &flights,
                    &airports,
                    &airport_mct,
                    &global_mct,
                    *dep_date,
                    "",
                    path_index,
                    &mut mct_cache,
                );
                for combo in results {
                    let keys: Vec<String> = combo
                        .iter()
                        .map(|f| flight_storage_key(
                            f.company(),
                            f.flight_id(),
                            f.origin().as_str(),
                            f.destination().as_str(),
                            f.dep_local().date_naive(),
                        ))
                        .collect();
                    od_combinations.push(keys);
                }
            }

            let date_str = dep_date.format("%Y-%m-%d").to_string();
            cache.insert((origin.clone(), dest.clone(), date_str), od_combinations);
        }

        println!(
            "  [{}/{}] built cache for {} -> {}",
            od_index + 1,
            hot_ods_vec.len(),
            origin,
            dest
        );
    }

    *data.itin_cache.write().unwrap() = cache;
    println!("Itinerary cache build complete. {} entries cached.", data.itin_cache.read().unwrap().len());
}

pub fn build_itin_cache_for_od(data: &WebData, origin: &str, destination: &str) {
    let flights = data.flights();
    let airports = data.airports();
    let airport_mct = data.airport_mct();
    let global_mct = data.global_mct();
    let route_edges = data.route_edges();

    let dep_dates = collect_dep_dates(&flights);
    let origin = origin.to_uppercase();
    let destination = destination.to_uppercase();

    let mut cache = data.itin_cache.write().unwrap();
    cache.retain(|(o, d, _), _| o != &origin || d != &destination);

    for dep_date in &dep_dates {
        let paths = find_paths_from_route_edges(
            &route_edges,
            &airports,
            &origin,
            &destination,
            3,
            MAX_CIRCUITY,
        );

        let mut od_combinations: Vec<Vec<String>> = Vec::new();
        for (path_index, path) in paths.iter().enumerate() {
            let mut mct_cache: HashMap<MctCacheKey, EffectiveMct> = HashMap::new();
            let results = build_itineraries_for_path(
                path,
                &flights,
                &airports,
                &airport_mct,
                &global_mct,
                *dep_date,
                "",
                path_index,
                &mut mct_cache,
            );
            for combo in results {
                let keys: Vec<String> = combo
                    .iter()
                    .map(|f| flight_storage_key(
                        f.company(),
                        f.flight_id(),
                        f.origin().as_str(),
                        f.destination().as_str(),
                        f.dep_local().date_naive(),
                    ))
                    .collect();
                od_combinations.push(keys);
            }
        }

        let date_str = dep_date.format("%Y-%m-%d").to_string();
        cache.insert((origin.clone(), destination.clone(), date_str), od_combinations);
    }

    println!(
        "Cache rebuilt for {} -> {}: {} date entries.",
        origin,
        destination,
        dep_dates.len()
    );
}

fn collect_dep_dates(flights: &HashMap<String, Flightcore>) -> Vec<chrono::NaiveDate> {
    let dates: HashSet<chrono::NaiveDate> = flights
        .values()
        .map(|f| f.dep_local().date_naive())
        .collect();
    let mut dates: Vec<_> = dates.into_iter().collect();
    dates.sort();
    dates
}
