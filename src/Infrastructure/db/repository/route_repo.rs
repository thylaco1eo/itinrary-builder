use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::{RecordId,SurrealValue};
use serde::Deserialize;

#[derive(Debug, Deserialize,SurrealValue)]
pub struct Segment {
    pub from: RecordId,
    pub to: RecordId,
    pub companies: Vec<String>,
    pub flights: Vec<String>,
    pub distance: f64,
    pub mct: i64,
}

#[derive(Debug, Deserialize,SurrealValue)]
pub struct PathResult {
    pub airports: Vec<RecordId>,
    pub segments: Vec<Segment>,
    pub total_dist: f64,
    pub circuity: f64,
}

pub async fn find_paths(
    db: &Surreal<Any>,
    dep: &str,
    arr: &str,
    max_hops: u8,
    max_circuity: f64,
) -> surrealdb::Result<Vec<PathResult>> {
    let query = format!(
        r#"
    LET $dep = airport:{dep};
    LET $arr = airport:{arr};
    LET $dep_loc = (SELECT location FROM $dep)[0].location;
    LET $arr_loc = (SELECT location FROM $arr)[0].location;
    LET $direct_dist = geo::distance($dep_loc, $arr_loc);

    LET $paths = array::flatten([
        {hop_queries}
    ]);

    LET $filtered = array::filter($paths, |$p|
        array::last($p) = $arr
        AND array::len($p) = array::len(array::distinct($p))
    );
    RETURN array::filter(
        array::map($filtered, |$path| {{
            LET $segments = array::map(array::windows($path, 2), |$pair| {{
                LET $rid = type::record('route', string::concat(
                    string::split(<string>$pair[0], ':')[1], '_',
                    string::split(<string>$pair[1], ':')[1]
                ));
                LET $loc_a = (SELECT location FROM $pair[0])[0];
                LET $loc_b = (SELECT location FROM $pair[1])[0];
                LET $route_info = (SELECT companies, flights FROM $rid)[0];
                RETURN {{
                    from: $pair[0],
                    to: $pair[1],
                    companies: $route_info.companies,
                    flights: $route_info.flights,
                    distance: geo::distance($loc_a.location, $loc_b.location),
                    mct: loc_b.mct
                }};
            }});
            LET $total_dist = math::sum(array::map($segments, |$s| $s.distance));
            RETURN {{
                airports: $path,
                segments: $segments,
                total_dist: $total_dist,
                circuity: $total_dist / $direct_dist
            }};
        }}),
        |$r| $r.circuity <= {max_circuity}
    );
    "#,
        dep = dep,
        arr = arr,
        max_circuity = max_circuity,
        hop_queries = (1..=max_hops)
            .map(|n| format!("$dep.{{..{}+path+inclusive}}->route->airport", n))
            .collect::<Vec<_>>()
            .join(",\n        "),
    );

    let mut result = db.query(&query).await?;

    // 7条 LET + 1条 RETURN，取索引7
    let paths: Vec<PathResult> = result.take(7)?;
    Ok(paths)
}


