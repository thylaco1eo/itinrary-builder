extern crate chrono;
mod flight_info;
pub mod services;

fn main() {
    //let mut path = String::new();
    //println!("Please enter the path to the flight data file:");
    //io::stdin().read_line(&mut path).expect("Failed to read line");
    let dpt_apt: std::collections::HashMap<String, Vec<flight_info::FlightInfo>> = services::data_service::import_sch::import_schedule_file("./data/cassim0401");
    let request = "PEKFRA01MAY25+0800";
    let path_list = services::search_service::search_flight::search_flight(&dpt_apt, request);
    if path_list.is_empty() {
        println!("No flights found");
    } else {
        for path in path_list {
            println!("Found path:");
            for (flt_id, dep_time, arr_station, flight_time) in path {
                println!("Flight ID: {}, Departure Time: {}, Arrival Station: {}, Flight Time: {} minutes", flt_id, dep_time, arr_station, flight_time);
            }
        }
    }
}
