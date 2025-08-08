use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use openssl::ssl::{SslConnector, SslMethod,SslVerifyMode};
use crate::structure::DataBase;


pub fn connect_db(db_info:&DataBase)-> Client {
    // This function connects to the database using the connection string
    // and throw an error if the connection fails.
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let connection_string = format!(
        "host={} port={} dbname={} user={} password={}",
        db_info.host(), db_info.port(), db_info.dbname(), db_info.username(), db_info.password()
    );
    Client::connect(&connection_string, connector).expect("Failed to connect to the database")
}

pub fn check_db_status(client: &mut Client) {
    //This function checks if the ITINBUILDER schema exists in the database
    //and creates it if it does not exist.
    let query = client.query(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'ITINBUILDER'
        )", &[]
    ).expect("Failed to execute query");
    println!("Database status: {:?}", query);
    if query.is_empty()|| query[0].get::<_, bool>(0) == false {
        client.execute(
            "CREATE SCHEMA ITINBUILDER", &[]
        ).expect("Failed to create ITINBUILDER schema");
        println!("ITINBUILDER schema created");
    } else {
        println!("ITINBUILDER schema already exists");
    }
}