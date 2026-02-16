use actix_web::{put, web, HttpResponse};
use crate::structure::WebData;
use actix_multipart::{
    form::{
        MultipartForm,
        tempfile::TempFile,
    },
};
use crate::domain::flightplan;
use crate::Infrastructure::db::repository::flight_repo;
use std::io::BufRead;


#[derive(Debug, MultipartForm)]
struct UploadForm {
    #[multipart(rename = "file")]
    file: TempFile,
}


#[put("/schedule")]
pub async fn add_schedule(data: web::Data<WebData>,MultipartForm(form): MultipartForm<UploadForm>) -> Result<HttpResponse, actix_web::Error> {
    let file = form.file.file.into_file();
    let reader = std::io::BufReader::new(file);
    for line in reader.lines(){
        let line = line?; // 如果你想传播 IO 错误
        if let Some(plan) = flightplan::parse_line(&line) {
            match flight_repo::add_flights_batch(data.database(),flightplan::expand(&plan)).await{
                Ok(_) => (),
                Err(_) => ()
            }
        }
    }
    Ok(HttpResponse::Ok().finish())
}