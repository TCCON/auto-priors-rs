#[macro_use] extern crate rocket;

mod tera;
mod orm;

use chrono::NaiveDate;
use rocket::response::content::RawHtml;
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::sqlx::{self};
use rocket_dyn_templates::Template;

#[get("/")]
fn index() -> RawHtml<&'static str> {
    RawHtml(r#"See <a href="tera">Tera</a> or ha ha just see that."#)
}

#[get("/<id>")]
async fn read(mut db: Connection<orm::PriorsDb>, id: i64) -> Option<String> {
    if let Some(ssj) = orm::StdSiteJob::get_by_id(db, id).await {
        return Some(format!("StdSiteJob with id {id} = {ssj:?}"));
    }else{
        return None
    }
}

#[get("/daterange")]
async fn read_daterange(mut db: Connection<orm::PriorsDb>) -> String {
    let start_date = NaiveDate::from_ymd(2010, 1, 1);
    let end_date = NaiveDate::from_ymd(2010, 1, 2);
    let ssjs = if let Some(x) = orm::StdSiteJob::get_by_date_range(db, start_date, end_date).await {
        x
    }else{
        return "No std sites found".to_owned()
    };

    return ssjs.iter().map(|x| format!("{x}\n")).collect();
}

#[get("/paths")]
async fn read_all(mut db: Connection<orm::PriorsDb>) -> String {
    let result = sqlx::query("SELECT * FROM GeosPaths;").fetch_one(&mut *db).await;
    let row = match result {
        Ok(r) => r,
        Err(e) => {return format!("Errored on query: {e}");}
    };

    return format!("{:#?}", row);
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![index, read, read_all, read_daterange])
        .mount("/tera", routes![tera::index, tera::hello, tera::about])
        .register("/tera", catchers![tera::not_found])
        .attach(Template::custom(|engines| {
            tera::customize(&mut engines.tera);
        }))
        .attach(orm::PriorsDb::init())
}
