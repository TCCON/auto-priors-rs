#[macro_use] extern crate rocket;

use chrono::NaiveDate;
use rocket::response::content::RawHtml;
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::sqlx::{self};
use rocket_dyn_templates::Template;

use orm::siteinfo;

#[derive(Database)]
#[database("tccon_priors")]
struct PriorsDb(sqlx::MySqlPool);

#[get("/")]
fn index() -> RawHtml<&'static str> {
    RawHtml(r#"Try going to /siteinfo/XX, where XX is a site id."#)
}

#[get("/siteinfo/<id>")]
async fn get_siteinfo(mut db: Connection<PriorsDb>, id: String) -> Option<String> {
    let info = siteinfo::get_most_recent_site_location(&mut *db, &id).await.ok()?;
    let site = info.get_std_site(&mut *db).await.ok()?;
    return Some(format!("Site: {:#?}\n\n Site info: {:#?}", site, info));
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![index, get_siteinfo])
        .attach(PriorsDb::init())
}