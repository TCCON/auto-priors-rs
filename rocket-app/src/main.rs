#[macro_use] extern crate rocket;

use rocket::response::content::RawHtml;
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::sqlx;

use orm::siteinfo;

mod api;

#[derive(Database)]
#[database("tccon_priors")]
pub struct PriorsDb(sqlx::MySqlPool);

#[get("/")]
fn index() -> RawHtml<&'static str> {
    RawHtml(r#"Try going to /siteinfo/XX, where XX is a site id."#)
}

#[get("/siteinfo/<id>")]
async fn get_siteinfo(mut db: Connection<PriorsDb>, id: String) -> Option<String> {
    let info = siteinfo::SiteInfo::get_most_recent_site_location(&mut *db, &id).await.ok()?;
    let site = info.get_std_site(&mut *db).await.ok()?;
    return Some(format!("Site: {:#?}\n\n Site info: {:#?}", site, info));
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![index, get_siteinfo])
        .mount("/api/v1", routes![api::v1::all_site_info])
        .attach(PriorsDb::init())
}