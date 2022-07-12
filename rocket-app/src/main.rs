#[macro_use] extern crate rocket;

use rocket::fs::{FileServer,relative};
use rocket::response::content::RawHtml;
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::sqlx;
use rocket_dyn_templates::{Template,context};

use orm::siteinfo;

mod utils;
mod nav;
mod api;
mod jobs;
mod stdsites;

#[derive(Database)]
#[database("tccon_priors")]
pub struct PriorsDb(sqlx::MySqlPool);


#[get("/")]
fn index() -> Template {
    Template::render("index", context!{title: "Home"})
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
        .mount("/", routes![index, get_siteinfo, jobs::check_jobs, jobs::submit_job, stdsites::check_std_sites])
        .mount("/api/v1", routes![api::v1::all_site_info])
        .mount("/static", FileServer::from(relative!("static/")))
        .attach(PriorsDb::init())
        .attach(Template::custom(|engines| {
            engines.tera.register_function("nav_url_for", nav::NavBarUrls::new());
            engines.tera.register_function("static", nav::StaticUrls::new("/static"));
        }))
}