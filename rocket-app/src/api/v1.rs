use rocket::serde::json::Json;
use rocket_db_pools::Connection;

use orm::siteinfo::SiteInfo;
use crate::PriorsDb;

#[get("/siteinfo/all")]
pub async fn all_site_info(mut db: Connection<PriorsDb>) -> Option<Json<Vec<SiteInfo>>> {
    let infos = SiteInfo::get_all_site_info(&mut *db).await.ok()?;
    return Some(Json(infos))
}