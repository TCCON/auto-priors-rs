use std::collections::HashMap;
use chrono::{Utc,Duration, NaiveDate};
use log::warn;
use rocket_db_pools::Connection;
use rocket_dyn_templates::{Template,context};
use orm::{geos,siteinfo,stdsitejobs,utils};

use crate::PriorsDb;

#[get("/stdsites")]
pub async fn check_std_sites(mut db: Connection<PriorsDb>) -> Result<Template, String> {
    let cfg = orm::config::load_config_file_or_default::<std::path::PathBuf>(None) // TODO: maybe the configuration should be a resource?
        .expect("Could not load default configuration file");
    let today = Utc::today().naive_utc();
    let start_date = (Utc::today() - Duration::days(14)).naive_utc();
    let dates = utils::date_range(start_date, today + Duration::days(1));

    let site_ids = siteinfo::StdSite::get_site_ids(&mut *db, None)
        .await
        .expect("Standard site database query failure!");

    let jobs = stdsitejobs::StdSiteJob::get_std_site_availability(
        &mut *db, start_date, Some(today), None
    ).await
    .expect("Standard site job database query failure!");

    let last_geos_date = geos::GeosFile::get_last_complete_date_for_default_mets(
        &mut *db, &cfg).await
        .expect("Geos file database query failure!")
        .unwrap_or(NaiveDate::from_ymd(1970, 1, 1));

    let mut table = HashMap::new();
    for sid in site_ids.iter() {
        let mut inner_table = HashMap::new();
        for date in dates.iter() {
            if date <= &last_geos_date {
                inner_table.insert(date, stdsitejobs::StdSiteJobState::MissingGeosPresent);
            } else {
                inner_table.insert(date, stdsitejobs::StdSiteJobState::MissingGeosUnavailable);
            }
        }
        table.insert(sid, inner_table);
    }

    for job in jobs.iter() {
        let inner_table = if let Some(el) = table.get_mut(&job.site_id) {
            el
        }else{
            warn!("While building the standard site jobs table, one of the jobs had a site ID '{}' not present in the outer map", &job.site_id);
            continue;
        };

        inner_table.insert(&job.date, job.state);
    }

    return Ok(Template::render("std-site-table", context!{
        title: "Standard sites",
        dates: dates.clone(),
        sites: site_ids.clone(),
        availability: table
    }))
}