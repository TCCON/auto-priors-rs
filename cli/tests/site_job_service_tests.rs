// What needs tested:
//  1. Jobs are added with all processing configurations for dates with multiple processing configs
//      1a: [x] starting from an empty jobs table
//      1b: [ ] starting with existing jobs for the first date(s)
//      1c: [ ] starting with existing jobs for a later date (need to check what the expected behavior is)
//  2. How it is handled if a new processing configuration is added (it should
//     fill in the missing rows?)
//  3. What should happen if a processing configuration is removed?
//  4. Actual (long test) that runs ginput and generates output tarballs
//     that I can verify.

use std::path::PathBuf;

use chrono::NaiveDate;
use itertools::Itertools;
use orm::{
    multiline_sql, multiline_sql_init, stdsitejobs::StdSiteJob, test_utils::make_dummy_config,
};

mod common;

/// Verify that
#[tokio::test]
async fn test_add_jobs_across_config_change() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    StdSiteJob::update_std_site_job_table(&mut *conn, &config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &config)
        .await
        .unwrap();

    let rows = sqlx::query!("SELECT * FROM v_StdSiteJobs")
        .fetch_all(&mut *conn)
        .await
        .unwrap();

    // To test, there we need to know the sites and the configurations.
    // I've hardcoded the expected processing configurations and dates
    // to minimize the chance that the test passes incorrectly because of an error
    // in any code I would use to determine that from the config itself.
    let expected_sites = sqlx::query!("SELECT DISTINCT(site_id) FROM StdSiteList")
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.site_id)
        .collect_vec();

    let expected_keys = [
        (
            NaiveDate::from_ymd_opt(2023, 5, 30).unwrap(),
            vec!["std-geosfpit", "altco-geosfpit"],
        ),
        (
            NaiveDate::from_ymd_opt(2023, 5, 31).unwrap(),
            vec!["std-geosfpit", "altco-geosfpit"],
        ),
        (
            NaiveDate::from_ymd_opt(2023, 6, 1).unwrap(),
            vec!["std-geosit"],
        ),
        (
            NaiveDate::from_ymd_opt(2023, 6, 2).unwrap(),
            vec!["std-geosit"],
        ),
    ];

    // This is to check that no extra rows were added
    let n_rows_per_site = expected_keys
        .iter()
        .fold(0, |total, (_, procs)| total + procs.len());
    let n_total_rows_expected = expected_sites.len() * n_rows_per_site;
    assert_eq!(
        rows.len(),
        n_total_rows_expected,
        "Too many or too few rows were added"
    );

    // Now we check that the details of rows added are correct
    for site_id in expected_sites {
        for (date, procs) in expected_keys.iter() {
            for proc in procs {
                let found = rows.iter().any(|r| {
                    r.site_id.as_ref() == Some(&site_id)
                        && &r.date == date
                        && &r.processing_key == proc
                });

                assert!(
                    found,
                    "Did not find a row for site {site_id} on {date} for processing '{proc}'"
                );
            }
        }
    }
}
