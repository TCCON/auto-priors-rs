// What needs tested:
//  1. Jobs are added with all processing configurations for dates with multiple processing configs
//      1a: [x] starting from an empty jobs table
//      1b: [x] starting with existing jobs for the first date(s)
//      1c: [x] starting with existing jobs for a later date (need to check what the expected behavior is)
//  2. How it is handled if a new processing configuration is added (it should
//     fill in the missing rows?)
//      2a: [x] add the alternate GEOS IT config
//      2b: [x] add the proper GEOS IT config
//  3. What should happen if a processing configuration is removed?
//      3a: [x] no existing rows removed
//  4. Actual (long test) that runs ginput and generates output tarballs
//     that I can verify.

use std::path::PathBuf;

use chrono::NaiveDate;
use itertools::Itertools;
use orm::{
    config::ProcCfgKey, multiline_sql, multiline_sql_init, stdsitejobs::StdSiteJob,
    test_utils::make_dummy_config, MySqlConn,
};

mod common;

#[derive(Debug)]
struct JobTestRow {
    site_id: Option<String>,
    date: NaiveDate,
    processing_key: String,
}

/// Verify that jobs are added for all of the days for which we have the right met files
/// when no jobs are present in the database.
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

    // To test, there we need to know the sites and the configurations.
    // I've hardcoded the expected processing configurations and dates
    // to minimize the chance that the test passes incorrectly because of an error
    // in any code I would use to determine that from the config itself.
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_keys, &rows);
}

/// Verify that jobs are added for all of the days for which we have the right met files
/// when jobs for one date at the start of the available met files are present in the database.
/// This should fill in all remaining jobs.
#[tokio::test]
async fn test_add_jobs_across_config_change_one_existing_date() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);
    multiline_sql!("sql/stdsites/add_20230530_jobs.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    StdSiteJob::update_std_site_job_table(&mut *conn, &config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &config)
        .await
        .unwrap();

    // To test, there we need to know the sites and the configurations.
    // I've hardcoded the expected processing configurations and dates
    // to minimize the chance that the test passes incorrectly because of an error
    // in any code I would use to determine that from the config itself.
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_keys, &rows);
}

/// Verify that jobs are added for all of the days for which we have the right met files
/// when jobs for multiple dates at the start of the available met files are present in the database.
/// This should fill in all remaining jobs.
#[tokio::test]
async fn test_add_jobs_across_config_change_many_existing_dates() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);
    multiline_sql!("sql/stdsites/add_20230530_jobs.sql", conn);
    multiline_sql!("sql/stdsites/add_20230531_jobs.sql", conn);
    multiline_sql!("sql/stdsites/add_20230601_jobs.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    StdSiteJob::update_std_site_job_table(&mut *conn, &config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &config)
        .await
        .unwrap();

    // To test, there we need to know the sites and the configurations.
    // I've hardcoded the expected processing configurations and dates
    // to minimize the chance that the test passes incorrectly because of an error
    // in any code I would use to determine that from the config itself.
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_keys, &rows);
}

/// Verify that jobs are added for all of the days for which we have the right met files
/// when jobs for one date in the middle of the available met files are present in the database.
/// This should fill in all remaining jobs.
#[tokio::test]
async fn test_add_jobs_across_config_change_existing_date_midway() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);
    multiline_sql!("sql/stdsites/add_20230601_jobs.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    StdSiteJob::update_std_site_job_table(&mut *conn, &config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &config)
        .await
        .unwrap();

    // To test, there we need to know the sites and the configurations.
    // I've hardcoded the expected processing configurations and dates
    // to minimize the chance that the test passes incorrectly because of an error
    // in any code I would use to determine that from the config itself.
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_keys, &rows);
}

/// Verify that if we add an alternate configuration to a period with existing standard site jobs,
/// those jobs for the alternate met are added.
#[tokio::test]
async fn test_add_alternate_config() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);

    let final_config =
        make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");
    let mut initial_config = final_config.clone();
    initial_config
        .processing_configuration
        .remove(&ProcCfgKey("altco-geosfpit".to_string()));

    StdSiteJob::update_std_site_job_table(&mut *conn, &initial_config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &initial_config)
        .await
        .unwrap();

    // First, we'll check that we only added the expected rows without the new configuration
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_initial_keys = get_expected_keys_without_proc("altco-geosfpit");
    verify_site_job_rows(&expected_sites, &expected_initial_keys, &rows);

    // Now, we act as though we added the alternate configuration in, run another instance of
    // the service action, and check that all of the final entries expected are there.
    StdSiteJob::update_std_site_job_table(&mut *conn, &final_config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &final_config)
        .await
        .unwrap();

    let rows = get_site_job_rows(&mut conn).await;
    let expected_final_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_final_keys, &rows);
}

/// Test if we add a new configuration that extends the standard site generation in time,
/// those days are correctly added.
#[tokio::test]
async fn test_add_later_config() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);

    let final_config =
        make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");
    let mut initial_config = final_config.clone();
    initial_config
        .processing_configuration
        .remove(&ProcCfgKey("std-geosit".to_string()));

    StdSiteJob::update_std_site_job_table(&mut *conn, &initial_config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &initial_config)
        .await
        .unwrap();

    // First, we'll check that we only added the expected rows without the new configuration
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_initial_keys = get_expected_keys_without_proc("std-geosit");
    verify_site_job_rows(&expected_sites, &expected_initial_keys, &rows);

    // As an extra check, the last date in the database should be May 31st
    let last_date = rows.iter().map(|r| r.date).max();
    assert_eq!(
        last_date,
        Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()),
        "Last date added without GEOS IT is incorrect"
    );

    // Now, we act as though we added the alternate configuration in, run another instance of
    // the service action, and check that all of the final entries expected are there.
    StdSiteJob::update_std_site_job_table(&mut *conn, &final_config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &final_config)
        .await
        .unwrap();

    let rows = get_site_job_rows(&mut conn).await;
    let expected_final_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_final_keys, &rows);
}

#[tokio::test]
async fn test_removed_alternate_config() {
    common::init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/stdsite_met.sql", conn);

    let initial_config =
        make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");
    let mut removed_config = initial_config.clone();
    removed_config
        .processing_configuration
        .remove(&ProcCfgKey("altco-geosfpit".to_string()));

    StdSiteJob::update_std_site_job_table(&mut *conn, &initial_config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &initial_config)
        .await
        .unwrap();

    // First, we'll check that we added all of the expected rows
    let rows = get_site_job_rows(&mut conn).await;
    let expected_sites = get_expected_sites(&mut conn).await;
    let expected_keys = get_expected_keys();
    verify_site_job_rows(&expected_sites, &expected_keys, &rows);

    // Now, we act as though we removed the alternate configuration in, run another instance of
    // the service action, and check that no rows were removed.
    StdSiteJob::update_std_site_job_table(&mut *conn, &removed_config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &removed_config)
        .await
        .unwrap();

    let rows = get_site_job_rows(&mut conn).await;
    verify_site_job_rows(&expected_sites, &expected_keys, &rows);
}

/// Helper function: get the list of unique site IDs used in the test.
async fn get_expected_sites(conn: &mut MySqlConn) -> Vec<String> {
    sqlx::query!("SELECT DISTINCT(site_id) FROM StdSiteList")
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.site_id)
        .collect_vec()
}

/// Helper function: hardcoded expected processing keys for each
/// of the dates used in these tests.
fn get_expected_keys() -> [(NaiveDate, Vec<&'static str>); 4] {
    [
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
    ]
}

fn get_expected_keys_without_proc(proc_to_remove: &str) -> [(NaiveDate, Vec<&'static str>); 4] {
    let mut expected = get_expected_keys();
    for (_, procs) in expected.iter_mut() {
        procs.retain(|p| p != &proc_to_remove);
    }
    expected
}

/// Helper function: retrieve the site jobs rows present in the database.
async fn get_site_job_rows(conn: &mut MySqlConn) -> Vec<JobTestRow> {
    sqlx::query_as!(
        JobTestRow,
        "SELECT site_id,date,processing_key FROM v_StdSiteJobs"
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

/// Helper function: verify that the job rows in the database match what we expect.
fn verify_site_job_rows(
    expected_sites: &[String],
    expected_keys: &[(NaiveDate, Vec<&str>)],
    rows: &[JobTestRow],
) {
    // This debug should print to the console if the test fails, which can be helpful
    // to diagnose why it failed.
    let rows = dbg!(rows);

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
