use std::{path::PathBuf, sync::Arc};

use chrono::NaiveDate;
use glob::glob;
use itertools::Itertools;
use orm::{
    config::ProcCfgKey,
    multiline_sql, multiline_sql_init, multiline_sql_init_pool,
    stdsitejobs::StdSiteJob,
    test_utils::{init_logging, make_dummy_config},
    MySqlConn, PoolWrapper,
};
use tccon_priors_service::{jobs, stdsitejobs};
use tokio::sync::mpsc;

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
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);
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
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);
    multiline_sql!("sql/add_20230530_jobs.sql", conn);
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
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);
    multiline_sql!("sql/add_20230530_jobs.sql", conn);
    multiline_sql!("sql/add_20230531_jobs.sql", conn);
    multiline_sql!("sql/add_20230601_jobs.sql", conn);
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
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);
    multiline_sql!("sql/add_20230601_jobs.sql", conn);
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
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);

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
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);

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

/// Test that when we remove an alternate processing approach from the configuration,
/// it does not alter the jobs listed in the database.
#[tokio::test]
async fn test_removed_alternate_config() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/init_test_sites.sql");
    multiline_sql!("sql/stdsite_met.sql", conn);

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

/// Test that running standard sites with the configuration set to have the GEOS IT
/// chemistry as an alternate to the GEOS FP-IT chemistry for the first day works
/// correctly. Note that this does not actually compare the output .mod and .vmr files
/// to any expected values - that check is currently done offline.
#[tokio::test]
#[ignore = "long test to run ginput - NOTE: does not validate output correctness"]
async fn test_site_job_run_ginput() {
    init_logging();
    let config = make_ginput_test_config();
    let (pool, _test_db) = multiline_sql_init_pool!("sql/init_test_sites.sql");

    let mut conn = pool
        .get_connection()
        .await
        .expect("Should be able to get connection from the database pool");
    // Rather than hand-write SQL to insert the met files into the database, we can
    // scan for the files, since we need to have the actual files on hand to run this test.
    orm::met::rescan_met_files(
        &mut conn,
        Some(NaiveDate::from_ymd_opt(2023, 5, 30).unwrap()),
        Some(NaiveDate::from_ymd_opt(2023, 6, 3).unwrap()),
        &config,
        None,
        false,
    )
    .await
    .expect("Scanning for met files should work");

    StdSiteJob::update_std_site_job_table(&mut *conn, &config, None)
        .await
        .unwrap();
    StdSiteJob::add_jobs_for_pending_rows(&mut *conn, &config)
        .await
        .unwrap();

    let shared_config = Arc::new(tokio::sync::RwLock::new(config));

    // Because the logic to run the jobs is a bit complex, we use the manager.
    let mut job_manager = make_job_manager(pool.clone(), Arc::clone(&shared_config)).await;

    // First run the LUT regen jobs
    job_manager
        .start_jobs_entry_point()
        .await
        .expect("Starting jobs should succeed");
    job_manager.wait_for_jobs_to_finish().await;

    // Then actually run the jobs - this is what will take a while to finish.
    job_manager
        .start_jobs_entry_point()
        .await
        .expect("Starting jobs should succeed");
    job_manager.wait_for_jobs_to_finish().await;

    // The logic to make the tarballs is simple (for now), but using the manager is
    // a better end-to-end test.
    let mut site_manager = make_stdsite_manager(pool, shared_config).await;
    site_manager
        .tar_std_sites_output()
        .await
        .expect("Making standard site tarballs should succeed");
}

/// Test that input files requesting the alternate GEOS IT chm + FP-IT met correctly get
/// reduced CO and that jobs not requesting an alternate processing configuration
/// correctly get the default met+chm for each date. This runs ginput, so it will
/// take some time. Note that this does not automatically verify that the output
/// matches (yet), use the plotting script in the top `testing` directory to
/// check the output manually.
#[tokio::test]
#[ignore = "long test to run ginput - NOTE: does not validate output correctness"]
async fn test_request_job_run_ginput() {
    init_logging();
    let mut config = make_ginput_test_config();
    // We need to keep the temporary directory objects; once they're dropped, the directories are deleted.
    let (_tmp_in_dir, _tmp_parsed_dir) = setup_test_input_files(&mut config);

    // We don't actually need the test sites, but this is a convenient way to initialize the pool.
    let (pool, _test_db) = multiline_sql_init_pool!("sql/init_test_sites.sql");

    let mut conn = pool
        .get_connection()
        .await
        .expect("Should be able to get connection from the database pool");
    // Rather than hand-write SQL to insert the met files into the database, we can
    // scan for the files, since we need to have the actual files on hand to run this test.
    orm::met::rescan_met_files(
        &mut conn,
        Some(NaiveDate::from_ymd_opt(2023, 5, 30).unwrap()),
        Some(NaiveDate::from_ymd_opt(2023, 6, 3).unwrap()),
        &config,
        None,
        false,
    )
    .await
    .expect("Scanning for met files should work");

    let shared_config = Arc::new(tokio::sync::RwLock::new(config));

    // We'll use the manager to mimic the real behavior
    let mut job_manager = make_job_manager(pool.clone(), Arc::clone(&shared_config)).await;

    // First run the LUT regen jobs
    job_manager
        .start_jobs_entry_point()
        .await
        .expect("Starting jobs should succeed");
    job_manager.wait_for_jobs_to_finish().await;

    // Then actually run the jobs - this is what will take a while to finish.
    job_manager
        .start_jobs_entry_point()
        .await
        .expect("Starting jobs should succeed");
    job_manager.wait_for_jobs_to_finish().await;

    // Unlike the standard site runs, this should be all we need to do. There should now be two jobs in the
    // testing/output directory.
}

fn get_ginput_testing_dir() -> PathBuf {
    // Get the workspace root. The manifest dir points to the actual package (`cli`, because
    // that's where I've been writing the tests), so we want the parent.
    let crate_root_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("expected CARGO_MANIFEST_DIR to have a parent")
        .to_path_buf();
    crate_root_dir.join("testing").join("ginput-tests")
}

fn make_ginput_test_config() -> orm::config::Config {
    let mut config =
        make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");
    let testing_dir = get_ginput_testing_dir();
    // Override the paths for the GEOS files, the ginput script locations, and the
    // output locations. Unlike many other tests, we want the results from this test
    // to persist so we can review them.
    for (key, met) in config.data.met_download.iter_mut() {
        match met.ginput_met_type {
            orm::met::GinputMetType::ChemEta => {
                let subdir = if key.0.starts_with("geosit") {
                    "chm-it"
                } else {
                    "chm-fpit"
                };
                met.download_dir = testing_dir.join(subdir).join("Nv");
            }
            orm::met::GinputMetType::MetEta => {
                met.download_dir = testing_dir.join("met").join("Nv");
            }
            orm::met::GinputMetType::Met2D => {
                met.download_dir = testing_dir.join("met").join("Nx");
            }
            orm::met::GinputMetType::Other => {
                unimplemented!("'Other' met type for ginput testing configuration")
            }
        }
    }

    for ginput in config.execution.ginput.values_mut() {
        match ginput {
            orm::config::GinputConfig::Script { entry_point_path } => {
                *entry_point_path = testing_dir.join("py-ginput").join("run_ginput.py")
            }
        }
    }

    config
        .processing_configuration
        .get_mut(&ProcCfgKey("std-geosfpit".to_string()))
        .unwrap()
        .auto_tarball_dir = Some(testing_dir.join("std-tarballs"));
    config
        .processing_configuration
        .get_mut(&ProcCfgKey("std-geosit".to_string()))
        .unwrap()
        .auto_tarball_dir = Some(testing_dir.join("std-tarballs"));
    config
        .processing_configuration
        .get_mut(&ProcCfgKey("altco-geosfpit".to_string()))
        .unwrap()
        .auto_tarball_dir = Some(testing_dir.join("alt-tarballs"));
    config.execution.std_sites_output_base = testing_dir.join("work");

    // This will be useful for the tests using request files
    config.execution.output_path = testing_dir.join("output");

    // Also need to point to the real base .vmr file and levels file
    config.data.base_vmr_file = Some(testing_dir.join("summer_35N.vmr"));
    config.data.zgrid_file = Some(testing_dir.join("ap_51_level_0_to_70km.gnd"));

    config
}

fn setup_test_input_files(
    config: &mut orm::config::Config,
) -> (tempdir::TempDir, tempdir::TempDir) {
    let testing_dir = get_ginput_testing_dir();
    let test_input_files = [
        testing_dir.join("input-files").join("alt_met.txt"),
        testing_dir.join("input-files").join("std_met.txt"),
    ];

    let tmp_input_dir = tempdir::TempDir::new_in(&testing_dir, "tmp-input-files")
        .expect("Should be able to create temporary input file directory");
    let tmp_parsed_dir = tempdir::TempDir::new_in(&testing_dir, "tmp-input-files-parsed")
        .expect("Should be able to create temporary parsed input file directory");

    for src in test_input_files {
        let fname = src.file_name().expect("input file should have a file name");
        let dest = tmp_input_dir.path().join(fname);
        std::fs::copy(&src, &dest).expect("Should be able to copy input file");
    }

    let input_pattern = tmp_input_dir
        .path()
        .join("*.txt")
        .to_string_lossy()
        .to_string();
    config.execution.input_file_pattern = input_pattern;
    config.execution.success_input_file_dir = tmp_parsed_dir.path().to_path_buf();
    config.execution.failure_input_file_dir = tmp_parsed_dir.path().to_path_buf();

    (tmp_input_dir, tmp_parsed_dir)
}

async fn make_job_manager(
    pool: PoolWrapper,
    shared_config: Arc<tokio::sync::RwLock<orm::config::Config>>,
) -> jobs::JobManager<jobs::ServiceJobRunner> {
    let (_, rx) = mpsc::channel::<jobs::JobMessage>(256);
    let error_handler = tccon_priors_service::error::ErrorHandler::Logging(
        tccon_priors_service::error::LoggingErrorHandler {},
    );
    let jobs_manager = jobs::JobManager::<jobs::ServiceJobRunner>::new_from_pool(
        pool,
        shared_config,
        error_handler,
        rx,
    )
    .await
    .expect("Creating the jobs manager should succeed");

    jobs_manager
}

async fn make_stdsite_manager(
    pool: PoolWrapper,
    shared_config: Arc<tokio::sync::RwLock<orm::config::Config>>,
) -> stdsitejobs::StdSiteManager {
    let (_, rx) = mpsc::channel::<stdsitejobs::StdSiteMessage>(256);
    let error_handler = tccon_priors_service::error::ErrorHandler::Logging(
        tccon_priors_service::error::LoggingErrorHandler {},
    );
    stdsitejobs::StdSiteManager::new_with_pool(pool, shared_config, error_handler, rx).await
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
