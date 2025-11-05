use std::{ffi::OsStr, path::PathBuf};

use chrono::NaiveDate;
use float_cmp::approx_eq;
use itertools::Itertools;
use orm::{
    config::Config,
    input_files,
    jobs::{Job, MapFmt, ModFmt, TarChoice, VmrFmt},
    siteinfo::{SiteInfo, SiteType, StdSite},
    test_utils::{make_dummy_config_with_temp_dirs, open_test_database},
    MySqlConn,
};
use tccon_priors_cli::met_download;

mod common;

/// Test that all input files in the "should_pass" subdirectory create jobs in the database.
///
/// Each file under "should_pass" must have a corresponding match arm in [`get_expected_job`]
/// that returns the vector of jobs that should be added.
#[test_log::test(tokio::test)]
async fn test_successful_input_files() {
    // We will programmatically add a month of met to the database, so call the pool function directly
    // rather than use an SQL file.
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Could not open database");
    let (config, _tmp_dir) =
        make_dummy_config_with_temp_dirs("priors-test").expect("Failed to make test configuration");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Could not get database connection from pool");

    let email_backend = if let orm::config::EmailBackend::Testing(backend) = &config.email.backend {
        backend
    } else {
        panic!("This test requires the Testing email backend be configured");
    };

    populate_met_in_db(&mut conn, &config).await;
    populate_standard_sites_in_db(&mut conn, &config).await;

    // For each job, add it to the database, then query out what was added and ensure
    // that the correct job was created.
    let test_input_files = list_test_input_files("should_pass");
    let mut file_mover = input_files::InputFileCleanupHandler::new_for_testing();
    let mut prev_n_jobs_in_db = 0;
    for file in test_input_files {
        log::info!("Adding job for input file {}", file.display());
        input_files::add_jobs_from_input_files(
            &mut conn,
            &config,
            &[file.clone()],
            &mut file_mover,
        )
        .await
        .expect("Adding job from input file should not error");

        let mut db_jobs = Job::get_jobs_list(&mut conn, false)
            .await
            .expect("Should be able to query jobs");

        // First confirm that the expected number of jobs were added
        let expected = get_expected_job(file.file_name().unwrap());
        let n_new_jobs = db_jobs.len() - prev_n_jobs_in_db;
        assert_eq!(
            n_new_jobs,
            expected.len(),
            "Expected {} new jobs after reading file {}, got {}: {:#?}",
            expected.len(),
            file.display(),
            n_new_jobs,
            &db_jobs[prev_n_jobs_in_db..]
        );

        // Then check that each job is as expected. Ensure that the jobs are in ID order
        // so that we take the most recent ones.
        db_jobs.sort_by_key(|j| j.job_id);
        let new_db_jobs = &db_jobs[prev_n_jobs_in_db..];
        for (i_job, (db_job, exp_job)) in new_db_jobs.into_iter().zip(expected.iter()).enumerate() {
            assert!(
                exp_job.matches_job(db_job),
                "Job {}/{} for input file {} does not match expected. {db_job:#?}\n\n{exp_job:#?}",
                i_job + 1,
                new_db_jobs.len(),
                file.display()
            )
        }

        // Last check that a message was sent if it was supposed to be.
        if expected[0].confirmation {
            assert_eq!(
                email_backend.num_messages(),
                1,
                "One email should be sent for all jobs"
            );
            email_backend.clear();
        } else {
            assert_eq!(
                email_backend.num_messages(),
                0,
                "Email was sent despite input file having confirmation=false"
            );
        }

        // TODO: confirm that a file was moved into the appropriate directory?

        prev_n_jobs_in_db = db_jobs.len();
    }
}

/// Test that all input files under the "should_fail" subdirectory do not add jobs and send the right response to the user.
///
/// Each file in "should_fail" must have a corresponding match arm in [`get_expected_error_list`] which returns the
/// list of reasons that job failed. They need not be in the same order, but all the expected reasons must be
/// in the list part of the email for the test to pass. Input files can also be expected to not send an email if e.g.
/// the user's email could not be determined.
#[test_log::test(tokio::test)]
async fn test_failed_input_files() {
    // We will programmatically add a month of met to the database, so call the pool function directly
    // rather than use an SQL file.
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Could not open database");
    let (config, _tmp_dir) =
        make_dummy_config_with_temp_dirs("priors-test").expect("Failed to make test configuration");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Could not get database connection from pool");

    let email_backend = if let orm::config::EmailBackend::Testing(backend) = &config.email.backend {
        backend
    } else {
        panic!("This test requires the Testing email backend be configured");
    };

    populate_met_in_db(&mut conn, &config).await;
    populate_standard_sites_in_db(&mut conn, &config).await;

    // For each job, add it to the database, then query out what was added and ensure
    // that no job was created.
    let test_input_files = list_test_input_files("should_fail");
    let mut file_mover = input_files::InputFileCleanupHandler::new_for_testing();

    for file in test_input_files {
        log::info!("Adding job for input file {}", file.display());
        input_files::add_jobs_from_input_files(
            &mut conn,
            &config,
            &[file.clone()],
            &mut file_mover,
        )
        .await
        .expect("Adding job from input file should not error");

        // Confirm that no job was added
        let db_jobs = Job::get_jobs_list(&mut conn, false)
            .await
            .expect("Should be able to query jobs");
        assert_eq!(
            db_jobs.len(),
            0,
            "Job(s) incorrectly added for {}",
            file.file_name().unwrap_or_default().to_string_lossy()
        );

        // Confirm that an email with the expected content was sent or that if one was not sent,
        // that was the expected behavior.
        assert!(
            email_backend.num_messages() <= 1,
            "Should only send one email for a failed job"
        );
        let last_msg = email_backend.pop_front();
        let expected_reasons = get_expected_error_list(file.file_name().unwrap());
        match (last_msg, expected_reasons) {
            (None, None) => {}
            (Some(msg), Some(reasons)) => {
                let missing_reasons = check_error_reasons_in_email(&msg, &reasons);
                assert!(
                    missing_reasons.is_empty(),
                    "Could not find {} of the expected reasons {} was not processed in the email. (Missing reasons: {:?}, message body follows)\n{}",
                    missing_reasons.len(),
                    file.file_name().unwrap().to_string_lossy(),
                    missing_reasons,
                    msg.message
                )
            }
            (Some(_), None) => assert!(
                false,
                "Expected no email sent for {}, but one was",
                file.file_name().unwrap().to_string_lossy()
            ),
            (None, Some(_)) => assert!(
                false,
                "Expected an email sent for {}, but no email was",
                file.file_name().unwrap().to_string_lossy()
            ),
        }
    }
}

/// Test that all input files under "blacklisted" are correctly rejected by the system.
///
/// Each input file in "blacklisted" must have a corresponding match arm in [`get_blacklist_expectation`]
/// that returns whether an email should be sent for that input file and the message that would be given
/// if so.
#[test_log::test(tokio::test)]
async fn test_blacklisted_input_files() {
    // We will programmatically add a year of met to the database, so call the pool function directly
    // rather than use an SQL file.
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Could not open database");
    let (config, _tmp_dir) =
        make_dummy_config_with_temp_dirs("priors-test").expect("Failed to make test configuration");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Could not get database connection from pool");

    let email_backend = if let orm::config::EmailBackend::Testing(backend) = &config.email.backend {
        backend
    } else {
        panic!("This test requires the Testing email backend be configured");
    };

    populate_met_in_db(&mut conn, &config).await;
    populate_standard_sites_in_db(&mut conn, &config).await;

    // For each job, add it to the database, then query out what was added and ensure
    // that no job was created.
    let test_input_files = list_test_input_files("blacklisted");
    let mut file_mover = input_files::InputFileCleanupHandler::new_for_testing();

    for file in test_input_files {
        log::info!("Adding job for input file {}", file.display());
        input_files::add_jobs_from_input_files(
            &mut conn,
            &config,
            &[file.clone()],
            &mut file_mover,
        )
        .await
        .expect("Adding job from input file should not error");

        // Confirm that no job was added
        let db_jobs = Job::get_jobs_list(&mut conn, false)
            .await
            .expect("Should be able to query jobs");
        assert_eq!(
            db_jobs.len(),
            0,
            "Job(s) incorrectly added for {}",
            file.file_name().unwrap_or_default().to_string_lossy()
        );

        // Confirm that an email with the expected content was sent or that if one was not sent,
        // that was the expected behavior.
        assert!(
            email_backend.num_messages() <= 1,
            "Should only send one email for a blacklisted user's input file"
        );
        let file_name = file.file_name().unwrap();
        let last_msg = email_backend.pop_front();
        let (email_expected, expected_message) = get_blacklist_expectation(file_name);

        let file_name = file_name.to_string_lossy();
        match (email_expected, last_msg) {
            (false, None) => {}
            (true, None) => assert!(
                false,
                "An email should have been sent for {file_name} but was not."
            ),
            (false, Some(_)) => assert!(
                false,
                "An email should not have been sent for {file_name} but was."
            ),
            (true, Some(sent_msg)) => {
                assert_eq!(sent_msg.message, expected_message, "The email sent for blacklisted file {file_name} did not match what was expected.")
            }
        }
    }
}

/// Add dummy met files to the database.
///
/// This will create January 2018 GEOS FP-IT and GEOS IT fake data plus 30 & 31 May 2023 FP-IT and 1 & 2 June
/// IT data. I usually use 2018 because it was one of the first years with GEOS IT data available so at this
/// point it's just habit. Using 1 June 2023 as the test date for a met transition is because June 2023 is when
/// I started developing that capability, so I chose the most recent month as the transition date.
async fn populate_met_in_db(conn: &mut MySqlConn, config: &Config) {
    // All tests will have 2018 GEOS FP-IT and GEOS IT data available. I usually use 2018 because it was
    // one of the first years with GEOS IT data available so at this point it's just habit.
    log::info!("Populating database with Jan 2018 met files");
    let downloader = common::TestDownloader::new();

    met_download::download_files_for_dates(
        conn,
        &common::test_geosfpit_met_keys().iter().collect_vec(),
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2018, 2, 1).unwrap()),
        &config,
        downloader.clone(),
        false,
    )
    .await
    .expect("'Downloading' Jan 2018 GEOS FP-IT files did not complete successfully");

    met_download::download_files_for_dates(
        conn,
        &common::test_geosit_met_keys().iter().collect_vec(),
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2018, 2, 1).unwrap()),
        &config,
        downloader.clone(),
        false,
    )
    .await
    .expect("'Downloading' Jan 2018 GEOS IT files did not complete successfully");

    log::info!("Populating database with late May/early June 2023 transition met files");
    met_download::download_files_for_dates(
        conn,
        &common::test_geosfpit_met_keys().iter().collect_vec(),
        NaiveDate::from_ymd_opt(2023, 5, 30).unwrap(),
        Some(NaiveDate::from_ymd_opt(2023, 6, 1).unwrap()),
        &config,
        downloader.clone(),
        false,
    )
    .await
    .expect("'Downloading' May 2023 GEOS FP-IT files did not complete successfully");

    met_download::download_files_for_dates(
        conn,
        &common::test_geosit_met_keys().iter().collect_vec(),
        NaiveDate::from_ymd_opt(2023, 6, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2023, 6, 3).unwrap()),
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' June 2023 GEOS IT files did not complete successfully");
}

/// Create Caltech (ci), Lamont (oc), and Park Falls (pa) standard sites in the database.
///
/// This is necessary to test input files that don't provide a lat/lon. It creates entries
/// in both the StdSite and StdSiteInfo tables to mimic how the database would be populated
/// if these sites were really present.
async fn populate_standard_sites_in_db(conn: &mut MySqlConn, config: &Config) {
    log::info!("Adding standard sites to database");
    let sites = [
        (
            "ci",
            "Caltech",
            "Pasadena, CA, USA".to_string(),
            34.1362,
            -118.1269,
        ),
        (
            "oc",
            "Lamont",
            "Lamont, OK, USA".to_string(),
            36.604,
            -97.486,
        ),
        (
            "pa",
            "Park Falls",
            "Park Falls, WI, USA".to_string(),
            45.945,
            -90.273,
        ),
    ];
    // To avoid a profileration of warning messages, we limit the sites to the period that we
    // populate test met data for.
    let start_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2018, 2, 1).unwrap();
    for (sid, name, loc, lat, lon) in sites {
        StdSite::create(conn, sid, name, SiteType::TCCON)
            .await
            .expect("Should be able to add new site");

        SiteInfo::set_site_info_for_dates(
            conn,
            config,
            sid,
            start_date,
            Some(end_date),
            Some(loc),
            Some(lon),
            Some(lat),
            None,
            false,
        )
        .await
        .expect("Should be able to set location information for standard site.")
    }
}

/// Gets the list of all input files in a given subdirectory of `cli/test_input_files`.
fn list_test_input_files(subdir: &str) -> Vec<PathBuf> {
    // This should resolve to the directory containing the Cargo.toml for the
    // cli crate, not the workspace.
    let cargo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let test_dir = cargo_root.join("test_input_files").join(subdir);
    log::info!("Looking for test input files in {}", test_dir.display());
    if !test_dir.exists() {
        panic!("Test input file directory {} not found", test_dir.display());
    }
    let mut files = vec![];
    for f in std::fs::read_dir(&test_dir).expect("Could not read test input file directory") {
        let f = f.expect("Could not get test file information");
        files.push(f.path());
    }
    files
}

/// Confirm that the reasons an input file failed to be parsed matched what was expected.
fn check_error_reasons_in_email<'r>(
    msg: &orm::email::TestEmailData,
    expected_reasons: &[&'r str],
) -> Vec<&'r str> {
    let body = msg.message.as_str();

    // First, get the lines from the list of errors - they should start with '*' after whitespace
    let mut sent_reasons = vec![];
    for line in body.lines() {
        if line.trim_start().starts_with("*") {
            let this_reason = line
                .trim_start_matches(|c| char::is_whitespace(c) || c == '*')
                .trim_end();
            sent_reasons.push(this_reason);
        }
    }

    // Then figure out if we're missing any reasons
    let missing_reasons = expected_reasons
        .into_iter()
        .filter_map(|&reason| {
            if sent_reasons.contains(&reason) {
                None
            } else {
                Some(reason)
            }
        })
        .collect_vec();
    missing_reasons
}

// ---------------------------------------- //
// Define expected jobs for each input file //
// ---------------------------------------- //

/// Return the list of jobs expected to be created for a given input file.
fn get_expected_job(file_name: &OsStr) -> Vec<ExpectedJob> {
    let file_name = file_name.to_string_lossy();
    match file_name.as_ref() {
        "all_keys_reordered.txt" => {
            vec![
                ExpectedJob::new(vec!["kc"], (2018, 1, 4), (2018, 1, 11), "test@test.rs")
                    .with_lat_lon(vec![2.0], vec![-2.0])
                    .with_mod_fmt(ModFmt::Text)
                    .with_vmr_fmt(VmrFmt::None)
                    .with_map_fmt(MapFmt::Text),
            ]
        }
        "all_keys.txt" => {
            vec![
                ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 11), "test@test.com")
                    .with_lat_lon(vec![0.0], vec![0.0])
                    .with_mod_fmt(ModFmt::Text)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None),
            ]
        }
        "all_positional.txt" => {
            vec![
                ExpectedJob::new(vec!["pa"], (2018, 1, 1), (2018, 1, 31), "test@test.net")
                    .with_lat_lon(vec![45.1], vec![-90.0])
                    .with_mod_fmt(ModFmt::None)
                    .with_vmr_fmt(VmrFmt::None)
                    .with_map_fmt(MapFmt::NetCDF),
            ]
        }
        "carriage_return.txt" => {
            vec![
                ExpectedJob::new(vec!["xd"], (2018, 1, 1), (2018, 1, 2), "cr@test.net")
                    .with_lat_lon(vec![34.691], vec![-117.818])
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None)
                    .with_egi(true),
            ]
        }
        "egi1.txt" => {
            vec![ExpectedJob::new(
                vec!["jp"],
                (2018, 1, 26),
                (2018, 1, 27),
                "jhedeliu@caltech.edu",
            )
            .with_lat_lon(vec![34.180], vec![-118.121])
            .with_mod_fmt(ModFmt::Text)
            .with_vmr_fmt(VmrFmt::Text)
            .with_map_fmt(MapFmt::None)
            .with_egi(true)]
        }
        "mixed_keys_pos.txt" => {
            vec![
                ExpectedJob::new(vec!["ma"], (2018, 1, 1), (2018, 1, 31), "test@test.mix")
                    .with_lat_lon(vec![42.0], vec![-42.0])
                    .with_mod_fmt(ModFmt::None)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None),
            ]
        }
        "multi_loc_multi_ids.txt" => {
            vec![ExpectedJob::new(
                vec!["mc", "md", "me"],
                (2018, 1, 1),
                (2018, 1, 31),
                "test@test.mix",
            )
            .with_lat_lon(vec![42.0, 43.0, 44.1], vec![-42.0, -41.0, -40.0])
            .with_mod_fmt(ModFmt::None)
            .with_vmr_fmt(VmrFmt::Text)
            .with_map_fmt(MapFmt::None)]
        }
        "multi_loc_one_id.txt" => {
            vec![
                ExpectedJob::new(vec!["mb"], (2018, 1, 1), (2018, 1, 31), "test@test.mix")
                    .with_lat_lon(
                        vec![42.0, 43.0, 44.1, 45.0],
                        vec![-42.0, -41.0, -40.0, -39.9],
                    )
                    .with_mod_fmt(ModFmt::None)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None),
            ]
        }
        "req_keys.txt" => {
            vec![
                ExpectedJob::new(vec!["kb"], (2018, 1, 1), (2018, 1, 11), "test@test.io")
                    .with_lat_lon(vec![-1.0], vec![1.0]),
            ]
        }
        "req_positional.txt" => {
            vec![
                ExpectedJob::new(vec!["pb"], (2018, 1, 1), (2018, 1, 28), "test@test.org")
                    .with_lat_lon(vec![-42.0], vec![101.0]),
            ]
        }
        "short_all_keys_no_confirm.txt" => {
            vec![
                ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 5), "test@test.com")
                    .with_lat_lon(vec![0.0], vec![0.0])
                    .with_mod_fmt(ModFmt::Text)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None)
                    .with_confirmation(false),
            ]
        }
        "short_all_keys_with_met.txt" => {
            vec![
                ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 5), "test@test.com")
                    .with_lat_lon(vec![0.0], vec![0.0])
                    .with_mod_fmt(ModFmt::Text)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None)
                    .with_alt_met("altco-geosfpit"),
            ]
        }
        "short_all_keys.txt" => {
            vec![
                ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 5), "test@test.com")
                    .with_lat_lon(vec![0.0], vec![0.0])
                    .with_mod_fmt(ModFmt::Text)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None),
            ]
        }
        "short_alt_met.txt" => {
            vec![
                ExpectedJob::new(vec!["ka"], (2018, 1, 1), (2018, 1, 2), "test@test.com")
                    .with_lat_lon(vec![0.0], vec![0.0])
                    .with_alt_met("altco-geosfpit"),
            ]
        }
        "split_days.txt" => {
            vec![
                // Although its split over the transition between mets, this doesn't actually need split
                // into separate jobs; the runner calls ginput for each day.
                ExpectedJob::new(vec!["ka"], (2023, 5, 31), (2023, 6, 2), "test@test.com")
                    .with_lat_lon(vec![0.0], vec![0.0])
                    .with_mod_fmt(ModFmt::Text)
                    .with_vmr_fmt(VmrFmt::Text)
                    .with_map_fmt(MapFmt::None),
            ]
        }
        "std_sites.txt" => {
            vec![ExpectedJob::new(
                vec!["pa", "oc", "ci"],
                (2018, 1, 1),
                (2018, 1, 31),
                "test@test.mix",
            )
            .with_mod_fmt(ModFmt::None)
            .with_vmr_fmt(VmrFmt::Text)
            .with_map_fmt(MapFmt::None)]
        }
        "trailing_whitespace.txt" => {
            vec![
                ExpectedJob::new(vec!["ka"], (2018, 1, 1), (2018, 1, 3), "test@test.net")
                    .with_lat_lon(vec![49.1025], vec![8.4397]),
            ]
        }
        _ => unimplemented!("No expected job was defined for {}", file_name),
    }
}

/// Returns a list of reasons that an input file could not be added
/// which should correspond to the lines starting with '*' in the email
/// (sans the '*'). If the file should not result in an email being sent
/// (because the email could not be identified), returns `None`.
fn get_expected_error_list(file_name: &OsStr) -> Option<&'static [&'static str]> {
    let file_name = file_name.to_string_lossy();
    match file_name.as_ref() {
        "alt_met_out_of_range.txt" => {
            Some(&["Invalid reanalysis: processing configuration 'altco-geosfpit' spans dates from 2000-01-01 up to but not including 2023-06-01 but you requested dates (2023-06-02 to 2023-06-02) outside this range"])
        }
        "bad_alt_met.txt" => {
            Some(&["Invalid reanalysis: Unknown processing configuration: 'all_the_reprocessing'"])
        }
        "bad_date_fmt.txt" => {
            Some(&[
                "Line 2: input contains invalid characters",
                "Line 3: input contains invalid characters",
                "missing field start_date",
                "missing field end_date"
            ])
        }
        "bad_date_order.txt" => {
            Some(&["End date (2018-01-01) must be at least one day after the start date (2018-01-02)"])
        }
        "bad_file_fmt.txt" => {
            Some(&[
                "Line 7: Unknown ModFmt value: netcdf",
                "Line 8: Unknown VmrFmt value: nothing",
                "Line 9: Unknown MapFmt value: magic"
            ])
        }
        "blank.txt" => {
            None
        }
        "dates_out_of_range.txt" => {
            Some(&["Your request could not be fulfilled: met data was unavailable for 1 of the dates requested: 2019-07-01. If you believe this should not be the case, contact the GGG priors automation administrators."])
        }
        "dates_same.txt" => {
            Some(&["End date (2018-01-01) must be at least one day after the start date (2018-01-01)"])
        }
        "input_file_too_many_days.txt" => {
            Some(&["Too many days requested: 31 requested but the maximum allowed is 30"])
        }
        "latlon_out_of_range.txt" => {
            Some(&[
                "Line 4: Latitudes must be between -90.0 and +90.0",
                "Line 5: Longitudes must be between -180.0 and +180.0"
            ])
        }
        "long_multi_site_id.txt" => {
            Some(&["Line 1: Cannot parse 'rich': must be a two-character site ID"])
        }
        "long_site_id.txt" => {
            Some(&["Line 1: Cannot parse 'karl': must be a two-character site ID"])
        }
        "mismatch_site_latlon_1.txt" => {
            Some(&["Inconsistent site_id/lat/lon: site_id must have length 1 or the same number of elements as lat & lon (got 2 site ID, 3 lat/lon)"])
        }
        "mismatch_site_latlon_2.txt" => {
            Some(&["Inconsistent site_id/lat/lon: site_id must have length 1 or the same number of elements as lat & lon (got 3 site ID, 1 lat/lon)"])
        }
        "missing_most_fields.txt" => {
            None
        }
        "unknown_std_site.txt" => {
            // TODO: fix the error message actually sent in the email
            Some(&["The site ID ua does not have standard lat/lons associated with it"])
        }
        "wrong_key_value.txt" => {
            Some(&[
                "Line 2: Unknown field 'start'",
                "Line 3: Unknown field 'end'",
                "missing field start_date",
                "missing field end_date"
            ])
        }

        _ => unimplemented!("No expected error defined for test input file {file_name}.")
    }
}

/// Return whether an email should be sent for a blacklisted user and the expected message body.
fn get_blacklist_expectation(file_name: &OsStr) -> (bool, String) {
    let file_name = file_name.to_string_lossy();
    let (email_sent, reason) = match file_name.as_ref() {
        "no_reason_bl.txt" => (true, None),
        "silent_bl.txt" => (false, None),
        "with_reason_bl.txt" => (true, Some("don't request a grid of priors")),
        _ => unimplemented!("No expected error defined for test input file {file_name}."),
    };

    if let Some(r) = reason {
        (email_sent, format!("Your priors request input file '{file_name}' has been rejected; further requests will NOT be accepted. Reason: {r}."))
    } else {
        (email_sent, format!("Your priors request input file '{file_name}' has been rejected; further requests will NOT be accepted."))
    }
}

#[derive(Debug)]
struct ExpectedJob {
    site_ids: Vec<&'static str>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lat: Vec<Option<f32>>,
    lon: Vec<Option<f32>>,
    email: &'static str,
    mod_fmt: ModFmt,
    vmr_fmt: VmrFmt,
    map_fmt: MapFmt,
    alt_reanalysis: Option<&'static str>,
    is_egi: bool,
    confirmation: bool,
}

impl ExpectedJob {
    fn new(
        site_ids: Vec<&'static str>,
        start_date: (i32, u32, u32),
        end_date: (i32, u32, u32),
        email: &'static str,
    ) -> Self {
        let start_date = NaiveDate::from_ymd_opt(start_date.0, start_date.1, start_date.2).unwrap();
        let end_date = NaiveDate::from_ymd_opt(end_date.0, end_date.1, end_date.2).unwrap();
        let coord = site_ids.iter().map(|_| None).collect_vec();
        Self {
            site_ids,
            start_date,
            end_date,
            email,
            lat: coord.clone(),
            lon: coord,
            mod_fmt: ModFmt::default(),
            vmr_fmt: VmrFmt::default(),
            map_fmt: MapFmt::default(),
            alt_reanalysis: None,
            is_egi: false,
            confirmation: true,
        }
    }

    fn with_lat_lon(mut self, lat: Vec<f32>, lon: Vec<f32>) -> Self {
        self.lat = lat.into_iter().map(|v| Some(v)).collect_vec();
        self.lon = lon.into_iter().map(|v| Some(v)).collect_vec();
        self
    }

    fn with_mod_fmt(mut self, mod_fmt: ModFmt) -> Self {
        self.mod_fmt = mod_fmt;
        self
    }

    fn with_vmr_fmt(mut self, vmr_fmt: VmrFmt) -> Self {
        self.vmr_fmt = vmr_fmt;
        self
    }

    fn with_map_fmt(mut self, map_fmt: MapFmt) -> Self {
        self.map_fmt = map_fmt;
        self
    }

    fn with_alt_met(mut self, alt_met: &'static str) -> Self {
        self.alt_reanalysis = Some(alt_met);
        self
    }

    fn with_egi(mut self, is_egi: bool) -> Self {
        self.is_egi = is_egi;
        self
    }

    fn with_confirmation(mut self, do_confirm: bool) -> Self {
        self.confirmation = do_confirm;
        self
    }

    fn matches_job(&self, db_job: &Job) -> bool {
        // Special case: user could input a single site ID for multiple locations
        if self.site_ids.len() == 1 && db_job.site_id.len() > 1 {
            if db_job.site_id.iter().any(|i| i != self.site_ids[0]) {
                log::error!("Single input site ID was not expanded properly");
                return false;
            }
        } else if self.site_ids != db_job.site_id {
            log::error!("Site IDs did not match");
            return false;
        }
        if self.start_date != db_job.start_date {
            log::error!("Start date did not match");
            return false;
        }
        if self.end_date != db_job.end_date {
            log::error!("End date did not match");
            return false;
        }
        if let Some(db_email) = db_job.email.as_deref() {
            if db_email != self.email {
                log::error!("Email did not match");
                return false;
            }
        } else {
            log::error!("Email was not recorded");
            return false;
        }
        if self.mod_fmt != db_job.mod_fmt {
            log::error!("MOD format did not match");
            return false;
        }
        if self.vmr_fmt != db_job.vmr_fmt {
            log::error!("VMR format did not match");
            return false;
        }
        if self.map_fmt != db_job.map_fmt {
            log::error!("MAP format did not match");
            return false;
        }

        if self.lat.len() != db_job.lat.len() {
            log::error!("Latitudes vectors were different lengths");
            return false;
        }
        if self.lon.len() != db_job.lon.len() {
            log::error!("Longitude vectors were different lengths");
            return false;
        }

        for (my_y, db_y) in self.lat.iter().zip(db_job.lat.iter()) {
            match (my_y, db_y) {
                (None, None) => {}
                (None, Some(_)) | (Some(_), None) => return false,
                (Some(y1), Some(y2)) => {
                    if !approx_eq!(f32, *y1, *y2, ulps = 2) {
                        log::error!("At least one lat differed: expected {y1} vs. actual {y2}");
                        return false;
                    }
                }
            }
        }

        for (my_x, db_x) in self.lon.iter().zip(db_job.lon.iter()) {
            match (my_x, db_x) {
                (None, None) => {}
                (None, Some(_)) | (Some(_), None) => return false,
                (Some(x1), Some(x2)) => {
                    if !approx_eq!(f32, *x1, *x2, ulps = 2) {
                        log::error!("At least one lon differed: expected {x1} vs. actual {x2}");
                        return false;
                    }
                }
            }
        }

        if let Some(alt_re_key) = self.alt_reanalysis {
            if Some(alt_re_key) != db_job.processing_key.as_deref() {
                log::error!("processing key (from alternate reanalysis request) did not match");
                return false;
            }
        }

        if self.is_egi {
            if db_job.save_tarball != TarChoice::Egi {
                log::error!("save_tarball expected to be \"EGI\", was not");
                return false;
            }
        } else {
            if db_job.save_tarball != TarChoice::Yes {
                log::error!("save_tarball expected to be \"Yes\", was not");
                return false;
            }
        }

        true
    }
}
