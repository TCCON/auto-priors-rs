use std::path::PathBuf;

use anyhow::Context;
use chrono::{NaiveDate, Duration};
use serial_test::serial;
use orm::met::{MetDayState, MetLevels, MetDataType};
use tccon_priors_cli::{met_download::{check_files_for_dates, self}, utils::{WgetDownloader, Downloader}};
mod common;



// Any tests that rely on database access should be marked as #[serial] to prevent
// them from conflicting. Even tests that use different tables should all be run
// in serial because the default reset migration drops ALL tables.

#[tokio::test]
#[serial]
async fn test_check_met() {
    let mut conn = common::multiline_sql_init!("sql/check_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let stat_map = check_files_for_dates(
        &mut conn,
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd(2020, 1, 1), 
        Some(NaiveDate::from_ymd(2020, 1, 2)),
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 1, 1)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Complete, "Day expected to be complete was not");

    // Should be marked as incomplete because they are each missing one of one type of file

    let stat_map = check_files_for_dates(
        &mut conn, 
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd(2020, 2, 1), 
        Some(NaiveDate::from_ymd(2020, 2, 4)),
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 2, 1)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing one surface met file was not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 2, 2)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing one eta met file was not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 2, 3)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing one eta chem file not marked Incomplete");

    // Should also be marked as incomplete - missing all of one type of file
    let stat_map = check_files_for_dates(
        &mut conn, 
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd(2020, 3, 1), 
        Some(NaiveDate::from_ymd(2020, 3, 4)),
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 3, 1)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing all surface met files not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 3, 2)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing all eta met files not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 3, 3)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing all eta chem files not marked Incomplete");

    // This day isn't in the database at all, should be marked as missing
    let stat_map = check_files_for_dates(
        &mut conn, 
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd(2020, 4, 1), 
        Some(NaiveDate::from_ymd(2020, 4, 2)),
    ).await.unwrap();
    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 4, 1)).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Missing, "Day missing all files not marked Missing");
}

#[tokio::test]
#[serial]
async fn test_download_default_fpit() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");

    // 2018 should be GEOS FP-IT in the default configuration
    todo!()
}

#[tokio::test]
#[serial]
async fn test_met_dates_defaults_empty_db() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter(
        &mut conn, 
        &config, 
        None, 
        None, 
        None,
        false).await.unwrap();

    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2000, 1, 1)), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_user_empty_db() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd(2010, 1, 1);
    let end = NaiveDate::from_ymd(2011, 1, 1);

    let mut date_iter = met_download::get_date_iter(
        &mut conn, 
        &config, 
        Some(start), 
        Some(end), 
        None,
        false).await.unwrap();

    assert_eq!(date_iter.next(), Some(start), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(end - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_default_fpit_in_db() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_next_date.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter(
        &mut conn, 
        &config, 
        None, 
        None, 
        None,
        false).await.unwrap();

        // the database has 2018-01-01 in it, so this should return 2018-01-02 as the start date
        assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2018, 1, 2)), "First date in iterator was incorrect");
        assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_user_override_fpit_in_db() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_next_date.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = Some(NaiveDate::from_ymd(2005, 6, 1));
    let mut date_iter = met_download::get_date_iter(
        &mut conn, 
        &config, 
        start, 
        None, 
        None,
        false).await.unwrap();

        // the database has 2018-01-01 in it, but we're overriding that, so this should return our specified start date
        assert_eq!(date_iter.next(), start, "First date in iterator was incorrect");
        assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_default_it_in_db() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_it_next_date.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter(
        &mut conn, 
        &config, 
        None, 
        None, 
        None,
        false).await.unwrap();

        // the database has 2023-07-01 in it, so this should return 2023-07-02 as the start date
        assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2023, 7, 2)), "First date in iterator was incorrect");
        assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_default_fpit_plus_partial_it_in_db() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_next_date.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter(
        &mut conn, 
        &config, 
        None, 
        None, 
        None,
        false).await.unwrap();

        // the database has 2018-01-01 in it for FPIT and only part of 2023-07-01 for IT, so this should return 2018-01-02 as the start date
        // this tests both that it will correctly look back for different met and that it ignores partial days
        assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2018, 1, 2)), "First date in iterator was incorrect");
        assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_single_met_dates_user_override() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd(2014, 5, 1);
    let end = NaiveDate::from_ymd(2016, 3, 1);

    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        Some(start),
        Some(end),
        Some("geosfpit"), 
        false).await.unwrap();

    // should ignore everything in the database
    assert_eq!(date_iter.next(), Some(start), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(end - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_single_met_dates_start_from_db() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day after we have fpit met data and stop on the last day before geos-it is set to start
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2018, 1, 2)), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd(2023, 5, 31)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_single_met_start_from_dl_config() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let mut config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day defined as the earliest day FPIT is available and stop on the last day before geos-it is set to start
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2000, 1, 1)), "First date in iterator was incorrect when all files have the same earliest_date values");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd(2023, 5, 31)), "Final date in iterator was incorrect when all files have the same earliest_date values");

    let fpit_cfg = config.data.download.get_mut("geosfpit").unwrap();
    fpit_cfg[0].earliest_date = NaiveDate::from_ymd(2011, 1, 1);

    // Redo the test with the different files having different start dates - should take the latest one
    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day defined as the earliest day FPIT is available and stop on the last day before geos-it is set to start
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2011, 1, 1)), "First date in iterator was incorrect when files have different earliest_date values");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd(2023, 5, 31)), "Final date in iterator was incorrect when files have different earliest_date values");

}


#[tokio::test]
#[serial]
async fn test_single_met_start_from_defaults() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let mut config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    // This is deliberately contrived - someone would really have to write a funky TOML file
    // for this to happen, but it could.
    config.data.download.insert("geosfpit".to_owned(), vec![]);
    // The test config doesn't define a start date for GEOS-FPIT, so let's change that.
    config.default_options
        .get_mut(0)
        .unwrap()
        .start_date = Some(NaiveDate::from_ymd(2004, 7, 1));

    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day defined as the earliest day FPIT is available and stop on the last day before geos-it is set to start
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd(2004, 7, 1)), "First date in iterator was incorrect when all files have the same earliest_date values");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd(2023, 5, 31)), "Final date in iterator was incorrect when all files have the same earliest_date values");
}


#[tokio::test]
#[serial]
async fn test_single_met_no_valid_start_err() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let mut config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    // This is deliberately contrived - someone would really have to write a funky TOML file
    // for this to happen, but it could. Now there should be no way for it to find a start date.
    config.data.download.insert("geosfpit".to_owned(), vec![]);

    let res = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await;

    // should start on the day defined as the earliest day FPIT is available and stop on the last day before geos-it is set to start
    assert!(res.is_err(), "Date iterator call did not error when no valid start date could be determined.");
}


#[tokio::test]
#[serial]
async fn test_single_met_cross_boundary_with_defaults() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd(2023, 5, 20);
    let end = NaiveDate::from_ymd(2023, 8, 1);
    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        Some(start),
        Some(end),
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day we requested and stop on the day before geos-it starts
    assert_eq!(date_iter.next(), Some(start), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd(2023, 5, 31)), "Final date in iterator was incorrect");
}


#[tokio::test]
#[serial]
async fn test_single_met_cross_boundary_ignoring_defaults() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd(2023, 5, 20);
    let end = NaiveDate::from_ymd(2023, 8, 1);
    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        Some(start),
        Some(end),
        Some("geosfpit"), 
        true).await.unwrap();

    // should start on the day we requested and stop on the day before our end
    assert_eq!(date_iter.next(), Some(start), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(end - Duration::days(1)), "Final date in iterator was incorrect");
}

#[test]
fn test_geosfp_download() {
    let tmp_dir = tempdir::TempDir::new("test_geosfp_download").expect("Failed to make temporary directory");
    let config = common::make_dummy_config(tmp_dir.path().to_owned()).expect("Failed to make test configuration");


    let fp_dl_cfg = config.get_met_configs("geosfp").expect("Could not get geosfp met configs");
    let fp_dl_cfg = fp_dl_cfg.iter()
        .find(|&cfg| cfg.levels == MetLevels::Surf && cfg.data_type == MetDataType::Met)
        .expect("Could not find the surface met GEOS FP download config");

    std::fs::create_dir(&fp_dl_cfg.download_dir)
        .expect("Could not create temporary download directory for GEOS FP");

    let test_datetime = NaiveDate::from_ymd(2018, 1, 1).and_hms(0, 0, 0);
    let test_url = test_datetime.format(&fp_dl_cfg.url_pattern).to_string();
    let mut downloader = WgetDownloader::new_with_verbosity(0);
    downloader.add_file_to_download(test_url).unwrap();
    downloader.download_files(&fp_dl_cfg.download_dir)
        .expect("Failed to download GEOS FP file");
    
    let expected_file = fp_dl_cfg.download_dir.join("GEOS.fp.asm.inst3_2d_asm_Nx.20180101_0000.V01.nc4");
    println!("Expected file = {}", expected_file.display());
    assert!(expected_file.exists(), "Download succeeded, but expected file is not present");

    let expected_checksum = hex_literal::hex!("ade5e528d45f55b9eb37e1676e782ec3");
    let actual_checksum = common::md5sum(&expected_file).expect("Could not compute checksum on downloaded file");
    assert_eq!(actual_checksum, expected_checksum, "GEOS FP file checksum did not match");
}