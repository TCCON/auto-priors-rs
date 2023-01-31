use anyhow::Context;
use chrono::NaiveDate;
use serial_test::serial;
use orm::geos::{GeosDayState,GeosProduct,GeosLevels};
use tccon_priors_cli::met_download::check_files_for_dates;
mod common;



// Any tests that rely on database access should be marked as #[serial] to prevent
// them from conflicting. Even tests that use different tables should all be run
// in serial because the default reset migration drops ALL tables.

#[tokio::test]
#[serial]
async fn test_check_met() {
    let mut conn = common::multiline_sql_init!("sql/check_met.sql");

    let stat_map = check_files_for_dates(
        &mut conn, 
        NaiveDate::from_ymd(2020, 1, 1), 
        Some(NaiveDate::from_ymd(2020, 1, 2)),
        GeosProduct::Fpit,
        GeosLevels::Eta,
        true
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 1, 1)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Complete, "Day expected to be complete was not");

    // Should be marked as incomplete because they are each missing one of one type of file

    let stat_map = check_files_for_dates(
        &mut conn, 
        NaiveDate::from_ymd(2020, 2, 1), 
        Some(NaiveDate::from_ymd(2020, 2, 4)),
        GeosProduct::Fpit,
        GeosLevels::Eta,
        true
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 2, 1)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Incomplete, "Day missing one surface met file was not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 2, 2)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Incomplete, "Day missing one eta met file was not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 2, 3)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Incomplete, "Day missing one eta chem file not marked Incomplete");

    // Should also be marked as incomplete - missing all of one type of file
    let stat_map = check_files_for_dates(
        &mut conn, 
        NaiveDate::from_ymd(2020, 3, 1), 
        Some(NaiveDate::from_ymd(2020, 3, 4)),
        GeosProduct::Fpit,
        GeosLevels::Eta,
        true
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 3, 1)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Incomplete, "Day missing all surface met files not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 3, 2)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Incomplete, "Day missing all eta met files not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 3, 3)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Incomplete, "Day missing all eta chem files not marked Incomplete");

    // This day isn't in the database at all, should be marked as missing
    let stat_map = check_files_for_dates(
        &mut conn, 
        NaiveDate::from_ymd(2020, 4, 1), 
        Some(NaiveDate::from_ymd(2020, 4, 2)),
        GeosProduct::Fpit,
        GeosLevels::Eta,
        true
    ).await.unwrap();
    let stat = stat_map.get(&NaiveDate::from_ymd(2020, 4, 1)).unwrap().unwrap();
    assert_eq!(stat, GeosDayState::Missing, "Day missing all files not marked Missing");
}