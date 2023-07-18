use std::path::PathBuf;

use anyhow::Context;
use chrono::{NaiveDate, Duration};
use serial_test::serial;
use orm::met::{MetDayState, MetLevels, MetDataType, MetFile};
use tccon_priors_cli::{met_download::{check_files_for_dates, self}, utils::{WgetDownloader, Downloader}};
mod common;

static EXPECTED_GEOSFPIT_FILES_20180102: [&'static str; 24] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0000.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0300.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1200.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_2100.V01.nc4",
];

static EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_A: [&'static str; 18] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1200.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_2100.V01.nc4",
];

static EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_B: [&'static str; 18] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0000.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_2100.V01.nc4",
];

static EXPECTED_GEOSIT_FILES_20180702: [&'static str; 24] = [
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0000.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0300.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0600.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1200.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0000.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0300.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0600.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T2100.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0000.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0300.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0600.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0900.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T1200.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T1500.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T1800.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T2100.V01.nc4",
];

static EXPECTED_GEOS_TRANSITION_FILES: [&'static str; 48] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_2100.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0000.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0300.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0600.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0900.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1200.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1500.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1800.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0000.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0300.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0600.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0000.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0300.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_1200.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_2100.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0000.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0300.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0600.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0900.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T1200.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T1500.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T1800.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T2100.V01.nc4",
];

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
        NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(), 
        Some(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()),
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Complete, "Day expected to be complete was not");

    // Should be marked as incomplete because they are each missing one of one type of file

    let stat_map = check_files_for_dates(
        &mut conn, 
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd_opt(2020, 2, 1).unwrap(), 
        Some(NaiveDate::from_ymd_opt(2020, 2, 4).unwrap()),
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 2, 1).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing one surface met file was not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 2, 2).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing one eta met file was not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 2, 3).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing one eta chem file not marked Incomplete");

    // Should also be marked as incomplete - missing all of one type of file
    let stat_map = check_files_for_dates(
        &mut conn, 
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd_opt(2020, 3, 1).unwrap(), 
        Some(NaiveDate::from_ymd_opt(2020, 3, 4).unwrap()),
    ).await.unwrap();

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 3, 1).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing all surface met files not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 3, 2).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing all eta met files not marked Incomplete");

    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 3, 3).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Incomplete, "Day missing all eta chem files not marked Incomplete");

    // This day isn't in the database at all, should be marked as missing
    let stat_map = check_files_for_dates(
        &mut conn, 
        &config,
        common::TEST_MET_KEY,
        NaiveDate::from_ymd_opt(2020, 4, 1).unwrap(), 
        Some(NaiveDate::from_ymd_opt(2020, 4, 2).unwrap()),
    ).await.unwrap();
    let stat = stat_map.get(&NaiveDate::from_ymd_opt(2020, 4, 1).unwrap()).unwrap().unwrap();
    assert_eq!(stat, MetDayState::Missing, "Day missing all files not marked Missing");
}

#[tokio::test]
#[serial]
async fn test_geosfpit_download_by_dates() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");

    // 2018 should be GEOS FP-IT in the default configuration
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("default_fpit").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_files_for_dates(
        &mut conn,
        "geosfpit", 
        NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(),
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        &config,
        downloader,
        false).await.expect("'Downloading' files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102)
        .expect("Not all 'FP-IT' files for 2018-01-02 downloaded");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Not all 'FP-IT' files for 2018-01-02 stored in the database");
}

#[tokio::test]
#[serial]
async fn test_geosit_download_by_dates() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");

    // 2023 after June should be GEOS IT in the default configuration
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("default_it").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_files_for_dates(
        &mut conn,
        "geosit",
        NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(),
        Some(NaiveDate::from_ymd_opt(2023, 7, 3).unwrap()),
        &config,
        downloader,
        false).await.expect("'Downloading' files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSIT_FILES_20180702)
        .expect("Not all 'IT' files for 2023-07-02 downloaded");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSIT_FILES_20180702)
        .await
        .expect("Not all 'IT' files for 2023-07-02 stored in the database");
}

#[tokio::test]
#[serial]
async fn test_geosfpit_to_geos_it_download_by_dates() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");

    // 2023 after June should be GEOS IT in the default configuration
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("default_fpit_to_it").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()),
        Some(NaiveDate::from_ymd_opt(2023, 6, 2).unwrap()),
        None,
        false,
        &config,
        downloader,
        false).await.expect("'Downloading' files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOS_TRANSITION_FILES)
        .expect("Not all 'GEOS FP-IT/IT' files in the 'transition period' downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOS_TRANSITION_FILES)
        .await
        .expect("Not all 'GEOS FP-IT/IT' files in the 'transition period' entered in the database.");
}

#[tokio::test]
#[serial]
async fn test_download_default_geosfpit_missing() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_next_date.sql");
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("missing_fpit").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        None,
        false,
        &config,
        downloader,
        false).await.expect("'Downloading' missing files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102)
        .expect("Not all missing 'GEOS FP-IT' files for 2018-01-02 downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Not all missing 'GEOS FP-IT' files for 2018-01-02 entered in the database.");
    
    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018,1,1).unwrap()
    ).fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(res, 0, "Files before 2018-01-01 should not have been added to the database, but were.");
}

#[tokio::test]
#[serial]
async fn test_download_default_geosit_missing() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_it_next_date.sql");
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("missing_it").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2023, 7, 3).unwrap()),
        None,
        false,
        &config,
        downloader,
        false).await.expect("'Downloading' missing files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSIT_FILES_20180702)
        .expect("Not all missing 'GEOS IT' files for 2023-07-02 downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSIT_FILES_20180702)
        .await
        .expect("Not all missing 'GEOS IT' files for 2023-07-02 entered in the database.");
    
    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2023,7,1).unwrap()
    ).fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(res, 0, "Files before 2023-07-01 should not have been added to the database, but were.");
}

#[tokio::test]
#[serial]
async fn test_download_default_geosfpit_to_geosit_missing() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_to_it_transition_next_date.sql");
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("missing_fpit_and_it").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2023, 7, 2).unwrap()),
        None,
        false,
        &config,
        downloader,
        false).await.expect("'Downloading' missing files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOS_TRANSITION_FILES)
        .expect("Not all missing 'GEOS FP-IT/IT' files for the 'transition period' downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOS_TRANSITION_FILES)
        .await
        .expect("Not all missing 'GEOS FP-IT/IT' files for the 'transition period' entered in the database.");
    
    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2023,5,30).unwrap()
    ).fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(res, 0, "Files before 2023-05-30 should not have been added to the database, but were.");
}

#[tokio::test]
#[serial]
async fn test_download_partial_day_from_start() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_next_partial.sql");
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("missing_fpit_partial").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        None,
        false,
        &config,
        downloader,
        false).await.expect("'Downloading' missing files did not complete successfully");

    // Because of how we're testing the download, only the "missing" files will actually be downloaded
    // so we can't check for all the 2018-01-02 files (hence the "partial" arrays)
    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_A)
        .expect("Not all missing 'GEOS FP-IT' files for the partial day downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_A)
        .await
        .expect("Not all missing 'GEOS FP-IT' files for the partial entered in the database.");
    
    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018,1,1).unwrap()
    ).fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(res, 0, "Files before 2018-01-01 should not have been added to the database, but were.");
}

#[tokio::test]
#[serial]
async fn test_download_partial_day_scattered() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_next_partial_scattered.sql");
    let (config, tmp_dir) = common::make_dummy_config_with_temp_dirs("missing_fpit_partial_scattered").expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        None,
        false,
        &config,
        downloader,
        false).await.expect("'Downloading' missing files did not complete successfully");

    // Because of how we're testing the download, only the "missing" files will actually be downloaded
    // so we can't check for all the 2018-01-02 files (hence the "partial" arrays)
    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_B)
        .expect("Not all missing 'GEOS FP-IT' files for the partial day downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_B)
        .await
        .expect("Not all missing 'GEOS FP-IT' files for the partial entered in the database.");
    
    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018,1,1).unwrap()
    ).fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(res, 0, "Files before 2018-01-01 should not have been added to the database, but were.");
}

#[tokio::test]
#[serial]
async fn test_met_rescanning() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let (config, _tmp_dir) = common::make_dummy_config_with_temp_dirs("met_rescan").expect("Failed to set up test config and temp directories");

    // Let's use the TestDownloader to "download" some files without adding them to the database
    let test_date = NaiveDate::from_ymd_opt(2018, 1, 2).unwrap();
    let geos_fpit_cfgs = config.get_met_configs("geosfpit").expect("geosfpit key missing from test download configs");
    for dl_cfg in geos_fpit_cfgs {
        let mut downloader = common::TestDownloader::new();
        for file_time in dl_cfg.times_on_day(test_date) {
            let file_url = file_time.format(&dl_cfg.url_pattern).to_string();
            downloader.add_file_to_download(file_url).unwrap();
        }
        downloader.download_files(&dl_cfg.download_dir).expect("Downloading files failed");
    }

    // Now we should have all the files from 2018-01-02
    met_download::rescan_met_files(
        &mut conn,
        Some(test_date),
        Some(test_date + Duration::days(1)),
        &config,
        None,
        false,
        false).await.expect("Rescanning for 2018-01-02 failed");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Rescanning 2018-01-02 failed to find all expected files");
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

    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_user_empty_db() {
    // Don't need any initial values in the database, just a connection to a blank database
    let pool = common::open_test_database(true).await.expect("Failed to open test database");
    let mut conn = pool.acquire().await.expect("Failed to acquire connection to database");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2011, 1, 1).unwrap();

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
        assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()), "First date in iterator was incorrect");
        assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_met_dates_user_override_fpit_in_db() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_next_date.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = Some(NaiveDate::from_ymd_opt(2005, 6, 1).unwrap());
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
        assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2023, 7, 2).unwrap()), "First date in iterator was incorrect");
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
        assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()), "First date in iterator was incorrect");
        assert_eq!(date_iter.last(), Some(chrono::offset::Utc::now().naive_utc().date() - Duration::days(1)), "Final date in iterator was incorrect");
}

#[tokio::test]
#[serial]
async fn test_single_met_dates_user_override() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd_opt(2014, 5, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2016, 3, 1).unwrap();

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
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()), "Final date in iterator was incorrect");
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
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()), "First date in iterator was incorrect when all files have the same earliest_date values");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()), "Final date in iterator was incorrect when all files have the same earliest_date values");

    let fpit_cfg = config.data.download.get_mut("geosfpit").unwrap();
    fpit_cfg[0].earliest_date = NaiveDate::from_ymd_opt(2011, 1, 1).unwrap();

    // Redo the test with the different files having different start dates - should take the latest one
    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day defined as the earliest day FPIT is available and stop on the last day before geos-it is set to start
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2011, 1, 1).unwrap()), "First date in iterator was incorrect when files have different earliest_date values");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()), "Final date in iterator was incorrect when files have different earliest_date values");

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
        .start_date = Some(NaiveDate::from_ymd_opt(2004, 7, 1).unwrap());

    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        None,
        None,
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day defined as the earliest day FPIT is available and stop on the last day before geos-it is set to start
    assert_eq!(date_iter.next(), Some(NaiveDate::from_ymd_opt(2004, 7, 1).unwrap()), "First date in iterator was incorrect when all files have the same earliest_date values");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()), "Final date in iterator was incorrect when all files have the same earliest_date values");
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

    let start = NaiveDate::from_ymd_opt(2023, 5, 20).unwrap();
    let end = NaiveDate::from_ymd_opt(2023, 8, 1).unwrap();
    let mut date_iter = met_download::get_date_iter(
        &mut conn,
        &config, 
        Some(start),
        Some(end),
        Some("geosfpit"), 
        false).await.unwrap();

    // should start on the day we requested and stop on the day before geos-it starts
    assert_eq!(date_iter.next(), Some(start), "First date in iterator was incorrect");
    assert_eq!(date_iter.last(), Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()), "Final date in iterator was incorrect");
}


#[tokio::test]
#[serial]
async fn test_single_met_cross_boundary_ignoring_defaults() {
    let mut conn = common::multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = common::make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd_opt(2023, 5, 20).unwrap();
    let end = NaiveDate::from_ymd_opt(2023, 8, 1).unwrap();
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

#[tokio::test]
#[serial]
async fn test_find_met_file_by_name() {
    let mut conn = common::multiline_sql_init!("sql/check_finding_met_file.sql");
    let check_some = MetFile::get_file_by_name(&mut conn, "geos_surf_test_20200101_0000.nc")
        .await
        .expect("Database query failed or returned >1 file");

    assert!(check_some.is_some(), "Failed to find file geos_surf_test_20200101_0000.nc");
    assert_eq!(check_some.unwrap().file_path, PathBuf::from("/data/met/Nx/geos_surf_test_20200101_0000.nc"));

    let check_none = MetFile::get_file_by_name(&mut conn, "bob")
        .await
        .expect("Database query failed or returned >1 file");

    assert!(check_none.is_none(), "Erroneously matched the file name 'bob'");
}

#[tokio::test]
#[serial]
async fn test_find_met_file_by_path() {
    let test_path = PathBuf::from("/data/met/Nx/geos_surf_test_20200101_0000.nc");
    let mut conn = common::multiline_sql_init!("sql/check_finding_met_file.sql");
    let check_some = MetFile::get_file_by_full_path(&mut conn, &test_path)
        .await
        .expect("Database query for full path failed or returned >1 file");

    assert!(check_some.is_some(), "Failed to find file /data/met/Nx/geos_surf_test_20200101_0000.nc");
    assert_eq!(check_some.unwrap().file_path, test_path);

    let check_none = MetFile::get_file_by_full_path(&mut conn, &PathBuf::from("geos_surf_test_20200101_0000.nc"))
        .await
        .expect("Database query for base name failed or returned >1 file");
    assert!(check_none.is_none(), "Erroneously matched the basename when checking for full path");
}

#[test] #[ignore = "requires downloading a file"]
fn test_geosfp_download() {
    let tmp_dir = tempdir::TempDir::new("test_geosfp_download").expect("Failed to make temporary directory");
    let config = common::make_dummy_config(tmp_dir.path().to_owned()).expect("Failed to make test configuration");


    let fp_dl_cfg = config.get_met_configs("geosfp").expect("Could not get geosfp met configs");
    let fp_dl_cfg = fp_dl_cfg.iter()
        .find(|&cfg| cfg.levels == MetLevels::Surf && cfg.data_type == MetDataType::Met)
        .expect("Could not find the surface met GEOS FP download config");

    std::fs::create_dir(&fp_dl_cfg.download_dir)
        .expect("Could not create temporary download directory for GEOS FP");

    let test_datetime = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
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