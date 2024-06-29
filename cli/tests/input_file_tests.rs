use std::{ffi::OsStr, path::PathBuf};

use chrono::NaiveDate;
use float_cmp::approx_eq;
use itertools::Itertools;
use orm::{config::Config, input_files, jobs::{Job, MapFmt, ModFmt, TarChoice, VmrFmt}, siteinfo::{SiteInfo, SiteType, StdSite}, MySqlConn};
use tccon_priors_cli::met_download;

mod common;

#[test_log::test(tokio::test)]
async fn test_successful_input_files() {
    // We will programmatically add a year of met to the database, so call the pool function directly
    // rather than use an SQL file.
    let (pool, _test_db) = common::open_test_database(true).await
        .expect("Could not open database");
    let (config, _tmp_dir) = common::make_dummy_config_with_temp_dirs("priors-test").expect("Failed to make test configuration");
    let mut conn = pool.get_connection().await
        .expect("Could not get database connection from pool");

    let email_backend = if let orm::config::EmailBackend::Testing(backend) = &config.email.backend {
        backend
    } else {
        panic!("This test requires the Testing email backend be configured");
    };

    populate_met_in_db(&mut conn, &config).await;
    populate_standard_sites_in_db(&mut conn).await;

    // For each job, add it to the database, then query out what was added and ensure
    // that the correct job was created.
    let test_input_files = list_test_input_files("should_pass");
    let mut file_mover = input_files::InputFileCleanupHandler::new_for_testing();
    let dummy_save_dir = PathBuf::from(".");
    let mut prev_n_jobs_in_db = 0;
    for file in test_input_files {
        log::info!("Adding job for input file {}", file.display());
        input_files::add_jobs_from_input_files(&mut conn, &config, &[file.clone()], &dummy_save_dir, &mut file_mover)
            .await
            .expect("Adding job from input file should not error");

        let mut db_jobs = Job::get_jobs_list(&mut conn, false).await.expect("Should be able to query jobs");

        // First confirm that the expected number of jobs were added
        let expected = get_expected_job(file.file_name().unwrap());
        let n_new_jobs = db_jobs.len() - prev_n_jobs_in_db;
        assert_eq!(n_new_jobs, expected.len(), "Expected {} new jobs after reading file {}, got {}: {:#?}", 
                   expected.len(), file.display(), n_new_jobs, &db_jobs[prev_n_jobs_in_db..]);

        // Then check that each job is as expected. Ensure that the jobs are in ID order
        // so that we take the most recent ones.
        db_jobs.sort_by_key(|j| j.job_id);
        let new_db_jobs = &db_jobs[prev_n_jobs_in_db..];
        for (i_job, (db_job, exp_job)) in new_db_jobs.into_iter().zip(expected.iter()).enumerate() {
            assert!(
                exp_job.matches_job(db_job, &config),
                "Job {}/{} for input file {} does not match expected. {db_job:#?}\n\n{exp_job:#?}",
                i_job+1,
                new_db_jobs.len(),
                file.display()
            )
        }

        // Last check that a message was sent if it was supposed to be.
        if expected[0].confirmation {
            assert_eq!(email_backend.num_messages(), 1, "One email should be sent for all jobs");
            email_backend.clear();
        } else {
            assert_eq!(email_backend.num_messages(), 0, "Email was sent despite input file having confirmation=false");
        }

        prev_n_jobs_in_db = db_jobs.len();
    }
}

async fn populate_met_in_db(conn: &mut MySqlConn, config: &Config) {
    // All tests will have 2018 GEOS FP-IT and GEOS IT data available. I usually use 2018 because it was
    // one of the first years with GEOS IT data available so at this point it's just habit.
    log::info!("Populating database with Jan 2018 met files");
    let downloader = common::TestDownloader::new();

    met_download::download_files_for_dates(
        conn,
        "geosfpit", 
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2018, 2, 1).unwrap()),
        &config,
        downloader.clone(),
        false).await.expect("'Downloading' Jan 2018 GEOS FP-IT files did not complete successfully");

    met_download::download_files_for_dates(
        conn,
        "geosit", 
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2018, 2, 1).unwrap()),
        &config,
        downloader.clone(),
        false).await.expect("'Downloading' Jan 2018 GEOS IT files did not complete successfully");

    log::info!("Populating database with late May/early June 2023 transition met files");
    met_download::download_files_for_dates(
        conn,
        "geosfpit", 
        NaiveDate::from_ymd_opt(2023, 5, 30).unwrap(),
        Some(NaiveDate::from_ymd_opt(2023, 6, 1).unwrap()),
        &config,
        downloader.clone(),
        false).await.expect("'Downloading' May 2023 GEOS FP-IT files did not complete successfully");

    met_download::download_files_for_dates(
        conn,
        "geosit", 
        NaiveDate::from_ymd_opt(2023, 6, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2023, 6, 3).unwrap()),
        &config,
        downloader,
        false).await.expect("'Downloading' June 2023 GEOS IT files did not complete successfully");
}

async fn populate_standard_sites_in_db(conn: &mut MySqlConn) {
    log::info!("Adding standard sites to database");
    let sites = [
        ("ci", "Caltech", "Pasadena, CA, USA".to_string(), 34.1362, -118.1269, 2012, 9),
        ("oc", "Lamont", "Lamont, OK, USA".to_string(), 36.604, -97.486, 2008, 7),
        ("pa", "Park Falls", "Park Falls, WI, USA".to_string(), 45.945, -90.273, 2004, 5)
    ];

    for (sid, name, loc, lat, lon, start_year, start_month) in sites {
        StdSite::create(conn, sid, name, SiteType::TCCON)
            .await.expect("Should be able to add new site");
        let start_date = NaiveDate::from_ymd_opt(start_year, start_month, 1).unwrap();
        SiteInfo::set_site_info_for_dates(
            conn, sid, start_date, None, Some(loc), Some(lon), Some(lat), None, false
        ).await.expect("Should be able to set location information for standard site.")

    }
}

fn list_test_input_files(subdir: &str) -> Vec<PathBuf> {
    // This should resolve to the directory containing the Cargo.toml for the
    // cli crate, not the workspace.
    let cargo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let test_dir = cargo_root
        .join("test_input_files")
        .join(subdir);
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

// ---------------------------------------- //
// Define expected jobs for each input file //
// ---------------------------------------- //

fn get_expected_job(file_name: &OsStr) -> Vec<ExpectedJob> {
    let file_name = file_name.to_string_lossy();
    match file_name.as_ref() {
        "all_keys_reordered.txt" => {vec![
            ExpectedJob::new(vec!["kc"], (2018, 1, 4), (2018, 1, 11), "test@test.rs")
                .with_lat_lon(vec![2.0], vec![-2.0])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::None).with_map_fmt(MapFmt::Text)
        ]},
        "all_keys.txt" => {vec![
            ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 11), "test@test.com")
                .with_lat_lon(vec![0.0], vec![0.0])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
        ]},
        "all_positional.txt" => {vec![
            ExpectedJob::new(vec!["pa"], (2018, 1, 1), (2018, 1, 31), "test@test.net")
                .with_lat_lon(vec![45.1], vec![-90.0])
                .with_mod_fmt(ModFmt::None).with_vmr_fmt(VmrFmt::None).with_map_fmt(MapFmt::NetCDF)
        ]},
        "carriage_return.txt" => {vec![
            ExpectedJob::new(vec!["xd"], (2018, 1, 1), (2018, 1, 2), "cr@test.net")
                .with_lat_lon(vec![34.691], vec![-117.818])
                .with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
                .with_egi(true)
        ]},
        "egi1.txt" => {vec![
            ExpectedJob::new(vec!["jp"], (2018, 1, 26), (2018, 1, 27), "jhedeliu@caltech.edu")
                .with_lat_lon(vec![34.180], vec![-118.121])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
                .with_egi(true)
        ]},
        "mixed_keys_pos.txt" => {vec![
            ExpectedJob::new(vec!["ma"], (2018, 1, 1), (2018, 1, 31), "test@test.mix")
                .with_lat_lon(vec![42.0], vec![-42.0])
                .with_mod_fmt(ModFmt::None).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
        ]},
        "multi_loc_multi_ids.txt" => {vec![
            ExpectedJob::new(vec!["mc","md","me"], (2018, 1, 1), (2018, 1, 31), "test@test.mix")
                .with_lat_lon(vec![42.0, 43.0, 44.1], vec![-42.0, -41.0, -40.0])
                .with_mod_fmt(ModFmt::None).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
        ]},
        "multi_loc_one_id.txt" => {vec![
            ExpectedJob::new(vec!["mb"], (2018, 1, 1), (2018, 1, 31), "test@test.mix")
                .with_lat_lon(vec![42.0, 43.0, 44.1,45.0], vec![-42.0, -41.0, -40.0,-39.9])
                .with_mod_fmt(ModFmt::None).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
        ]},
        "req_keys.txt" => {vec![
            ExpectedJob::new(vec!["kb"], (2018, 1, 1), (2018, 1, 11), "test@test.io")
                .with_lat_lon(vec![-1.0], vec![1.0]) 
        ]},
        "req_positional.txt" => {vec![
            ExpectedJob::new(vec!["pb"], (2018, 1, 1), (2018, 1, 28), "test@test.org")
                .with_lat_lon(vec![-42.0], vec![101.0]) 
        ]},
        "short_all_keys_no_confirm.txt" => {vec![
            ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 5), "test@test.com")
                .with_lat_lon(vec![0.0], vec![0.0])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
                .with_confirmation(false)
        ]},
        "short_all_keys_with_met.txt" => {vec![
            ExpectedJob::new(vec!["ka"], (2018, 1, 4), (2018, 1, 5), "test@test.com")
                .with_lat_lon(vec![0.0], vec![0.0])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
                .with_alt_met("co_reprocessing")
        ]},
        "short_all_keys.txt" => {vec![
            ExpectedJob::new(vec!["ka"], (2018,1,4), (2018,1,5), "test@test.com")
                .with_lat_lon(vec![0.0], vec![0.0])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
        ]},
        "short_alt_met.txt" => {vec![
            ExpectedJob::new(vec!["ka"], (2018, 1, 1), (2018, 1, 2), "test@test.com")
                .with_lat_lon(vec![0.0], vec![0.0])
                .with_alt_met("co_reprocessing")
        ]},
        "split_days.txt" => {vec![
            // Although its split over the transition between mets, this doesn't actually need split
            // into separate jobs; the runner calls ginput for each day.
            ExpectedJob::new(vec!["ka"], (2023,5,31), (2023,6,2), "test@test.com")
                .with_lat_lon(vec![0.0], vec![0.0])
                .with_mod_fmt(ModFmt::Text).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None),
        ]},
        "std_sites.txt" => {vec![
            ExpectedJob::new(vec!["pa","oc","ci"], (2018, 1, 1), (2018, 1, 31), "test@test.mix")
                .with_mod_fmt(ModFmt::None).with_vmr_fmt(VmrFmt::Text).with_map_fmt(MapFmt::None)
        ]},
        "trailing_whitespace.txt" => {vec![
            ExpectedJob::new(vec!["ka"], (2018, 1, 1), (2018, 1, 3), "test@test.net")
                .with_lat_lon(vec![49.1025], vec![8.4397])
        ]},
        _ => unimplemented!("No expected job was defined for {}", file_name)
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
    alt_met: Option<&'static str>,
    is_egi: bool,
    confirmation: bool,
}

impl ExpectedJob {
    fn new(site_ids: Vec<&'static str>, start_date: (i32, u32, u32), end_date: (i32, u32, u32), email: &'static str) -> Self {
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
            alt_met: None,
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
        self.alt_met = Some(alt_met);
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

    fn matches_job(&self, db_job: &Job, config: &Config) -> bool {
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
                (None, None) => {},
                (None, Some(_)) | (Some(_), None) => return false,
                (Some(y1), Some(y2)) => {
                    if !approx_eq!(f32, *y1, *y2, ulps = 2) {
                        log::error!("At least one lat differed: expected {y1} vs. actual {y2}");
                        return false
                    }
                },
            }
        }

        for (my_x, db_x) in self.lon.iter().zip(db_job.lon.iter()) {
            match (my_x, db_x) {
                (None, None) => {},
                (None, Some(_)) | (Some(_), None) => return false,
                (Some(x1), Some(x2)) => {
                    if !approx_eq!(f32, *x1, *x2, ulps = 2) {
                        log::error!("At least one lon differed: expected {x1} vs. actual {x2}");
                        return false
                    }
                },
            }
        }

        if let Some(alt_met) = self.alt_met {
            let alt_cfg = config.requests.allowed_mets.get(alt_met)
                .expect("Alternate met should be defined in the test config");
            if alt_cfg.met_key != db_job.met_key.as_deref().unwrap_or("???") {
                log::error!("met_key (from alternate met request) did not match");
                return false;
            }
            if alt_cfg.ginput_key != db_job.ginput_key.as_deref().unwrap_or("???") {
                log::error!("ginput_key (from alternate met request) did not match");
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