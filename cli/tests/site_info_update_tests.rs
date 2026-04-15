use std::path::PathBuf;

use chrono::NaiveDate;
use itertools::Itertools;
use orm::{
    multiline_sql, multiline_sql_init,
    siteinfo::SiteInfo,
    stdsitejobs::{StdSiteJob, StdSiteJobState},
    test_utils::{add_dummy_met_for_date_range, init_logging, make_dummy_config},
    utils::DateIterator,
};

#[tokio::test]
async fn test_add_site_info_state_reset() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    // Add met around the Caltech start date. That way we can test that extending the
    // Caltech period forward adds jobs and that changing the Lamont info for a subset
    // of this flags those rows for regeneration.
    add_dummy_met_for_date_range(
        &mut conn,
        NaiveDate::from_ymd_opt(2012, 8, 1).unwrap(),
        NaiveDate::from_ymd_opt(2012, 10, 1).unwrap(),
        true,
    )
    .await;

    // The first update to the job table doesn't need to actually create the jobs,
    // we'll just manually set the state to be completed
    StdSiteJob::update_std_site_job_table(&mut *conn, &config, None)
        .await
        .unwrap();

    sqlx::query!(
        "UPDATE StdSiteJobs SET state = ?",
        StdSiteJobState::Complete
    )
    .execute(&mut *conn)
    .await
    .expect("Setting all standard site jobs to 'complete' should succeed");

    // Double check that the correct number of rows were added
    let n_ci = sqlx::query!(
        "SELECT COUNT(*) as n FROM v_StdSiteJobs WHERE site_id = ? AND state = ?",
        "ci",
        StdSiteJobState::Complete
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Checking the number of ci rows should succeed")
    .n;
    assert_eq!(
        n_ci, 30,
        "ci should have 30 days (all of September 2012) added to the StdSiteJobs table initially"
    );

    let n_oc = sqlx::query!(
        "SELECT COUNT(*) as n FROM v_StdSiteJobs WHERE site_id = ? AND state = ?",
        "oc",
        StdSiteJobState::Complete
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Checking the number of oc rows should succeed")
    .n;
    assert_eq!(
        n_oc, 61,
        "oc should have 61 days (Aug and Sept 2012) added to the StdSiteJobs table initially"
    );

    // Now we update the information for both sites - extend Caltech back in time + change a few days and change a few days of Lamont.
    // Changed days should be flagged as needing regeneration and new days as needing a job.
    let change_start = NaiveDate::from_ymd_opt(2012, 8, 30).unwrap();
    let change_end = NaiveDate::from_ymd_opt(2012, 9, 3).unwrap();
    SiteInfo::set_site_info_for_dates(
        &mut conn,
        &config,
        "ci",
        change_start,
        Some(change_end),
        None,
        Some(0.0),
        Some(0.0),
        None,
        false,
    )
    .await
    .expect("Changing ci info should succeed");
    SiteInfo::set_site_info_for_dates(
        &mut conn,
        &config,
        "oc",
        change_start,
        Some(change_end),
        None,
        Some(90.0),
        Some(45.0),
        None,
        false,
    )
    .await
    .expect("Changing oc info should succeed");

    let ci_expected_new_dates =
        DateIterator::new_one_range(change_start, NaiveDate::from_ymd_opt(2012, 9, 1).unwrap())
            .collect_vec();
    let ci_expected_changed_dates =
        DateIterator::new_one_range(NaiveDate::from_ymd_opt(2012, 9, 1).unwrap(), change_end)
            .collect_vec();
    let oc_expected_changed_dates =
        DateIterator::new_one_range(change_start, change_end).collect_vec();

    let ci_actual_new_dates = sqlx::query!(
        "SELECT date FROM v_StdSiteJobs WHERE site_id = ? AND state = ?",
        "ci",
        StdSiteJobState::JobNeeded
    )
    .fetch_all(&mut *conn)
    .await
    .expect("Getting new ci rows should succeed")
    .into_iter()
    .map(|r| r.date)
    .collect_vec();

    let ci_actual_changed_dates = sqlx::query!(
        "SELECT date FROM v_StdSiteJobs WHERE site_id = ? AND state = ?",
        "ci",
        StdSiteJobState::RegenNeeded
    )
    .fetch_all(&mut *conn)
    .await
    .expect("Getting changed ci rows should succeed")
    .into_iter()
    .map(|r| r.date)
    .collect_vec();

    let oc_actual_new_dates = sqlx::query!(
        "SELECT date FROM v_StdSiteJobs WHERE site_id = ? AND state = ?",
        "oc",
        StdSiteJobState::JobNeeded
    )
    .fetch_all(&mut *conn)
    .await
    .expect("Getting new oc rows should succeed")
    .into_iter()
    .map(|r| r.date)
    .collect_vec();

    let oc_actual_changed_dates = sqlx::query!(
        "SELECT date FROM v_StdSiteJobs WHERE site_id = ? AND state = ?",
        "oc",
        StdSiteJobState::RegenNeeded
    )
    .fetch_all(&mut *conn)
    .await
    .expect("Getting changed oc rows should succeed")
    .into_iter()
    .map(|r| r.date)
    .collect_vec();

    assert_eq!(ci_actual_changed_dates, ci_expected_changed_dates);
    assert_eq!(ci_actual_new_dates, ci_expected_new_dates);
    assert_eq!(oc_actual_new_dates.len(), 0);
    assert_eq!(oc_actual_changed_dates, oc_expected_changed_dates)
}
