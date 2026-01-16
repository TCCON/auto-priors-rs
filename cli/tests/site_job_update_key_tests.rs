use std::path::PathBuf;

use itertools::Itertools;
use orm::{
    config::{DefaultOptions, ProcCfgKey},
    multiline_sql, multiline_sql_init,
    test_utils::{init_logging, make_dummy_config},
};
use tccon_priors_cli::stdsites::update_processing_key;

mod common;

/// Test that running without filters updates the processing key for all rows
#[tokio::test]
async fn test_update_row_proc_key() {
    init_logging();
    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    update_processing_key(&mut conn, &config, Some(&new_key), None, None, None, None)
        .await
        .unwrap();

    // If the number of rows where the processing key was changed matched the total, this worked.
    let n_with_new_key = sqlx::query!(
        "SELECT COUNT(*) as count FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap()
    .count;

    let n_any_key = sqlx::query!("SELECT COUNT(*) as count FROM v_StdSiteJobs")
        .fetch_one(&mut *conn)
        .await
        .unwrap()
        .count;

    assert_eq!(
        n_with_new_key, n_any_key,
        "Not all rows had their processing key updated"
    );
}

/// Test that running with the old key specified only updates rows with that
/// as the current processing key
#[tokio::test]
async fn test_update_row_proc_key_filter_old_key() {
    init_logging();
    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    let old_key = ProcCfgKey("ALPHA".to_string());

    let ids_with_old_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        old_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    update_processing_key(
        &mut conn,
        &config,
        Some(&new_key),
        Some(&old_key),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    // Check that both the rows with the new key match those that had the old one,
    // and that all rows with the old key are gone.
    let ids_with_new_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    assert_eq!(
        ids_with_old_key, ids_with_new_key,
        "The rows that have the new key are not those that had the old key"
    );

    let n_with_old_key_left = sqlx::query!(
        "SELECT COUNT(*) as count FROM v_StdSiteJobs WHERE processing_key = ?",
        old_key
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap()
    .count;

    assert_eq!(
        n_with_old_key_left, 0,
        "There were rows with the old key remaining."
    );
}

/// Test that running with the site ID specified only updates rows for that
/// site ID
#[tokio::test]
async fn test_update_row_proc_key_filter_site_id() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    update_processing_key(
        &mut conn,
        &config,
        Some(&new_key),
        None,
        None,
        None,
        Some("pa"),
    )
    .await
    .unwrap();

    // Confirm that the rows with the new key match the ones with "pa" as the site ID
    let ids_with_new_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    let ids_with_site = sqlx::query!("SELECT id FROM v_StdSiteJobs WHERE site_id = ?", "pa")
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect_vec();

    assert_eq!(
        ids_with_new_key, ids_with_site,
        "Rows with updated processing key do not match those with the intended site"
    );
}

/// Test that running with first date specified only updates
/// rows for jobs on or after that date.
#[tokio::test]
async fn test_update_row_proc_key_filter_dates_after() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    let first_date = chrono::NaiveDate::from_ymd_opt(2023, 6, 1).unwrap();
    update_processing_key(
        &mut conn,
        &config,
        Some(&new_key),
        None,
        Some(first_date),
        None,
        None,
    )
    .await
    .unwrap();

    // Confirm that the rows with the new key match the ones a date greater than or
    // equal to the above date
    let ids_with_new_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    let ids_after_date = sqlx::query!("SELECT id FROM v_StdSiteJobs WHERE date >= ?", first_date)
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect_vec();

    assert_eq!(
        ids_with_new_key, ids_after_date,
        "Rows with updated processing key do not match those on or after the given date ({})",
        first_date
    );
}

/// Test that running with first date specified only updates
/// rows for jobs before that date.
#[tokio::test]
async fn test_update_row_proc_key_filter_dates_before() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    let last_date = chrono::NaiveDate::from_ymd_opt(2023, 6, 2).unwrap();
    update_processing_key(
        &mut conn,
        &config,
        Some(&new_key),
        None,
        None,
        Some(last_date),
        None,
    )
    .await
    .unwrap();

    // Confirm that the rows with the new key match the ones a date greater than or
    // equal to the above date
    let ids_with_new_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    let ids_before_date = sqlx::query!("SELECT id FROM v_StdSiteJobs WHERE date < ?", last_date)
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect_vec();

    assert_eq!(
        ids_with_new_key, ids_before_date,
        "Rows with updated processing key do not match those before the given date ({})",
        last_date
    );
}

/// Test that running with first and last date specified only updates
/// rows for jobs between those dates.
#[tokio::test]
async fn test_update_row_proc_key_filter_dates_between() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    let first_date = chrono::NaiveDate::from_ymd_opt(2023, 6, 1).unwrap();
    let last_date = chrono::NaiveDate::from_ymd_opt(2023, 6, 2).unwrap();
    update_processing_key(
        &mut conn,
        &config,
        Some(&new_key),
        None,
        Some(first_date),
        Some(last_date),
        None,
    )
    .await
    .unwrap();

    // Confirm that the rows with the new key match the ones a date greater than or
    // equal to the above date
    let ids_with_new_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    let ids_between_dates = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE date >= ? AND date < ?",
        first_date,
        last_date
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    assert_eq!(
        ids_with_new_key, ids_between_dates,
        "Rows with updated processing key do not match those on or after the given date range ({} to {})",
        first_date,
        last_date
    );
}

/// Test that running with no new key specified uses the defaults from the
/// configuration.
#[tokio::test]
async fn test_update_row_proc_key_to_defaults() {
    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let defaults = &config.default_options;
    let (split_date, first_key, second_key) = get_correct_keys_for_defaults_test(defaults);

    update_processing_key(&mut conn, &config, None, None, None, None, None)
        .await
        .unwrap();

    let ids_with_first_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        first_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    let ids_with_second_key = sqlx::query!(
        "SELECT id FROM v_StdSiteJobs WHERE processing_key = ?",
        second_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.id)
    .collect_vec();

    let ids_before_split = sqlx::query!("SELECT id FROM v_StdSiteJobs WHERE date < ?", split_date)
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect_vec();

    let ids_after_split = sqlx::query!("SELECT id FROM v_StdSiteJobs WHERE date >= ?", split_date)
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect_vec();

    // For debugging - helpful to see what keys are assigned if the test fails
    let keys = sqlx::query!("SELECT processing_key FROM v_StdSiteJobs")
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.processing_key)
        .collect_vec();
    dbg!(keys);
    // End debugging

    assert_eq!(
        ids_with_first_key, ids_before_split,
        "Rows before the change in default did not match the rows with that key"
    );
    assert_eq!(
        ids_with_second_key, ids_after_split,
        "Rows after the change in default did not match the rows with that key"
    );
}

fn get_correct_keys_for_defaults_test(
    defaults: &[DefaultOptions],
) -> (chrono::NaiveDate, ProcCfgKey, ProcCfgKey) {
    // To keep the logic simple-ish, I want to ensure that there are only two defaults.
    // If we need to test a 3 default config, we'll handle that in the future.
    assert_eq!(
        defaults.len(),
        2,
        "Test is designed for 2 default processing configurations"
    );

    let mut first_key = None;
    let mut second_key = None;
    let mut split_date = None;

    for def in defaults {
        if def.end_date.is_some() && def.start_date.is_none() {
            first_key = Some(def.processing_configuration.clone());
            if split_date.is_none() {
                split_date = Some(def.end_date.unwrap());
            } else if split_date != def.end_date {
                panic!("Configuration has defaults that don't define a single split date.");
            }
        } else if def.start_date.is_some() && def.end_date.is_none() {
            second_key = Some(def.processing_configuration.clone());
            if split_date.is_none() {
                split_date = Some(def.start_date.unwrap());
            } else if split_date != def.start_date {
                panic!("Configuration has defaults that don't define a single split date.");
            }
        } else {
            panic!("Configuration has a default with a start and end date (not expected for this test)")
        }
    }

    let first_key =
        first_key.expect("Should have found a default with an end date and no start date");
    let second_key =
        second_key.expect("Should have found a default with a start date and no end date");
    let split_date = split_date.expect("Should have found the split date between two defaults");
    (split_date, first_key, second_key)
}

/// Test that running with old key, site ID, and dates specified only updates
/// the rows that match all of the criteria.
#[tokio::test]
async fn test_update_row_proc_key_filter_multiple() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/stdsites/init_test_sites.sql");
    multiline_sql!("sql/stdsites/update_proc_key.sql", conn);
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let new_key = ProcCfgKey("NEW".to_string());
    let old_key = ProcCfgKey("BETA".to_string());
    let last_date = chrono::NaiveDate::from_ymd_opt(2023, 6, 2).unwrap();
    let site_id = "ci";
    update_processing_key(
        &mut conn,
        &config,
        Some(&new_key),
        Some(&old_key),
        None,
        Some(last_date),
        Some(site_id),
    )
    .await
    .unwrap();

    let rows = sqlx::query!(
        "SELECT * FROM v_StdSiteJobs WHERE processing_key = ?",
        new_key
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "Expected only one row to match the multiple filters"
    );
    let row = &rows[0];
    assert_eq!(
        row.date,
        chrono::NaiveDate::from_ymd_opt(2023, 6, 1).unwrap()
    );
    assert_eq!(row.site_id.as_deref(), Some(site_id));
}
