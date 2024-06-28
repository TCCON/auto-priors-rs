use std::time::{Duration, Instant};

use anyhow::Context;
use orm::{MySqlPC, jobs::{Job, JobState}, error::JobError};
use sqlx::{Connection, MySqlConnection};

mod common;

static TEST_QUEUE_NAME: &'static str = "default";


async fn mock_run_job_with_delay<'t>(conn: &mut MySqlConnection, delay_seconds: f32) -> anyhow::Result<i32> {
    let mut j = Job::get_next_job_in_queue(conn, TEST_QUEUE_NAME, &orm::jobs::PrioritySubmitFS{}).await
        .with_context(|| "Query to get next test job failed")?
        .expect("Expected at least one job in test queue");

    let delay = Duration::from_secs_f32(delay_seconds);
    tokio::time::sleep(delay).await;
    j.set_state(conn, JobState::Running).await
        .with_context(|| format!("Could not set job state to running after {delay_seconds} s artificial delay"))?;

    Ok(j.job_id)
}

async fn mock_run_job_with_delay_transaction(mut conn: MySqlPC, delay_seconds: f32) -> anyhow::Result<i32> {
    let mut tries = 0;
    while tries < 5 {
        tries += 1;
        let mut trans = conn.begin().await
            .with_context(|| "Could not begin transaction")?;

        println!("{:?} ({delay_seconds}): Got transaction", Instant::now());

        let mut j = Job::get_next_job_in_queue(&mut trans, TEST_QUEUE_NAME, &orm::jobs::PrioritySubmitFS{}).await
            .with_context(|| "Query to get next test job failed")?
            .expect("Expected at least one job in test queue");

        println!("{:?} ({delay_seconds}): Got job ID = {}", Instant::now(), j.job_id);
        
        let delay = Duration::from_secs_f32(delay_seconds);
        tokio::time::sleep(delay).await;

        match j.set_state(&mut trans, JobState::Running).await {
            Ok(nrows) => {
                trans.commit().await.expect("Could not commit transaction");
                println!("{:?} ({delay_seconds}): Set job state for {nrows} rows", Instant::now());
                return Ok(j.job_id)
            },
            Err(JobError::DeadlockError(_)) => {
                println!("{:?} ({delay_seconds}): Deadlocked, trying again", Instant::now());
                continue;
            },
            Err(e) => {
                return Err(e).with_context(|| format!("Could not set job state to running after {delay_seconds} s artificial delay"))
            }
        }
    }

    anyhow::bail!("Could not get next job after 5 tries")
}

// Because the tests use a database, if we're not using testcontainers to give each test its
// own database, we have to call the tests as `$ cargo test -- --test-threads=1` to ensure only
// one test runs at a time.
#[test_log::test(tokio::test)]
async fn test_next_job_no_transaction() {
    // We'll need two connections to the database for this test, so we'll handle initialization manually
    let (pool, _test_db) = common::open_test_database(true).await
        .expect("Could not open database");

    let mut conn1 = pool.get_connection().await.expect("Could not get first DB connection");
    let mut conn2 = pool.get_connection().await.expect("Could not get first DB connection");

    common::multiline_sql!("sql/two_test_jobs.sql", conn1);

    let fut1 = mock_run_job_with_delay(&mut conn1, 3.0);
    let fut2 = mock_run_job_with_delay(&mut conn2, 0.1);

    let (jid1, jid2) = tokio::join!(fut1, fut2);

    assert_eq!(jid1.unwrap(), 1, "First job selected was not the higher priority Job #1");
    assert_eq!(jid2.unwrap(), 1, "Second job selected did not duplicate the higher priority Job #1 without transactions, which was the expected outcome");
}

#[tokio::test]
async fn test_next_job_with_transaction() {
    // We'll need two connections to the database for this test, so we'll handle initialization manually
    let (pool, _test_db) = common::open_test_database(true).await
        .expect("Could not open database");

    let mut conn1 = pool.get_connection().await.expect("Could not get first DB connection");
    let conn2 = pool.get_connection().await.expect("Could not get second DB connection");

    common::multiline_sql!("sql/two_test_jobs.sql", conn1);
    let fut1 = mock_run_job_with_delay_transaction(conn1, 3.0);
    let fut2 = mock_run_job_with_delay_transaction(conn2, 1.0);

    let (jid1, jid2) = tokio::join!(fut1, fut2);

    // I added this in because I was having issues with the job state not being updated. It was just
    // me forgetting to commit the transaction, but I left it in because it's a nice check that things
    // went right.
    let mut conn3 = pool.get_connection().await.expect("Could not get third DB connection");
    let j1 = Job::get_job_with_id(&mut conn3, 1)
        .await
        .expect("Could not get job ID = 1 to verify state");

    assert_eq!(j1.state, JobState::Running, "The first job did not have its state set to 'running'");
    assert_ne!(jid1.unwrap(), jid2.unwrap(), "The two job runners picked up the same job; this should not have happened with transactions.")
}

#[tokio::test]
async fn test_claim_job() {
    // The difference between this and test_next_job_with_transaction is this tests the library
    // capability to claim a job with a transaction and deadlock checking

    // We'll need two connections to the database for this test, so we'll handle initialization manually
    let (pool, _test_db) = common::open_test_database(true).await
        .expect("Could not open database");

    let mut conn1 = pool.get_connection().await.expect("Could not get first DB connection");
    let mut conn2 = pool.get_connection().await.expect("Could not get second DB connection");
    common::multiline_sql!("sql/two_test_jobs.sql", conn1);


    let fut1 = mock_run_job_with_delay_transaction(conn1, 3.0);
    let fut2 = Job::claim_next_job_in_queue(&mut conn2, TEST_QUEUE_NAME, &orm::jobs::PrioritySubmitFS{});

    let (job1, job2) = tokio::join!(fut1, fut2);

    let job1 = job1.expect("Delayed job claim failed");
    let job2 = job2.expect("Standard job claim failed")
        .expect("Should have been at least one job in the test queue")
        .job_id;

    assert_ne!(job1, job2, "Job IDs should not be the same, this means the race condition was not caught");
    assert_eq!(job2, 1, "Expected the claim without delay to claim the higher priority job, this did not happen");

    let job2_check = Job::get_job_with_id(&mut conn2, job2)
        .await
        .expect("Failed to query the undelayed job");

    assert_eq!(job2_check.state, JobState::Running, "The undelayed job was not set to state 'running'");
}

#[tokio::test]
async fn test_set_job_state() {
    let (mut conn, _test_db) = multiline_sql_init!("sql/two_test_jobs.sql");
    let mut job = Job::get_job_with_id(&mut conn, 1)
        .await
        .expect("Could not get job with ID = 1");

    job.set_state(&mut conn, JobState::Running)
        .await
        .expect("Query to set job state to 'running' failed");

    let job2 = Job::get_job_with_id(&mut conn, 1)
        .await
        .expect("Could not get job with ID = 1 the second time");

    assert_eq!(job2.state, JobState::Running, "Job state was not set to 'running' in the database");
}
