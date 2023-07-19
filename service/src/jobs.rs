use orm::{jobs::Job, MySqlConn};
use tokio::process::Child;

pub struct JobRunner {
    job: Job,
    ginput_process: Option<Child>
}

impl JobRunner {
    pub async fn new_from_next_job(conn: &mut MySqlConn, queue: &str) -> anyhow::Result<Option<Self>> {
        let next_job = orm::jobs::Job::get_next_job_in_queue(conn, queue).await?;
        if let Some(job) = next_job {
            Ok(Some(Self { job, ginput_process: None }))
        } else {
            Ok(None)
        }
    }

    pub async fn start_job(&mut self, conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<()> {
        self.job.set_state(conn, orm::jobs::JobState::Running).await?;

        // Alright, this is going to be complicated, because it needs to look at the dates for the job
        // and check that they do not cross a boundary for which met/ginput version to use. That also should
        // be checked on submission, but it's also possible that the config changed since the job was submitted,
        // so we verify here too. (That check should go in the core-orm since it'll be used in a couple places.)
        // As long as the job falls entirely within one of the configured time periods, we can set up the ginput
        // run. Otherwise, we either error or break this into multiple calls.

        Ok(())
    }
}