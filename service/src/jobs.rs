use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use anyhow::Context;
use log::{warn, info, debug};
use orm::{jobs::{Job, JobState, GinputRunner}, MySqlConn, config::Config, MySqlPC};
use tokio::sync::{RwLock, watch::Receiver};

use crate::{ExitCommand, error::ErrorHandler};

const LUT_REGEN_BLOCKING_PRIORITY: i32 = 10;
static LUT_QUEUE_NAME: &'static str = "LUT_REGEN";


#[derive(Debug)]
pub(crate) struct JobManager<T: Queueable, H: ErrorHandler> {
    pub(crate) db_conn: MySqlPC,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) job_queues: HashMap<String, Queue<T>>,
    pub(crate) error_handler: H,
    pub(crate) exit_signal: Receiver<ExitCommand>
}

impl<T: Queueable, H: ErrorHandler> JobManager<T, H> {
    pub(crate) async fn scheduler_entry_point(&mut self) -> bool {
        // Ensure that the exit command is cloned/copied to avoid a deadlock:
        // https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html#method.borrow
        let sig = { self.exit_signal.borrow().to_owned() };
        match sig {
            ExitCommand::Continue => {
                // No signal to exit, keep going
                self.scan_for_job_submissions()
                    .await
                    .unwrap_or_else(|e| {
                        self.error_handler.report_error(e.as_ref())
                    });

                self.add_pending_jobs_to_queues()
                    .await
                    .unwrap_or_else(|e| {
                        self.error_handler.report_error(e.as_ref())
                    });

                self.start_queues_with_jobs()
                    .await
                    .unwrap_or_else(|e| {
                        self.error_handler.report_error(e.as_ref())
                    });
                return false;
            },
            ExitCommand::Graceful => {
                // Allow current jobs to finish, then exit
                self.wait_for_jobs_to_finish().await;
                info!("All current jobs complete, stopping job runner loop");
                return true;
            },
            ExitCommand::Rapid => {
                // Cancel running jobs, but take time to clean them up
                // and reset their status
                self.stop_and_reset_jobs().await;
                info!("All current jobs stopped and reset, stopping job runner loop");
                return true;
            }
        }
    }

    pub(crate) async fn schedule_lut_regen(&mut self) {
        // For each ginput defined in the config, add a blocking job to the special queue
        // to regenerate its LUTs
        let lut_queue = self.job_queues.entry(LUT_QUEUE_NAME.to_string())
            .or_insert_with(|| Queue::new_blocking(usize::MAX, LUT_REGEN_BLOCKING_PRIORITY));

        let ginput_keys: Vec<_> = {
            let config = self.shared_config.read().await;
            config.execution.ginput.keys()
                .map(|k| k.to_string())
                .collect()
        };

        for key in ginput_keys {
            let lut_job = T::new_lut_job(key.clone());
            if !lut_queue.add(lut_job).await {
                self.error_handler.report_error(
                    anyhow::anyhow!("Failed to add job to regenerate LUTs for ginput key '{key}'").as_ref()
                )
            }
        }
    }

    pub(crate) async fn scan_for_job_submissions(&mut self) -> anyhow::Result<()> {
        let (input_glob_pattern, save_dir) = { 
            let c = self.shared_config
                .read().await;
            let igp = c.execution.input_file_pattern.clone();
            let sd = c.execution.output_path.clone();
            (igp, sd)
        };
        info!("Scanning for new input files matching {input_glob_pattern}");

        let input_files = glob::glob(&input_glob_pattern)
            .with_context(|| format!("Globbing for input files in {input_glob_pattern} failed"))?
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("Error collecting input files in {input_glob_pattern}"))?;

        info!("{} new input files found", input_files.len());
        
        orm::input_files::add_jobs_from_input_files(&mut self.db_conn, &input_files, &save_dir).await
    }

    pub(crate) async fn add_pending_jobs_to_queues(&mut self) -> anyhow::Result<()> {
        let queue_names = self.get_all_queue_names().await?;

        info!("{} queues to add jobs for", queue_names.len());
        for name in queue_names {
            let queue_options = self.shared_config
                .read()
                .await
                .get_queue(&name)
                .unwrap_or_default();

            let this_queue = if self.job_queues.contains_key(&name) {
                self.job_queues.get_mut(&name).unwrap()
            } else {
                let new_queue = Queue::new(queue_options.max_num_procs);
                self.job_queues.insert(name.clone(), new_queue);
                self.job_queues.get_mut(&name).unwrap()
            };

            this_queue.clean_up_finished(&mut self.db_conn, &self.error_handler).await;
            let mut n_to_add = this_queue.num_can_add();
            while n_to_add > 0 {
                let next_job = Job::claim_next_job_in_queue(&mut self.db_conn, &name)
                    .await
                    .with_context(|| format!("Error occurred while trying to claim the next job in queue '{name}'"))?;

                let next_job = if let Some(j) = next_job {
                    j
                } else {
                    break;
                };

                let job_id = next_job.job_id;
                let runner = T::new_from_job(
                    next_job, 
                    & *self.shared_config.read().await,
                );
                
                if !this_queue.add(runner).await {
                    // Really we should not enter this block; the loop over n_to_add should ensure we only
                    // add as many jobs as we are allow to. But just in case, we should ensure that a job not
                    // added to the queue gets reset to 'pending'
                    let mut job = Job::get_job_with_id(&mut self.db_conn, job_id)
                        .await
                        .with_context(|| format!("Could not add job ID {job_id} to queue {name} and failed get it from the database to reset its state to 'pending'!"))?;

                    job.set_state(&mut self.db_conn, JobState::Pending)
                        .await
                        .with_context(|| format!("Could not add job ID {job_id} to queue {name} and failed to reset its state to 'pending'!"))?;

                    warn!("Tried to add job ID #{job_id} to queue '{name}', but queue refused the job. This should not happen, but the job was successfully reset to 'pending'.");
                    break;
                } else {
                    n_to_add -= 1;
                }
            }

        }

        Ok(())
    }

    async fn start_queues_with_jobs(&mut self) -> anyhow::Result<()> {
        let pending_queues = self.get_all_queue_names().await?;

        for queue_name in pending_queues.iter() {
            if self.can_queue_start_jobs(queue_name).await? {
                let conn = &mut self.db_conn;
                let config = self.shared_config.read().await;
                if let Some(queue) = self.job_queues.get_mut(queue_name) {
                    queue.start(conn, &config, &self.error_handler).await;
                }
            } 
        }

        Ok(())
    }

    async fn get_all_queue_names(&mut self) -> anyhow::Result<Vec<String>> {
        let mut queue_names = Job::get_queues_with_pending_jobs(&mut self.db_conn)
            .await
            .with_context(|| "Error occurred while trying to retrieve the list of queues with pending jobs")?;

        for extant_name in self.job_queues.keys() {
            if !queue_names.contains(extant_name) {
                queue_names.push(extant_name.to_string());
            }
        }

        Ok(queue_names)
    }

    async fn can_queue_start_jobs(&self, queue_name: &str) -> anyhow::Result<bool> {
        
        let queue_bp = self.job_queues
            .get(queue_name)
            .map(|q| q.blocking_priority())
            .unwrap_or(0);

        for (other_name, other_queue) in self.job_queues.iter() {
            let other_bp = other_queue.blocking_priority();
            if other_bp < queue_bp {
                if other_queue.has_running_jobs() {
                    info!("Queue '{queue_name}' cannot start jobs because it is waiting for jobs in queue {other_name} to finish");
                    return Ok(false)
                }

            }

            if other_bp > queue_bp {
                if other_queue.num_jobs_left() > 0 {
                    info!("Queue '{queue_name}' cannot start jobs because a higher blocking priority queue ({other_name}) has jobs pending");
                    return Ok(false)
                }
            }
        }

        Ok(true)
    }

    async fn wait_for_jobs_to_finish(&mut self) {
        loop {
            let mut njobs = 0;
            for (name, queue) in self.job_queues.iter_mut() {
                queue.clean_up_finished(&mut self.db_conn, &self.error_handler).await;
                njobs += queue.num_jobs_left();
                debug!("Jobs remaining in {name}: {}", queue.num_jobs_left());
            }

            if njobs == 0 {
                return;                
            } else {
                info!("{njobs} still running, waiting for them to complete");
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        }
    }

    async fn stop_and_reset_jobs(&mut self) {
        for (name, queue) in self.job_queues.iter_mut() {
            queue.cancel_running_jobs(&mut self.db_conn, &self.error_handler).await;
            info!("Stopped and reset all jobs in {name} queue");
        }
    }
}


#[async_trait]
pub(crate) trait Queueable {
    fn new_from_job(job: Job, config: &Config) -> Self;
    fn new_lut_job(ginput_key: String) -> Self;
    async fn start(&mut self, conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()>;
    fn is_running(&self) -> bool;
    async fn is_done(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool>;
    async fn cancel(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool>;
}

#[derive(Debug)]
pub(crate) struct Queue<T: Queueable> {
    max_num_items: usize,
    items: Vec<T>,
    blocking_priority: i32
} 

impl<T: Queueable> Queue<T> {
    pub fn new(max_num_items: usize) -> Self {
        Self { max_num_items, items: Vec::new(), blocking_priority: 0 }
    }

    pub fn new_blocking(max_num_items: usize, blocking_priority: i32) -> Self {
        Self { max_num_items, items: Vec::new(), blocking_priority }
    }

    pub fn blocking_priority(&self) -> i32 {
        self.blocking_priority
    }

    /// Check whether a new item can be added to this queue
    pub fn can_add(&self) -> bool {
        self.items.len() < self.max_num_items
    }

    /// Return how many items can be added to this queue
    pub fn num_can_add(&self) -> usize {
        let n = self.items.len();
        if n >= self.max_num_items {
            0
        } else {
            self.max_num_items - n
        }
    }

    /// Add a new item to the queue, if possible.
    /// 
    /// Returns `true` if the item was added, `false` otherwise.
    /// If the item was added, its `start` method is called.
    pub async fn add(&mut self, item: T) -> bool {
        if !self.can_add() {
            false
        } else {
            self.items.push(item);
            true
        }
    }

    pub async fn start(&mut self, conn: &mut MySqlConn, config: &Config, error_handler: &dyn ErrorHandler) {
        for item in self.items.iter_mut() {
            if !item.is_running() {
                item.start(conn, config).await
                .unwrap_or_else(|e| error_handler.report_error(e.as_ref()));
            }
        }
    }

    /// Cleans up items in the queue that have completed.
    /// 
    /// Typically, you would call this method before `add`
    /// or `num_can_add` to remove any completed jobs to
    /// make room for new ones.
    pub async fn clean_up_finished(&mut self, conn: &mut MySqlConn, error_handler: &dyn ErrorHandler) {
        let old_items = std::mem::take(&mut self.items);
        for mut item in old_items {
            let still_running = match item.is_done(conn).await {
                Ok(done) => !done,
                Err(e) => {
                    error_handler.report_error(e.as_ref());
                    // Assume that we shouldn't keep the job in the queue if there was an error - this
                    // either means the job failed or we lost the abillity to check if it's running.
                    false
                }
            };
            if still_running {
                self.items.push(item);
            }
        }
    }

    pub fn num_jobs_left(&self) -> usize {
        self.items.len()
    }

    #[allow(dead_code)] // used in unit tests
    pub fn num_jobs_running(&self) -> usize {
        self.items.iter()
            .fold(0, |acc, el| {
                if el.is_running() {
                    acc + 1
                }else{
                    acc
                }
            })
    }

    pub fn has_running_jobs(&self) -> bool {
        self.items.iter()
            .any(|i| i.is_running())
    }

    pub async fn cancel_running_jobs(&mut self, conn: &mut MySqlConn, error_handler: &dyn ErrorHandler) {
        for item in self.items.iter_mut() {
            item.cancel(conn).await
                .map(|_| ()) // need this to avoid a type error on the unwrap; don't care if there was a task to cancel
                .unwrap_or_else(|e| error_handler.report_error(e.as_ref()));
        }
    }
}

#[derive(Debug)]
pub enum ServiceJobRunner {
    GinputJob{job: Job, inner_runner: Option<GinputRunner>},
    LutRegenJob{ginput_key: String, inner_runner: Option<GinputRunner>}
}

impl ServiceJobRunner {

    async fn is_ginput_job_done(job: &mut Job, inner_runner: &mut Option<GinputRunner>, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        let task = if let Some(runner) = inner_runner {
            runner
        } else {
            return Ok(false)
        };

        let res = task.is_done()
            .with_context(|| format!(
                "Error checking if job #{} is done", job.job_id
            ))?;
        match res {
            orm::jobs::RunState::InProgress => Ok(false),
            orm::jobs::RunState::Complete => {
                // todo: handle setting output path to the tar file when appropriate
                let output_path = job.save_dir.clone();
                job.set_completed(conn, &output_path, None)
                    .await?;
                return Ok(true);
            },
            orm::jobs::RunState::Errored => {
                job.set_state(conn, JobState::Errored).await?;
                anyhow::bail!("ginput error occurred in job #{}", job.job_id);
            },
        }
    }

    async fn is_lut_job_done(ginput_key: &str, inner_runner: &mut Option<GinputRunner>) -> anyhow::Result<bool> {
        let task = if let Some(runner) = inner_runner {
            runner
        } else {
            return Ok(false)
        };

        let res = task.is_done()
            .with_context(|| format!("Error checking if an LUT regeneration for ginput key '{ginput_key}' is done"))?;

        match res {
            orm::jobs::RunState::InProgress => Ok(false),
            orm::jobs::RunState::Complete => Ok(true),
            orm::jobs::RunState::Errored => Err(anyhow::anyhow!("LUT regeneration for ginput key '{ginput_key}' errored")),
        }
    }

    async fn start_ginput_job(job: &mut Job, inner_runner: &mut Option<GinputRunner>, config: &Config, conn: &mut MySqlConn) -> anyhow::Result<()> {
        // The job state is set by the manager when it claims it
        
        // Alright, this is going to be complicated, because it needs to look at the dates for the job
        // and check that they do not cross a boundary for which met/ginput version to use. That also should
        // be checked on submission, but it's also possible that the config changed since the job was submitted,
        // so we verify here too. (That check should go in the core-orm since it'll be used in a couple places.)
        // As long as the job falls entirely within one of the configured time periods, we can set up the ginput
        // run. Otherwise, we either error or break this into multiple calls.
        let date_iter = orm::utils::DateIterator::new(
            vec![(job.start_date, job.end_date)]
        );

        for date in date_iter {
            let ginput_key = if let Some(key) = &job.ginput_key {
                key
            } else {
                let defaults = config.get_defaults_for_date(date)
                    .with_context(|| format!("Could not get defaults for date {date}; occurred while trying to start ginput for job {}", job.job_id))?;
                &defaults.ginput
            };

            let ginput = config.execution.ginput.get(ginput_key)
                .ok_or_else(|| anyhow::anyhow!("Ginput key '{ginput_key}', required by job #{}, is not defined in the configuration", job.job_id))?;
            
            let task_res = ginput.start_job_for_date(conn, date, job, config)
                .await;

            let task = match task_res {
                Ok(c) => c,
                Err(e) => {
                    // Could not start the job for whatever reason; set the job state to "errored" and exit
                    job.set_state(conn, JobState::Errored).await?;
                    return Err(e).with_context(|| format!(
                        "Was not able to start job #{} for date {date}", job.job_id
                    ));
                }
            };
            *inner_runner = Some(task);

            // I'd originally worried that there could be a race condition between this method and cancel,
            // but since both require mut refs and you can't have >1 mut ref to the same object at a time,
            // a race should actually be impossible.
        }

        Ok(())
    }

    async fn start_lut_job(ginput_key: &str, inner_runner: &mut Option<GinputRunner>, config: &Config) -> anyhow::Result<()> {
        let ginput = config.execution.ginput.get(ginput_key)
            .ok_or_else(|| anyhow::anyhow!("start_lut_job callled with invalid ginput key: '{ginput_key}'"))?;

        let task = ginput.start_lut_regen().await
            .with_context(|| format!("Error occurred while trying to start LUT regen job for ginput key '{ginput_key}'"))?;

        *inner_runner = Some(task);
        Ok(())
    }

    async fn cancel_ginput_job(job: &mut Job, inner_runner: &mut Option<GinputRunner>, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        if let Some(task) = inner_runner {
            task.cancel_ginput_job(conn, Some(job))
                .await
                .with_context(|| format!("Error occurred while trying to stop job #{}", job.job_id))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn cancel_lut_job(ginput_key: &str, inner_runner: &mut Option<GinputRunner>, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        if let Some(task) = inner_runner {
            task.cancel_ginput_job(conn, None)
                .await
                .with_context(|| format!("Error occurred while trying to cancel LUT regeneration for ginput key '{ginput_key}'"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl Queueable for ServiceJobRunner {

    fn new_from_job(job: Job, _config: &Config) -> Self {
        Self::GinputJob { job, inner_runner: None }
    }

    fn new_lut_job(ginput_key: String) -> Self {
        Self::LutRegenJob { ginput_key, inner_runner: None }
    }

    fn is_running(&self) -> bool {
        // I don't think there can be a race condition between this and start - since start
        // takes a mutable ref, it can't be called at the same time as this function,
        // so we should never have a case where start is in progress when this is called
        match self {
            Self::GinputJob{job: _, inner_runner} => inner_runner.is_some(),
            Self::LutRegenJob{ginput_key: _, inner_runner} => inner_runner.is_some()
        }
    }

    async fn is_done(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        match self {
            Self::GinputJob{job, inner_runner} => Self::is_ginput_job_done(job, inner_runner, conn).await,
            Self::LutRegenJob{ginput_key, inner_runner} => Self::is_lut_job_done(&ginput_key, inner_runner).await
        }
    }

    async fn start(&mut self, conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()> {
        match self {
            Self::GinputJob{job, inner_runner} => Self::start_ginput_job(job, inner_runner, config, conn).await,
            Self::LutRegenJob{ginput_key, inner_runner} => Self::start_lut_job(ginput_key, inner_runner, config).await,
        }
    }

    async fn cancel(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        match self {
            Self::GinputJob{job, inner_runner} => Self::cancel_ginput_job(job, inner_runner, conn).await,
            Self::LutRegenJob{ginput_key, inner_runner} => Self::cancel_lut_job(ginput_key, inner_runner, conn).await,
        }
        
    }

    
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::error::LoggingErrorHandler;

    use super::*;

    const TEST_QUEUE_NAME: &'static str = "TEST";
    const TEST_QUEUE_MAX_NUM_ITEMS: usize = 2;

    #[derive(Debug, Clone)]
    struct DummyJobRunner {
        delay: Duration,
        start_time: Option<Instant>
    }

    impl DummyJobRunner {
        fn new_from_seconds(seconds: u64) -> Self {
            let delay = Duration::from_secs(seconds);
            Self { delay, start_time: None }
        }
    }

    #[async_trait]
    impl Queueable for DummyJobRunner {
        fn new_from_job(_job: Job, _config: &Config) -> Self {
            Self::new_from_seconds(5)
        }

        fn new_lut_job(_ginput_key: String) -> Self {
            Self::new_from_seconds(5)
        }

        async fn start(&mut self, _conn: &mut MySqlConn, _config: &Config) -> anyhow::Result<()> {
            self.start_time = Some(Instant::now());
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.start_time.is_some()
        }

        async fn is_done(&mut self, _conn: &mut MySqlConn) -> anyhow::Result<bool> {
            let start = if let Some(t) = self.start_time {
                t
            } else {
                return Ok(false)
            };

            Ok(Instant::now().duration_since(start) > self.delay)
        }

        async fn cancel(&mut self, _conn: &mut MySqlConn) -> anyhow::Result<bool> {
            Ok(true)
        }
    }

    async fn make_dummy_job_manager() -> JobManager<DummyJobRunner, LoggingErrorHandler> {
        let db_url = orm::get_database_url(None).expect("Could not get database URL");
        let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

        let mut config = orm::config::Config::default();
        config.execution.queues.insert(
            TEST_QUEUE_NAME.to_string(), 
            orm::config::JobQueueOptions{ max_num_procs: TEST_QUEUE_MAX_NUM_ITEMS }
        );

        let (_, exit_rx) = tokio::sync::watch::channel(ExitCommand::Continue);

        JobManager {
            db_conn: db.get_connection().await.expect("Failed to initialize database connection for job manager"),
            shared_config: Arc::new(RwLock::new(config)),
            job_queues: HashMap::new(),
            error_handler: LoggingErrorHandler{},
            exit_signal: exit_rx,
        }
    }

    // TODO: test that:
    // 1) [x] Already running standard jobs prevent the LUT jobs from starting
    // 2) [x] The presence of LUT jobs in the queue prevent new standard jobs from starting
    // 3) [ ] Once LUT jobs finish, standard jobs are allowed to start
    #[tokio::test]
    async fn test_lut_with_running_jobs() {
        let mut manager = make_dummy_job_manager().await;
        let test_job = DummyJobRunner::new_from_seconds(u64::MAX);
        let mut std_queue = Queue::new(TEST_QUEUE_MAX_NUM_ITEMS);
        for _ in 0..TEST_QUEUE_MAX_NUM_ITEMS {
            std_queue.add(test_job.clone()).await;
        }
        manager.job_queues.insert(TEST_QUEUE_NAME.to_string(), std_queue);
        manager.start_queues_with_jobs().await.unwrap();

        let n_running = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_running, 2, "Did not have the correct number of standard jobs running");

        let lut_job = DummyJobRunner::new_lut_job("bob".to_string());
        let mut lut_queue = Queue::new_blocking(usize::MAX, LUT_REGEN_BLOCKING_PRIORITY);
        lut_queue.add(lut_job).await;
        manager.job_queues.insert("LUT".to_string(), lut_queue);
        manager.start_queues_with_jobs().await.unwrap();

        let n_running = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        let n_lut_running = manager.job_queues
            .get("LUT")
            .unwrap()
            .num_jobs_running();

        assert_eq!(n_running, 2, "Did not have the correct number of standard jobs running after adding the LUT job");
        assert_eq!(n_lut_running, 0, "The LUT job started when it should not have");
    }

    #[tokio::test]
    async fn test_lut_blocks_std_jobs() {
        let mut manager = make_dummy_job_manager().await;
        let test_job = DummyJobRunner::new_from_seconds(u64::MAX);
        let mut std_queue = Queue::new(TEST_QUEUE_MAX_NUM_ITEMS);
        std_queue.add(test_job.clone()).await;
        manager.job_queues.insert(TEST_QUEUE_NAME.to_string(), std_queue);
        manager.start_queues_with_jobs().await.unwrap();

        let n_running_before = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_running_before, 1, "Did not have the correct number of standard jobs running before adding the LUT job");

        manager.job_queues
            .get_mut(TEST_QUEUE_NAME)
            .unwrap()
            .add(test_job).await;

        let lut_job = DummyJobRunner::new_lut_job("bob".to_string());
        let mut lut_queue = Queue::new_blocking(usize::MAX, LUT_REGEN_BLOCKING_PRIORITY);
        lut_queue.add(lut_job).await;
        manager.job_queues.insert("LUT".to_string(), lut_queue);
        manager.start_queues_with_jobs().await.unwrap();

        let n_running_after = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        let n_in_queue = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_left();
        let n_lut_running = manager.job_queues
            .get("LUT")
            .unwrap()
            .num_jobs_running();
        let n_lut_in_queue = manager.job_queues
            .get("LUT")
            .unwrap()
            .num_jobs_left();

        assert_eq!(n_in_queue, 2, "Did not have the right number of standard jobs after adding the LUT job");
        assert_eq!(n_running_after, 1, "The second standard job started even though there was an LUT job pending");
        assert_eq!(n_lut_in_queue, 1, "Did not have 1 LUT job in its queue");
        assert_eq!(n_lut_running, 0, "The LUT job started when it should not have");
    }

    #[tokio::test]
    async fn test_jobs_unblock() {
        let mut manager = make_dummy_job_manager().await;
        let test_job = DummyJobRunner::new_from_seconds(5);
        let mut std_queue = Queue::new(TEST_QUEUE_MAX_NUM_ITEMS);
        std_queue.add(test_job.clone()).await;
        manager.job_queues.insert(TEST_QUEUE_NAME.to_string(), std_queue);

        let lut_job = DummyJobRunner::new_lut_job("bob".to_string());
        let mut lut_queue = Queue::new_blocking(usize::MAX, LUT_REGEN_BLOCKING_PRIORITY);
        lut_queue.add(lut_job).await;

        manager.scheduler_entry_point().await;
        let n_running = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_running, 1, "Did not have the correct number of standard jobs running before adding the LUT job");

        manager.job_queues.insert("LUT".to_string(), lut_queue);
        manager.job_queues
            .get_mut(TEST_QUEUE_NAME)
            .unwrap()
            .add(test_job).await;
        manager.scheduler_entry_point().await;

        let n_running = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_running, 1, "Did not have the correct number of standard jobs running after adding the LUT job");

        let n_lut_running = manager.job_queues
            .get("LUT")
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_lut_running, 0, "LUT job started before it should have");

        // Now wait long enough that the first standard job should *definitely* finish
        tokio::time::sleep(Duration::from_secs(10)).await;
        
        manager.scheduler_entry_point().await;
        let n_in_queue = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_left();
        assert_eq!(n_in_queue, 1, "Did not clean up finished first job as expected");

        let n_running = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_running, 0, "Did not have the correct number of standard jobs running after waiting for the first job to finish");

        let n_lut_running = manager.job_queues
            .get("LUT")
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_lut_running, 1, "LUT job should have started");

        // Now wait for the LUT job to finish
        tokio::time::sleep(Duration::from_secs(10)).await;

        manager.scheduler_entry_point().await;

        let n_lut_in_queue = manager.job_queues
            .get("LUT")
            .unwrap()
            .num_jobs_left();
        assert_eq!(n_lut_in_queue, 0, "LUT job should have been cleaned up");

        let n_in_queue = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_left();
        assert_eq!(n_in_queue, 1, "Did not have the expected number of standard jobs left in the queue after the LUT job finished");

        let n_running = manager.job_queues
            .get(TEST_QUEUE_NAME)
            .unwrap()
            .num_jobs_running();
        assert_eq!(n_running, 1, "Did not start the second standard job after the LUT job finished");

        

    }
}