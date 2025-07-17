use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use anyhow::Context;
use log::{warn, info, debug, error};
use orm::{jobs::{Job, JobState, start_priors_gen_job, GinputHandle, start_lut_regen_job}, config::Config, MySqlPC, PoolWrapper};
use sqlx::Connection;
use tokio::sync::RwLock;

use crate::error::ErrorHandler;

const LUT_REGEN_BLOCKING_PRIORITY: i32 = 10;
static LUT_QUEUE_NAME: &'static str = "LUT_REGEN";

#[derive(Debug, Clone, Copy)]
pub(crate) enum JobMessage {
    StartJobs,
    RegenLut,
    CleanUpJobs,
    SendStatusReports,
    StopGracefully,
    StopRapidly,
}

/// A manager for parsing job input files, starting jobs, and regenerating the chemical LUTs
/// 
/// Processing automatic ginput jobs as part of the systemd service requires several steps:
/// 
/// 1. *Parse input files:* this requires scanning for text files matching the `input_file_pattern`
///    in the configuration and entering the request into the `Jobs` database table. 
/// 2. *Regenerate the chemical LUTs if needed:* `ginput` needs to calculate look up tables for
///    stratospheric CO2, N2O, and CH4. Calculating these takes a fair amount of time, so we don't
///    do it for every run. Instead this should be done once and the tables stored on disk for reuse.
///    `ginput` can do that automatically, but if we let each job make that determination, we run the 
///    risk of corrupting the LUT files with multiple processes each writing to the files at the same
///    time. Instead, this needs to be done as a special job periodically (at least once a month, but
///    once a day is safer). See **Usage** and **Queues** below.
/// 3. *Starting queued jobs:* once jobs are in the `Jobs` table, we need to launch `ginput` for each
///    one, but limit the maximum number to avoid monopolizing all the resources on our system. See
///    **Queues** below.
/// 
/// # Usage
/// 
/// 1. Instantiate an instance using either `new_from_pool` or `new`. The former is 
///    preferred when you will already have a pool of database connections that we
///    can get a database connection from:
/// 
/// ```ignore
/// // orm is the tccon_priors_orm crate
/// let db_url = orm::get_database_url(None)?;
/// let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();
/// let config = orm::config::load_config_file_or_default(None).unwrap();
/// let job_manager = JobManager::new_from_pool(
///     &db, Arc::new(RwLock::new(config)), LoggingErrorHandler {}
/// )
/// 
/// // now we can reuse the `db` pool for other components that need database connections
/// ```
/// 
/// 2. Periodically call the [`JobManager::scheduler_entry_point`] method. This is intended
///    to be called from within a [`clokwerk::AsyncScheduler`] job, but could also just be
///    called in a loop. Each time this method is called, it reads any job input files and
///    starts as many of the next highest priority jobs as it is allowed.
/// 
/// 3. About once per day, call the [`JobManager::schedule_lut_regen`] method. This will
///    add a special set of jobs to regenerate the `ginput` chemical look up tables at the
///    next opportunity.
/// 
/// 4. To stop, call the [`JobManager::stop_jobs`] method. If called with [`ExitCommand::Graceful`],
///    this will allow any currently running jobs to finish, then return. If called with 
///    [`ExitCommand::Rapid`], it will immediately stop the jobs, but clean up their run
///    directories and reset their status in the database to "pending". 
/// 
/// # Queues
/// In this automation code, we use "queue" to refer to two similar but slightly different
/// concepts. The first is what set of resources a particular job in the database table is
/// allowed to call on. For example, we could define a "requests" queue which manually 
/// requested jobs go into and a "stdsites" queue, which the jobs to generate the priors
/// for standard TCCON and EM27 sites go into. The "requests" queue might be allowed to use
/// at most 4 processors, so users can't overwhelm our system, but the "stdsites" queue might
/// get 10 processors, since we can schedule it for times when the system isn't being heavily
/// used.
/// 
/// The second use of "queue" refers to the list of actively running jobs maintained by the
/// `JobManager` as instances of [`Queue`]. Each queue of the first sense maps to one queue 
/// of the second sense; that is, given the example above, the `JobManager` would have one 
/// [`Queue`] for requested jobs and one for standard site jobs. These [`Queue`] objects
/// help ensure that the number of jobs running in them is correctly capped, per the configuration.
/// 
/// The [`Queue`] objects also have the concept of a "blocking priority", which was put in
/// place to deal with the LUT regeneration requirements. Blocking priority is set up so that
/// queues with different blocking priorities never run at the same time, and jobs waiting in
/// a queue with a higher blocking priority prevent jobs in a lower blocking priority queue 
/// from starting. As an example, consider this sequence of events:
/// 
/// * Four regular `ginput` jobs are running in a queue with a blocking priority of 0.
/// * While they are running, jobs to regenerate the LUTs are added to a queue with a blocking
///   priority of 10. These LUT jobs can't start yet because jobs in a queue with a different
///   blocking priority (the four `ginput` jobs) are still running.
/// * As each `ginput` job finishes, the job manager will replace it with the next job in the
///   database table. However, the new jobs won't start, because the LUT jobs (with their
///   higher blocking priority) are waiting.
/// * Once all four of the initial `ginput` jobs are done, then the LUT jobs can start.
///   As long as they are running, no regular `ginput` jobs will start.
/// * Finally, when the LUT jobs are done, the `ginput` jobs resume.
/// 
/// This approach can be extended to other tasks that are mutually exclusive with each other.
#[derive(Debug)]
pub(crate) struct JobManager<T: Queueable> {
    pub(crate) pool: PoolWrapper,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) job_queues: HashMap<String, Queue<T>>,
    pub(crate) input_file_mover: orm::input_files::InputFileCleanupHandler,
    pub(crate) error_handler: ErrorHandler,
    pub(crate) msg_recv: tokio::sync::mpsc::Receiver<JobMessage>
}

impl<T: Queueable> JobManager<T> {
    /// Create a new instance of a `JobManager`
    /// 
    /// This method creates a database pool just to get onoe connection from it.
    /// If you need other database connections, use [`JobManager::new_from_pool`]
    /// instead and create a pool yourself.
    /// 
    /// # Returns
    /// The `JobManager` instance, only returns an `Err` if connecting to the database
    /// failed.
    #[allow(dead_code)] // used in tests
    pub(crate) async fn new(
        shared_config: Arc<RwLock<Config>>, 
        error_handler: ErrorHandler,
        msg_recv: tokio::sync::mpsc::Receiver<JobMessage>,
    ) -> anyhow::Result<Self> {
        let db_url = orm::get_database_url(None)?;
        let db = orm::get_database_pool(Some(db_url.clone())).await?;
        Self::new_from_pool(db, shared_config, error_handler, msg_recv).await
    }

    /// Create a new instance of `JobManager`, taking a database connection from an existing database pool.
    /// 
    /// See the struct help for example usage. This also schedules a job to regenerate the ginput stratospheric
    /// LUTs to ensure that they are ready for the first jobs to run.
    /// 
    /// # Returns
    /// The `JobManager` instance, only returns an `Err` if getting the database connection failed.
    pub(crate) async fn new_from_pool(
        pool: PoolWrapper, 
        shared_config: Arc<RwLock<Config>>, 
        error_handler: ErrorHandler,
        msg_recv: tokio::sync::mpsc::Receiver<JobMessage>
    ) -> anyhow::Result<Self> {
        let mut me = Self { 
            pool,
            shared_config,
            job_queues: HashMap::new(), 
            input_file_mover: orm::input_files::InputFileCleanupHandler::new(),
            error_handler,
            msg_recv
        };
        me.schedule_lut_regen().await?;
        Ok(me)
    }

    pub(crate) async fn message_loop(&mut self) {
        loop {
            debug!("Job manager waiting for next message");
            let msg = self.msg_recv.recv().await;
            // Must always handle messages, otherwise the shutdown messages aren't processed.
            // Check if this component is disabled in the working functions.
            if let Some(m) = msg {
                debug!("Job manager received message: {m:?}");
                let res = match m {
                    JobMessage::StartJobs => self.start_jobs_entry_point().await.context("Error occurred while starting jobs"),
                    JobMessage::RegenLut => self.schedule_lut_regen().await.context("Error occurred while scheduling LUT regeneration"),
                    JobMessage::CleanUpJobs => self.clean_up_expired_jobs().await.context("Error occurred while cleaning up jobs"),
                    JobMessage::SendStatusReports => self.scan_for_status_requests().await.context("Error occurred while handling status requests"),
                    JobMessage::StopGracefully => {
                        self.msg_recv.close();
                        self.wait_for_jobs_to_finish().await;
                        break;
                    },
                    JobMessage::StopRapidly => {
                        self.msg_recv.close();
                        self.stop_and_reset_jobs().await;
                        break;
                    }
                };

                if let Err(e) = res {
                    self.error_handler.report_error(e.as_ref());
                }

                debug!("Job manager finished handling message: {m:?}");
            } else {
                info!("JobManager: receiver closed, exiting message loop");
                break;
            }
        }
    }

    async fn am_i_disabled(&self) -> bool {
        self.shared_config.read().await.timing.disable_job
    }

    /// The main driver function to be called in a loop or frequently scheduled task.
    /// 
    /// This will scan for new job submission files and start as many jobs as it is
    /// allowed. Complete jobs will be pruned from the internal queues as well.
    /// 
    /// Note that while errors may occur in this function, they are passed to the instance's
    /// error handler to report (usually to a log file and/or email). 
    async fn start_jobs_entry_point(&mut self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Job management disabled in configuration");
            return Ok(());
        }

        self.update_queue_max_jobs().await;

        self.scan_for_job_submissions().await?;

        self.add_pending_jobs_to_queues().await?;

        self.start_queues_with_jobs().await?;

        debug!("Finished in scheduler_entry_point");
        
        Ok(())
    }

    /// Insert special jobs to regenerate ginput's chemical LUTs into the queues
    /// 
    /// This should be called about once per day to ensure that the LUTs are up to date,
    /// as they will periodically need to extrapolate further into the future.
    async fn schedule_lut_regen(&mut self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Job management disabled in configuration");
            return Ok(())
        }

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

        Ok(())
    }

    async fn clean_up_expired_jobs(&self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Job management disabled in configuration");
            return Ok(())
        }

        let mut conn = self.pool.get_connection().await
            .context("Could not get database connection to clean up expired jobs")?;
        Job::clean_up_expired_jobs(&mut conn, false).await
            .context("Error occurred in call to Job::clean_up_expired_jobs")?;
        Ok(())
    }

    pub(crate) async fn reset_running_jobs(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.get_connection().await
            .context("Unable to get connection from DB pool while trying to reset running jobs")?;

        let mut trans = conn.begin().await
            .context("Unable to begin transaction while resetting running jobs")?;

        let running_jobs = Job::get_jobs_in_state(&mut trans, JobState::Running).await
            .context("Unable to get jobs listed in database as 'running'")?;

        let njobs = running_jobs.len();
        if njobs > 0 {
            warn!("{njobs} job(s) detected in running state. Assuming these were orphaned from an incomplete shutdown and resetting them to 'pending'.");
        }
        for mut job in running_jobs {
            job.reset(&mut trans).await
                .with_context(|| format!("Unable to reset running job #{}", job.job_id))?;
        }
        trans.commit().await
            .context("Unable to commit transaction to reset running jobs")?;

        info!("{njobs} running job(s) reset to pending");
        Ok(())
    }

    /// Scan for job request files and add them to the database.
    async fn scan_for_job_submissions(&mut self) -> anyhow::Result<()> {
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
        
        let config = &self.shared_config.read().await;
        orm::input_files::add_jobs_from_input_files(&mut self.pool.get_connection().await?.detach(), &config, &input_files, &save_dir, &mut self.input_file_mover).await?;

        info!("Jobs from input files added to queue");
        Ok(())
    }

    async fn scan_for_status_requests(&mut self) -> anyhow::Result<()> {
        let config = self.shared_config.read().await;
        let input_glob_pattern = &config.execution.status_request_file_pattern;

        let request_files = glob::glob(input_glob_pattern)
            .with_context(|| format!("Globbing for input files in {input_glob_pattern} failed"))?
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("Error collecting input files in {input_glob_pattern}"))?;

        if request_files.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get_connection()
            .await
            .context("Error occurred while trying to get a database connection to send status reports")?;

        if let Err(errors) = orm::input_files::send_status_reports(&mut conn, &config, request_files, &mut self.input_file_mover).await {
            // Because this can return a list of errors and there isn't a clean way to really transform those into a single
            // error, we'll just handle them here.
            for err in errors {
                self.error_handler.report_error(err.as_ref());
            }
        }

        Ok(())
    }

    /// Insert the next highest priority job(s) from the database into the internal queues.
    /// 
    /// This also prunes finished jobs from the internal queues. It does *not* start the jobs,
    /// see `start_queues_with_jobs`.
    async fn add_pending_jobs_to_queues(&mut self) -> anyhow::Result<()> {
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

            let mut conn = self.pool.get_connection().await?;
            {
                let tmp_config = self.shared_config.read().await;
                this_queue.clean_up_finished(&mut conn, &self.error_handler, &tmp_config).await;
            }
            let mut n_to_add = this_queue.num_can_add();
            let n_total = this_queue.max_num_items;
            let n_running = this_queue.num_jobs_running();
            info!("Queue '{name}' has {} of {n_total} allowed jobs allotted, {n_running} are active", n_total - n_to_add);
            while n_to_add > 0 {
                let next_job = Job::claim_next_job_in_queue(&mut conn, &name, &queue_options.fair_share_policy)
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
                    & *self.shared_config.read().await
                );
                
                if !this_queue.add(runner).await {
                    // Really we should not enter this block; the loop over n_to_add should ensure we only
                    // add as many jobs as we are allow to. But just in case, we should ensure that a job not
                    // added to the queue gets reset to 'pending'
                    let mut job = Job::get_job_with_id(&mut conn, job_id)
                        .await
                        .with_context(|| format!("Could not add job ID {job_id} to queue {name} and failed get it from the database to reset its state to 'pending'!"))?;

                    job.set_state(&mut conn, JobState::Pending)
                        .await
                        .with_context(|| format!("Could not add job ID {job_id} to queue {name} and failed to reset its state to 'pending'!"))?;

                    warn!("Tried to add job ID #{job_id} to queue '{name}', but queue refused the job. This should not happen, but the job was successfully reset to 'pending'.");
                    break;
                } else {
                    info!("Put job ID {job_id} into queue {name}");
                    n_to_add -= 1;
                }
            }

        }

        Ok(())
    }

    /// Start jobs waiting in queues, respecting queue blocking priority
    async fn start_queues_with_jobs(&mut self) -> anyhow::Result<()> {
        let pending_queues = self.get_all_queue_names().await?;

        for queue_name in pending_queues.iter() {
            if self.can_queue_start_jobs(queue_name).await {
                info!("Starting jobs in queue '{queue_name}'");
                let config = self.shared_config.read().await;
                if let Some(queue) = self.job_queues.get_mut(queue_name) {
                    queue.start(self.pool.clone(), &config, &self.error_handler).await;
                } else {
                    warn!("Failed to get queue {queue_name} to start jobs, even though it was listed as a pending queue");
                }
            } else {
                info!("Cannot start jobs in queue '{queue_name}' due to another queue blocking it");
            }
        }

        Ok(())
    }

    /// Get the list of all queue names we need to update.
    /// 
    /// This includes all internal queues (whether or not they have jobs) and
    /// any queues listed in the `Jobs` database table. Will return an error
    /// if querying the database for queue names failed.
    async fn get_all_queue_names(&mut self) -> anyhow::Result<Vec<String>> {
        let mut queue_names = Job::get_queues_with_pending_jobs(&mut self.pool.get_connection().await?.detach())
            .await
            .with_context(|| "Error occurred while trying to retrieve the list of queues with pending jobs")?;

        for extant_name in self.job_queues.keys() {
            if !queue_names.contains(extant_name) {
                queue_names.push(extant_name.to_string());
            }
        }

        Ok(queue_names)
    }

    async fn update_queue_max_jobs(&mut self) {
        let config = self.shared_config.read().await;
        
        for (name, queue) in self.job_queues.iter_mut() {
            let max_njobs = if let Some(cfg_queue) = config.get_queue(name) {
                cfg_queue.max_num_procs
            } else if name == LUT_QUEUE_NAME {
                continue;
            } else {
                debug!("Queue '{name}' not found in configuration, assuming only allowed 1 job at a time");
                1
            };

            if queue.max_num_items != max_njobs {
                info!("Updating maximum number of jobs in queue '{name}' from {} to {}", queue.max_num_items, max_njobs);
                queue.max_num_items = max_njobs;
            }
        }
    }

    /// Check if a queue with the given name is allowed to start jobs, based on blocking priority rules.
    async fn can_queue_start_jobs(&self, queue_name: &str) -> bool {
        
        let queue_bp = self.job_queues
            .get(queue_name)
            .map(|q| q.blocking_priority())
            .unwrap_or(0);

        for (other_name, other_queue) in self.job_queues.iter() {
            let other_bp = other_queue.blocking_priority();
            if other_bp < queue_bp {
                if other_queue.has_running_jobs() {
                    info!("Queue '{queue_name}' cannot start jobs because it is waiting for jobs in queue {other_name} to finish");
                    return false
                }

            }

            if other_bp > queue_bp {
                if other_queue.num_jobs_left() > 0 {
                    info!("Queue '{queue_name}' cannot start jobs because a higher blocking priority queue ({other_name}) has jobs pending");
                    return false
                }
            }
        }

        true
    }

    /// Repeatedly check if all jobs currently running are done and return when that is true.
    /// 
    /// This is used for the [`ExitCommand::Graceful`] case.
    async fn wait_for_jobs_to_finish(&mut self) {
        let mut conn = match self.pool.get_connection().await {
            Ok(c) => c,
            Err(e) => {
                self.error_handler.report_error_with_context(
                    &e,
                    "Failed to acquire database connect while trying to cancel jobs"
                );

                return ;
            }
        };
        loop {
            let mut njobs = 0;
            let config = { self.shared_config.read().await.clone() };
            for (name, queue) in self.job_queues.iter_mut() {
                queue.clean_up_finished(&mut conn, &self.error_handler, &config).await;
                njobs += queue.num_jobs_running();
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

    /// Halt any running jobs and reset their state in the database and on disk.
    /// 
    /// This is used for the [`ExitCommand::Rapid`] case.
    async fn stop_and_reset_jobs(&mut self) {
        let mut conn = match self.pool.get_connection().await {
            Ok(c) => c,
            Err(e) => {
                self.error_handler.report_error_with_context(
                    &e,
                    "Failed to acquire database connection while trying to stop and reset jobs"
                );
                return;
            }
        };
        for (name, queue) in self.job_queues.iter_mut() {
            queue.cancel_running_jobs(&mut conn, &self.error_handler).await;
            info!("Stopped and reset all jobs in {name} queue");
        }
    }
}


/// Represents an object that can be used in a [`Queue`]
/// 
/// Note, this mainly exists to allow mocking jobs for testing; in proper execution,
/// all jobs will be instances of [`ServiceJobRunner`]. Jobs to run ginput to generate
/// priors and to update the LUTs are both variants of [`ServiceJobRunner`], not 
/// separate implementors of this trait (though that could change in the future).
#[async_trait]
pub(crate) trait Queueable {
    fn describe(&self) -> String;

    /// Create a new instance of this type from a job in the database.
    fn new_from_job(job: Job, config: &Config) -> Self;

    /// Create a new instance of this type that will regenerate LUTs for the given `ginput` configuration.
    fn new_lut_job(ginput_key: String) -> Self;

    /// Start the job, updating the database if needed.
    async fn start(&mut self, pool: PoolWrapper, config: &Config) -> anyhow::Result<()>;

    /// Return whether this job has started yet. It can be `true` even if the job is complete,
    /// use `is_done` instead to check if a previously started job is complete or errored.
    fn has_started(&self) -> bool;

    /// Mark that this item should be cleaned up the next time the queue is tidied up.
    fn mark_for_cleanup(&mut self);

    /// Return whether a job is waiting to start/actively running (`false`) or completed/errored (`true`).
    /// This should also do any finalization 
    async fn is_done(&mut self, conn: &mut MySqlPC, config: &Config) -> anyhow::Result<bool>;

    /// Stop this job prematurely, do whatever cleanup is required.
    async fn cancel(&mut self, conn: &mut MySqlPC) -> anyhow::Result<bool>;
}

#[derive(Debug)]
pub(crate) struct Queue<T: Queueable> {
    max_num_items: usize,
    items: Vec<T>,
    blocking_priority: i32
} 

impl<T: Queueable> Queue<T> {
    /// Create a new `Queue` that allows at most `max_num_items` to be in it at once.
    /// The queue will have a blocking priority of 0 (see the [`JobManager`] help for
    /// details on blocking priority).
    pub fn new(max_num_items: usize) -> Self {
        Self { max_num_items, items: Vec::new(), blocking_priority: 0 }
    }

    /// Create a new queue with a given blocking priority, see [`JobManager`] for details
    /// on that.
    pub fn new_blocking(max_num_items: usize, blocking_priority: i32) -> Self {
        Self { max_num_items, items: Vec::new(), blocking_priority }
    }

    /// Return the queue's blocking priority.
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

    /// Start running any items in the queue not already in progrees.
    pub async fn start(&mut self, pool: PoolWrapper, config: &Config, error_handler: &ErrorHandler) {
        for item in self.items.iter_mut() {
            if !item.has_started() {
                debug!("Starting queued item: {}", item.describe());
                item.start(pool.clone(), config).await
                .unwrap_or_else(|e| error_handler.report_error(e.as_ref()));
                debug!("Queued item {} started successfully", item.describe());
            } else {
                debug!("Queued item {} already started", item.describe());
            }
        }
    }

    /// Cleans up items in the queue that have completed.
    /// 
    /// Typically, you would call this method before `add`
    /// or `num_can_add` to remove any completed jobs to
    /// make room for new ones.
    pub async fn clean_up_finished(&mut self, conn: &mut MySqlPC, error_handler: &ErrorHandler, config: &Config) {
        let old_items = std::mem::take(&mut self.items);
        for mut item in old_items {
            let still_running = match item.is_done(conn, config).await {
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

    /// How many jobs are currently in this queue.
    /// Note that jobs waiting to start and completed/errored count; this does
    /// *not* run `clean_up_finished` before counting jobs.
    pub fn num_jobs_left(&self) -> usize {
        self.items.len()
    }

    /// Number of jobs that have been started. Jobs that have finished but not
    /// been pruned yet still count.
    pub fn num_jobs_running(&self) -> usize {
        self.items.iter()
            .fold(0, |acc, el| {
                if el.has_started() {
                    acc + 1
                }else{
                    acc
                }
            })
    }

    /// Return `true` if there are any actively running jobs in this queue.
    pub fn has_running_jobs(&self) -> bool {
        self.items.iter()
            .any(|i| i.has_started())
    }

    /// Stop and clean up any jobs running in this queue.
    pub async fn cancel_running_jobs(&mut self, conn: &mut MySqlPC, error_handler: &ErrorHandler) {
        for item in self.items.iter_mut() {
            item.cancel(conn).await
                .map(|_| ()) // need this to avoid a type error on the unwrap; don't care if there was a task to cancel
                .unwrap_or_else(|e| error_handler.report_error(e.as_ref()));
        }
    }
}


/// The object that runs a job
/// 
/// There are three levels of indirection:
/// 1. This enum handles whether we are generating priors (`GinputJob`) or updating the look up tables
///    (`LutRegenJob`) as well as iterating over dates in a given job. The latter is necessary in case
///    different ginput configurations or meteorology paths are needed for different days of the same job.
/// 2. Internally, it has a [`GinputRunner`], which is an enum over possible ways of calling `ginput`,
///    e.g. via the shell or directly through a Rust/Python interface. 
/// 3. [`GinputRunner`] holds internally a separate struct which actually implements the given way of 
///    calling `ginput`. Right now, there is only a [`ShellGinputRunner`].
#[derive(Debug)]
pub enum ServiceJobRunner {
    GinputJob{job: Job, join_handle: Option<GinputHandle>, force_cleanup: bool},
    LutRegenJob{ginput_key: String, join_handle: Option<GinputHandle>, force_cleanup: bool}
}

impl ServiceJobRunner {

    async fn is_ginput_job_done(job: &mut Job, join_handle: &mut Option<GinputHandle>, conn: &mut MySqlPC, config: &Config) -> anyhow::Result<bool> {
        let task = if let Some(runner) = join_handle {
            runner
        } else {
            return Ok(false)
        };

        if !task.is_finished() {
            return Ok(false);
        }

        let inner_res = match task.await {
            Ok(r) => r,
            Err(e) => anyhow::bail!("Panic occurred in job #{}: {e:?}", job.job_id)
        };

        match inner_res {
            Ok(_) => {
                Ok(true)
            },
            Err(e) => {
                let email_config = &config.email;
                job.set_errored(conn, &e, Some(email_config)).await
                    .unwrap_or_else(|e| error!("Failed to set state for job {} to 'errored' because: {e}", job.job_id));
                anyhow::bail!("Error occurred in job #{}: {e:?}", job.job_id)
            }
        }
    }

    async fn is_lut_job_done(ginput_key: &str, join_handle: &mut Option<GinputHandle>) -> anyhow::Result<bool> {
        let task = if let Some(runner) = join_handle {
            runner
        } else {
            return Ok(false)
        };

        if !task.is_finished() {
            return Ok(false)
        }

        let inner_res = match task.await {
            Ok(r) => r,
            Err(e) => anyhow::bail!("Panic occurred regenerating LUTs for ginput '{ginput_key}': {e:?}")
        };

        match inner_res {
            Ok(_) => Ok(true),
            Err(e) => {anyhow::bail!("Error occurred regenerating LUTs for ginput '{ginput_key}': {e:?}")}
        }
    }

    async fn start_ginput_job(pool: PoolWrapper, job: Job, config: Config, join_handle: &mut Option<GinputHandle>) -> anyhow::Result<()> {
        *join_handle = Some(start_priors_gen_job(pool, job, config));
        debug!("Ginput job started, join handle = {join_handle:?}");
        Ok(())
    }

    async fn start_lut_job(ginput_key: String, join_handle: &mut Option<GinputHandle>, config: Config) -> anyhow::Result<()> {
        *join_handle = Some(start_lut_regen_job(ginput_key, config));
        debug!("LUT job started, join handle = {join_handle:?}");
        Ok(())
    }

    async fn cancel_ginput_job(job: &mut Job, join_handle: &mut Option<GinputHandle>, conn: &mut MySqlPC) -> anyhow::Result<bool> {
        if let Some(task) = join_handle {
            task.abort();
            match task.await {
                Ok(_) => info!("Job {} was already complete when tried to cancel", job.job_id),
                Err(e) if e.is_cancelled() => (),
                Err(e) => {
                    anyhow::bail!("Job #{} had encountered an error before being cancelled: {e:?}", job.job_id);
                }
            }
            orm::jobs::cleanup_cancelled_ginput_job(&mut *conn, job).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn cancel_lut_job(ginput_key: &str, join_handle: &mut Option<GinputHandle>) -> anyhow::Result<bool> {
        if let Some(task) = join_handle {
            task.abort();
            match task.await {
                Ok(_) => (),
                Err(e) if e.is_cancelled() => (),
                Err(e) => {
                    anyhow::bail!("Regenerating LUTs for ginput '{ginput_key}' encountered an error before being cancelled: {e:?}");
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl Queueable for ServiceJobRunner {
    fn describe(&self) -> String {
        match self {
            Self::GinputJob{job, join_handle: _, force_cleanup: _} => format!("ServiceJobRunner(job = {})", job.job_id),
            Self::LutRegenJob{ginput_key, join_handle: _, force_cleanup: _} => format!("ServiceJobRunner(lut ginput = {ginput_key})")
        }
    }

    fn new_from_job(job: Job, _config: &Config) -> Self {
        Self::GinputJob { job, join_handle: None, force_cleanup: false }
    }

    fn new_lut_job(ginput_key: String) -> Self {
        Self::LutRegenJob { ginput_key, join_handle: None, force_cleanup: false }
    }

    fn has_started(&self) -> bool {
        // I don't think there can be a race condition between this and start - since start
        // takes a mutable ref, it can't be called at the same time as this function,
        // so we should never have a case where start is in progress when this is called
        match self {
            Self::GinputJob{job: _, join_handle, force_cleanup: _} => join_handle.is_some(),
            Self::LutRegenJob{ginput_key: _, join_handle, force_cleanup: _} => join_handle.is_some()
        }
    }

    fn mark_for_cleanup(&mut self) {
        match self {
            Self::GinputJob{job: _, join_handle: _, force_cleanup} => *force_cleanup = true,
            Self::LutRegenJob{ginput_key: _, join_handle: _, force_cleanup} => *force_cleanup = true,
        }
    }

    async fn is_done(&mut self, conn: &mut MySqlPC, config: &Config) -> anyhow::Result<bool> {
        match self {
            Self::GinputJob{job, join_handle, force_cleanup} => {
                if *force_cleanup {
                    Ok(true)
                } else {
                    Self::is_ginput_job_done(job, join_handle, conn, config).await
                }
            },
            Self::LutRegenJob{ginput_key, join_handle, force_cleanup} => {
                if *force_cleanup {
                    Ok(true)
                } else {
                    Self::is_lut_job_done(&ginput_key, join_handle).await
                }
            }
        }
    }

    async fn start(&mut self, pool: PoolWrapper, config: &Config) -> anyhow::Result<()> {
        match self {
            Self::GinputJob{job, join_handle, force_cleanup: _} => Self::start_ginput_job(pool, job.clone(), config.clone(), join_handle).await,
            Self::LutRegenJob{ginput_key, join_handle, force_cleanup: _} => Self::start_lut_job(ginput_key.clone(), join_handle, config.clone()).await,
        }
    }

    async fn cancel(&mut self, conn: &mut MySqlPC) -> anyhow::Result<bool> {
        match self {
            Self::GinputJob{job, join_handle, force_cleanup} => {
                *force_cleanup = true;
                Self::cancel_ginput_job(job, join_handle, conn).await
            },
            Self::LutRegenJob{ginput_key, join_handle, force_cleanup} => {
                *force_cleanup = true;
                Self::cancel_lut_job(ginput_key, join_handle).await
            },
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
        start_time: Option<Instant>,
        force_cleanup: bool,
    }

    impl DummyJobRunner {
        fn new_from_seconds(seconds: u64) -> Self {
            let delay = Duration::from_secs(seconds);
            Self { delay, start_time: None, force_cleanup: false }
        }
    }

    #[async_trait]
    impl Queueable for DummyJobRunner {
        fn describe(&self) -> String {
            format!("DummyJobRunner(delay = {:?})", self.delay)
        }

        fn new_from_job(_job: Job, _config: &Config) -> Self {
            Self::new_from_seconds(5)
        }

        fn new_lut_job(_ginput_key: String) -> Self {
            Self::new_from_seconds(5)
        }

        async fn start(&mut self, mut _conn: PoolWrapper, _config: &Config) -> anyhow::Result<()> {
            self.start_time = Some(Instant::now());
            Ok(())
        }

        fn has_started(&self) -> bool {
            self.start_time.is_some()
        }

        fn mark_for_cleanup(&mut self) {
            self.force_cleanup = true;
        }

        async fn is_done(&mut self, _conn: &mut MySqlPC, _config: &Config) -> anyhow::Result<bool> {
            if self.force_cleanup {
                return Ok(true)
            }

            let start = if let Some(t) = self.start_time {
                t
            } else {
                return Ok(false)
            };

            Ok(Instant::now().duration_since(start) > self.delay)
        }

        async fn cancel(&mut self, _conn: &mut MySqlPC) -> anyhow::Result<bool> {
            Ok(true)
        }
    }

    async fn make_dummy_job_manager() -> (JobManager<DummyJobRunner>, orm::test_utils::TestDb) {
        let mut config = orm::config::Config::default();
        config.execution.queues.insert(
            TEST_QUEUE_NAME.to_string(), 
            orm::config::JobQueueOptions{ max_num_procs: TEST_QUEUE_MAX_NUM_ITEMS, ..Default::default() }
        );

        let (_, rx) = tokio::sync::mpsc::channel(256);
        let (pool, test_db) = orm::test_utils::open_test_database(true)
            .await
            .expect("Should be able to create a connection to the test database");

        let jm = JobManager::new_from_pool(
            pool,
            Arc::new(RwLock::new(config)), 
            ErrorHandler::Logging(LoggingErrorHandler{}),
            rx
        ).await.expect("Could not make dummy JobManager");

        (jm, test_db)
    }

    // Test that:
    // 1) [x] Already running standard jobs prevent the LUT jobs from starting
    // 2) [x] The presence of LUT jobs in the queue prevent new standard jobs from starting
    // 3) [x] Once LUT jobs finish, standard jobs are allowed to start
    #[tokio::test]
    async fn test_lut_with_running_jobs() {
        let (mut manager, _test_db) = make_dummy_job_manager().await;
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
        let (mut manager, _test_db) = make_dummy_job_manager().await;
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
        let (mut manager, _test_db) = make_dummy_job_manager().await;
        let test_job = DummyJobRunner::new_from_seconds(5);
        let mut std_queue = Queue::new(TEST_QUEUE_MAX_NUM_ITEMS);
        std_queue.add(test_job.clone()).await;
        manager.job_queues.insert(TEST_QUEUE_NAME.to_string(), std_queue);

        let lut_job = DummyJobRunner::new_lut_job("bob".to_string());
        let mut lut_queue = Queue::new_blocking(usize::MAX, LUT_REGEN_BLOCKING_PRIORITY);
        lut_queue.add(lut_job).await;

        manager.start_jobs_entry_point().await.unwrap();
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
        manager.start_jobs_entry_point().await.unwrap();

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
        
        manager.start_jobs_entry_point().await.unwrap();
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

        manager.start_jobs_entry_point().await.unwrap();

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