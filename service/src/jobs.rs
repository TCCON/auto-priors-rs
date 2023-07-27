use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use anyhow::Context;
use log::{warn, info, debug};
use orm::{jobs::{Job, JobState, JobRunner}, MySqlConn, config::Config, MySqlPC};
use tokio::sync::{RwLock, watch::Receiver};

use crate::{ExitCommand, error::ErrorHandler};

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

                self.start_pending_jobs()
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

    pub(crate) async fn start_pending_jobs(&mut self) -> anyhow::Result<()> {
        let queue_names = Job::get_queues_with_pending_jobs(&mut self.db_conn)
            .await
            .with_context(|| "Error occurred while trying to retrieve the list of queues with pending jobs")?;

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
                
                let config = self.shared_config.read().await;
                if !this_queue.add(runner, &mut self.db_conn, &config, &self.error_handler).await {
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
    async fn start(&mut self, conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()>;
    async fn is_done(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool>;
    async fn cancel(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool>;
}

pub(crate) struct Queue<T: Queueable> {
    max_num_items: usize,
    items: Vec<T>
} 

impl<T: Queueable> Queue<T> {
    pub fn new(max_num_items: usize) -> Self {
        Self { max_num_items, items: Vec::new() }
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
    pub async fn add(&mut self, mut item: T, conn: &mut MySqlConn, config: &Config, error_handler: &dyn ErrorHandler) -> bool {
        if !self.can_add() {
            false
        } else {
            item.start(conn, config).await
                .unwrap_or_else(|e| error_handler.report_error(e.as_ref()));
            self.items.push(item);
            true
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

    pub async fn cancel_running_jobs(&mut self, conn: &mut MySqlConn, error_handler: &dyn ErrorHandler) {
        for item in self.items.iter_mut() {
            item.cancel(conn).await
                .map(|_| ()) // need this to avoid a type error on the unwrap; don't care if there was a task to cancel
                .unwrap_or_else(|e| error_handler.report_error(e.as_ref()));
        }
    }
}

pub struct ServiceJobRunner {
    job: Job,
    inner_runner: Option<JobRunner>,
    cancel_requested: bool,
    cancel_completed: bool
}

#[async_trait]
impl Queueable for ServiceJobRunner {

    fn new_from_job(job: Job, _config: &Config) -> Self {
        Self { job, inner_runner: None, cancel_requested: false, cancel_completed: false }
    }

    async fn is_done(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        let task = if let Some(runner) = &mut self.inner_runner {
            runner
        } else {
            return Ok(false)
        };

        let res = task.is_done()
            .with_context(|| format!(
                "Error checking if job #{} is done", self.job.job_id
            ))?;
        match res {
            orm::jobs::RunState::InProgress => Ok(false),
            orm::jobs::RunState::Complete => {
                // todo: handle setting output path to the tar file when appropriate
                let output_path = self.job.save_dir.clone();
                self.job.set_completed(conn, &output_path, None)
                    .await?;
                return Ok(true);
            },
            orm::jobs::RunState::Errored => {
                self.job.set_state(conn, JobState::Errored).await?;
                anyhow::bail!("ginput error occurred in job #{}", self.job.job_id);
            },
        }
    }

    async fn start(&mut self, conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()> {
        // The job state is set by the manager when it claims it
        
        // Alright, this is going to be complicated, because it needs to look at the dates for the job
        // and check that they do not cross a boundary for which met/ginput version to use. That also should
        // be checked on submission, but it's also possible that the config changed since the job was submitted,
        // so we verify here too. (That check should go in the core-orm since it'll be used in a couple places.)
        // As long as the job falls entirely within one of the configured time periods, we can set up the ginput
        // run. Otherwise, we either error or break this into multiple calls.
        let date_iter = orm::utils::DateIterator::new(
            vec![(self.job.start_date, self.job.end_date)]
        );
        
        for date in date_iter {
            let ginput_key = if let Some(key) = &self.job.ginput_key {
                key
            } else {
                let defaults = config.get_defaults_for_date(date)
                    .with_context(|| format!("Could not get defaults for date {date}; occurred while trying to start ginput for job {}", self.job.job_id))?;
                &defaults.ginput
            };

            let ginput = config.execution.ginput.get(ginput_key)
                .ok_or_else(|| anyhow::anyhow!("Ginput key '{ginput_key}', required by job #{}, is not defined in the configuration", self.job.job_id))?;
            
            let task_res = ginput.start_job_for_date(conn, date, &self.job, config)
                .await;

            let task = match task_res {
                Ok(c) => c,
                Err(e) => {
                    // Could not start the job for whatever reason; set the job state to "errored" and exit
                    self.job.set_state(conn, JobState::Errored).await?;
                    return Err(e).with_context(|| format!(
                        "Was not able to start job #{} for date {date}", self.job.job_id
                    ));
                }
            };
            self.inner_runner = Some(task);

            // There is a possibility of a race condition that the task may be cancelled while the startup process
            // is running. If we reach this point, the any future cancels will work properly because the task has
            // been stored. 
            if self.cancel_requested && !self.cancel_completed {
                self.cancel(conn).await?;
            }
        }

        Ok(())
    }

    async fn cancel(&mut self, conn: &mut MySqlConn) -> anyhow::Result<bool> {
        self.cancel_requested = true;

        if let Some(task) = &mut self.inner_runner {
            task.cancel(conn, &mut self.job)
                .await
                .with_context(|| format!("Error occurred while trying to stop job #{}", self.job.job_id))
                .map(|_| {
                    self.cancel_completed = true;
                    true
                })
        } else {
            Ok(false)
        }
    }

    
}