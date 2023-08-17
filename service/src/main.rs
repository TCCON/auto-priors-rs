use std::{sync::{Arc, atomic::AtomicBool}, time::Duration};

use clokwerk::{TimeUnits, Job};
use error::LoggingErrorHandler;
use jobs::JobMessage;
use log::{info, warn, error, debug};
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{RwLock, mpsc::{self, error::TrySendError, error::TryRecvError, Sender}};

use crate::error::ErrorHandler;

mod error;
mod jobs;
mod met;
mod stdsitejobs;

const MSG_BUFFER_SIZE: usize = 256;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Uncomment this, and restore the console-subscriber dependency in the service Cargo.toml
    // and the tokio tracing feature in the workspace Cargo.toml to use the tokio-console app
    // to measure tokio behavior. See https://github.com/tokio-rs/console for RUSTFLAGS needed
    // as well
    // console_subscriber::init();

    env_logger::Builder::from_default_env()
        .filter_module("sqlx", log::LevelFilter::Warn)
        .init();

    println!("Service starting");
    info!("Starting tccon-priors-service");
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file)?;
    let timing_config = (&config.timing).clone();
    let config = Arc::new(RwLock::new(config));

    let err_handler = error::LoggingErrorHandler{};

    let mut sync_scheduler = clokwerk::Scheduler::new();
    let signals = setup_signals().expect("Could not set up signal handling");

    // JOB MANAGER SETUP //

    let (tx_jobs, rx_jobs) = mpsc::channel::<jobs::JobMessage>(MSG_BUFFER_SIZE);        
    let job_man_handle = {
        let db = db.clone();
        let shared_config = Arc::clone(&config);
        let err_handler = err_handler.clone();
        tokio::spawn(async move {
            let mut job_manager: jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler> = jobs::JobManager::new_from_pool(
                db, 
                shared_config, 
                err_handler.clone(), 
                rx_jobs
            ).await.expect("Failed to initialize job manager");

            if let Err(e) = job_manager.reset_running_jobs().await {
                err_handler.report_error_with_context(
                    e.as_ref(), 
                    "Error occurred during job manager start up while trying to reset jobs left as 'running' in the database. These jobs will likely still be stuck as 'running'."
                );
            }
            job_manager.message_loop().await;
        })  
    };

    info!("Setting up job parsing and execution to run");
    let tx_run_jobs = tx_jobs.clone();
    sync_scheduler
        .every(timing_config.job_start_seconds.seconds())
        .run(move || {
            debug!("Scheduler: sending StartJobs message");
            match tx_run_jobs.try_send(jobs::JobMessage::StartJobs) {
                Ok(_) => debug!("Scheduler: StartJobs message sent"),
                Err(TrySendError::Closed(_)) => warn!("Could not send StartJobs message, channel closed"),
                Err(TrySendError::Full(_)) => warn!("Could not send StartJobs message, channel full"),
            }
        });

    
    info!("Setting up strat LUT regen to run");
    let tx_lut_regen = tx_jobs.clone();
    let lut_job = sync_scheduler
        .every(timing_config.lut_regen_days.days())
        .run(move || {
            debug!("Scheduler: sending RegenLut message");
            match tx_lut_regen.try_send(jobs::JobMessage::RegenLut) {
                Ok(_) => (),
                Err(TrySendError::Closed(_)) => warn!("Could not send RegenLut message, channel closed"),
                Err(TrySendError::Full(_)) => warn!("Could not send RegenLut message, channel full")
            }
        });
    if let Some(at) = timing_config.lut_regen_at {
        lut_job.at(&at);
    }
    
    // END JOB MANAGER SETUP //

    // MET MANAGER SETUP //

    let (tx_met, rx_met) = mpsc::channel::<met::MetMessage>(MSG_BUFFER_SIZE);
    let met_manager_handle = {
        let db = db.clone();
        let shared_config = Arc::clone(&config);
        tokio::spawn(async move {
            let mut met_manager = met::MetManager::new_with_pool(
                db.clone(), 
                shared_config, 
                LoggingErrorHandler {  },
                rx_met
            ).await;

            met_manager.message_loop().await;
        })
    };
    

    {
        let tx_met_dl = tx_met.clone();
        sync_scheduler
            .every(timing_config.met_download_hours.hours())
            .run(move || {
                debug!("Scheduler: sending DownloadMet message");
                match tx_met_dl.try_send(met::MetMessage::DownloadMet) {
                    Ok(_) => (),
                    Err(TrySendError::Closed(_)) => warn!("Could not send DownloadMet message, channel closed"),
                    Err(TrySendError::Full(_)) => warn!("Could not send DownloadMet message, channel full")
                }
            });
    }

    // END MET MANAGER SETUP //

    // STD SITE MANAGER SETUP //

    let (tx_std_sites, rx_std_sites) = mpsc::channel::<stdsitejobs::StdSiteMessage>(MSG_BUFFER_SIZE);  
    let std_site_manager_handler = {
        let db = db.clone();
        let shared_config = Arc::clone(&config);
        let err_handler = err_handler.clone();
        tokio::spawn(async move {
            let mut std_site_manager = stdsitejobs::StdSiteManager::new_with_pool(
                db,
                shared_config,
                err_handler,
                rx_std_sites
            ).await;

            std_site_manager.message_loop().await;
        })
    };

    info!("Setting up job parsing and execution to run");
    let tx_std_site_submit = tx_std_sites.clone();
    let std_site_add_job = sync_scheduler
        .every(timing_config.std_site_gen_hours.hours())
        .run(move || {
            debug!("Scheduler: sending AddJobs message to StdSiteManager");
            match tx_std_site_submit.try_send(stdsitejobs::StdSiteMessage::AddJobs) {
                Ok(_) => debug!("Scheduler: AddJobs message sent"),
                Err(TrySendError::Closed(_)) => warn!("Could not send AddJobs message, channel closed"),
                Err(TrySendError::Full(_)) => warn!("Could not send AddJobs message, channel full"),
            }
        });
    if let Some(at) = timing_config.std_site_gen_offset_minutes {
        std_site_add_job.plus(at.minutes());
    }

    let tx_std_site_tar = tx_std_sites.clone();
    sync_scheduler
        .every(timing_config.std_site_tar_minutes.minutes())
        .run(move || {
            debug!("Scheduler: sending MakeTarballs message to StdSiteManager");
            match tx_std_site_tar.try_send(stdsitejobs::StdSiteMessage::MakeTarballs) {
                Ok(_) => debug!("Scheduler MakeTarballs message sent"),
                Err(TrySendError::Closed(_)) => warn!("Could not send MakeTarballs message, channel closed"),
                Err(TrySendError::Full(_)) => warn!("Could not send MakeTarballs message, channel full"),
            }
        });
    // END STD SITE MANAGER SETUP //

    // Start scheduler

    let (tx_scheduler, mut rx_scheduler) = mpsc::channel::<bool>(4);
    let schedule_handle = tokio::spawn(async move {
        loop {
            match rx_scheduler.try_recv() {
                Ok(true) => { 
                    info!("Stopping scheduler loop");
                    break;
                },
                Ok(false) => {
                    debug!("Heartbeat scheduler message received");
                },
                Err(TryRecvError::Disconnected) => {
                    warn!("Scheduler receiver disconnected, aborting loop");
                    break;
                },
                Err(TryRecvError::Empty) => ()
            }
            debug!("Running pending jobs in scheduler");
            sync_scheduler.run_pending();
            debug!("Finished running pending jobs in scheduler");
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    // Start signal processing
    let signal_handle = {
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            process_signals(signals, config, tx_scheduler, tx_met, tx_jobs, tx_std_sites).await
                .unwrap_or_else(|e| error!("Error occurred while processing signals: {e}"));
        })
    };

    tokio::try_join!(
        met_manager_handle,
        job_man_handle,
        std_site_manager_handler,
        schedule_handle,
        signal_handle,
    ).map(|_| ())
    .unwrap_or_else(|e| {
        err_handler.report_error_with_context(&e, "Error occurred in join on all top level threads");
    });

    

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ExitCommand {
    Graceful,
    Rapid
}

fn setup_signals() -> std::io::Result<Signals> {
    // Copied from https://docs.rs/signal-hook/latest/signal_hook/index.html
    // This should make it so that two Ctrl+C signals will immediately exit
    let term_now = Arc::new(AtomicBool::new(true));
    for sig in signal_hook::consts::TERM_SIGNALS {
        signal_hook::flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        signal_hook::flag::register(*sig, Arc::clone(&term_now))?;
    }

    let mut sigs = vec![
        signal::SIGHUP, // we'll use this to reload the config, signal_hook docs imply that is common for daemons
    ];
    // this should include SIGTERM, SIGQUIT, and SIGINT. INT will be our graceful shutdown, the other two our rapid
    // shutdown.
    sigs.extend(signal_hook::consts::TERM_SIGNALS);

    Signals::new(sigs)
}

async fn process_signals(
    mut signals: Signals, 
    config: Arc<RwLock<orm::config::Config>>, 
    tx_scheduler: Sender<bool>,
    tx_met: Sender<met::MetMessage>,
    tx_jobs: Sender<jobs::JobMessage>,
    tx_std_sites: Sender<stdsitejobs::StdSiteMessage>,
    ) -> anyhow::Result<()> {
    // If I understand the signal_hook docs correctly, this should be an infinite loop.
    for sig in &mut signals {
        match sig {
            signal::SIGHUP => {
                let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
                info!("Reloading configuration");
                let new_config = orm::config::load_config_file_or_default(config_file)?;
                let mut global_config = config.write().await;
                *global_config = new_config;
                
            }, // reload config
            signal::SIGINT => {
                info!("Beginning graceful shutdown");
                shutdown_components(ExitCommand::Graceful, tx_scheduler, tx_met, tx_jobs, tx_std_sites).await;
                info!("Graceful shutdown complete");
                break;
            },
            signal::SIGTERM | signal::SIGQUIT => {
                info!("Beginning rapid shutdown");
                shutdown_components(ExitCommand::Rapid, tx_scheduler, tx_met, tx_jobs, tx_std_sites).await;
                info!("Rapid shutdown complete");
                break;
            },
            _ => {
                info!("Received signal {sig}, taking no action");
            }
        }
    }

    Ok(())
}

async fn shutdown_components(
    exit_cmd: ExitCommand, 
    tx_scheduler: Sender<bool>,
    tx_met: Sender<met::MetMessage>,
    tx_jobs: Sender<jobs::JobMessage>,
    tx_std_sites: Sender<stdsitejobs::StdSiteMessage>
) {
    tx_scheduler.send(true).await
        .unwrap_or_else(|e| error!("Could not send shutdown message to scheduler: {e}"));

    match exit_cmd {
        ExitCommand::Graceful => {
            tx_met.send(met::MetMessage::StopGracefully).await
                .unwrap_or_else(|e| error!("Could not send graceful shutdown message to met manager: {e}"));
            tx_jobs.send(jobs::JobMessage::StopGracefully).await
                .unwrap_or_else(|e| error!("Could not send graceful shutdown message to jobs manager: {e}"));
            tx_std_sites.send(stdsitejobs::StdSiteMessage::StopGracefully).await
                .unwrap_or_else(|e| error!("Could not send graceful shutdown message to std. sites manager: {e}"));
        },
        ExitCommand::Rapid => {
            tx_met.send(met::MetMessage::StopRapidly).await
                .unwrap_or_else(|e| error!("Could not send rapid shutdown message to met manager: {e}"));
            tx_jobs.send(JobMessage::StopRapidly).await
                .unwrap_or_else(|e| error!("Could not send rapid shutdown message to jobs manager: {e}"));
            tx_std_sites.send(stdsitejobs::StdSiteMessage::StopRapidly).await
                .unwrap_or_else(|e| error!("Could not send rapid shutdown message to std. sites manager: {e}"));
        },
    }
}