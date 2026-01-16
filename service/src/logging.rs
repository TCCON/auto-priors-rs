use clap::Args;
use log::LevelFilter;
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        rolling_file::{
            policy::compound::{
                roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
            },
            RollingFileAppender,
        },
    },
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

/// The TCCON automatic priors service that handles all automation
#[derive(Debug, Args)]
#[clap(setting = clap::AppSettings::DeriveDisplayOrder, version)]
pub struct ServiceLoggingCli {
    /// Disable logging to stderr
    #[clap(long)]
    no_log_stderr: bool,

    /// The lowest severity message to print to stderr, options are
    /// OFF, ERROR, WARN, INFO, DEBUG, or TRACE. Default is INFO.
    #[clap(long, default_value_t = LevelFilter::Info)]
    stderr_level: LevelFilter,

    /// If given, a file which messages from the service will be written to.
    #[clap(long)]
    pub log_file: Option<String>,

    /// The lowest severity message to write to the file. Same options as --stderr-level
    #[clap(long, default_value_t = LevelFilter::Info)]
    file_level: LevelFilter,

    /// The size in MB that triggers a log file to roll over to the next index
    #[clap(long, default_value_t = 10)]
    log_file_max_size: u64,

    /// The number of backup log files to keep. With the default of 10, this would keep
    /// extra files *.0 through *.9, in addition to the current log file, but would
    /// delete the *.9 file the next time a new file is started.
    #[clap(long, default_value_t = 10)]
    log_file_num_rolls: u32,
}

impl ServiceLoggingCli {
    pub fn configure_logging(args: ServiceLoggingCli) {
        setup_logging(
            !args.no_log_stderr,
            args.stderr_level,
            args.log_file.as_deref(),
            args.file_level,
            args.log_file_max_size,
            args.log_file_num_rolls,
        );
    }
}

pub fn setup_logging(
    log_to_console: bool,
    console_log_level: LevelFilter,
    log_file: Option<&str>,
    file_log_level: LevelFilter,
    log_file_size_mb: u64,
    num_log_file_rolls: u32,
) {
    let config = log4rs::Config::builder();
    let root = Root::builder();
    let mod_filter = ModuleFilter::new(vec!["tccon"]);

    let (config, root) = if log_to_console {
        let stderr_appender = ConsoleAppender::builder()
            .encoder(Box::new(PatternEncoder::new(
                "{h({d(%Y-%m-%d %H:%M:%S)} [{l}] from line {L} in {M})} - {m}{n}",
            )))
            .target(Target::Stderr)
            .build();
        let config = config.appender(
            Appender::builder()
                .filter(Box::new(mod_filter.clone()))
                .filter(Box::new(ThresholdFilter::new(console_log_level)))
                .build("stderr", Box::new(stderr_appender)),
        );
        let root = root.appender("stderr");
        (config, root)
    } else {
        (config, root)
    };

    let (config, root) = if let Some(path) = log_file {
        let trigger = SizeTrigger::new(log_file_size_mb * 1_000_000); // ~10 MB
        let roller = FixedWindowRoller::builder()
            .build(&format!("{path}.{{}}"), num_log_file_rolls)
            .expect("Could not set up the rolling log file");

        let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

        let logfile_appender = RollingFileAppender::builder()
            // Use almost the same pattern as the stderr, but without emphasis colors (those often muck up files in my experience)
            .encoder(Box::new(PatternEncoder::new(
                "{d(%Y-%m-%d %H:%M:%S)} [{l}] from line {L} in {M} - {m}{n}",
            )))
            .append(true)
            .build(path, Box::new(policy))
            .expect("Could not write to service log file");

        let config = config.appender(
            Appender::builder()
                .filter(Box::new(mod_filter.clone()))
                .filter(Box::new(ThresholdFilter::new(file_log_level)))
                .build("logfile", Box::new(logfile_appender)),
        );

        let root = root.appender("logfile");

        (config, root)
    } else {
        (config, root)
    };

    // The level here seems to be the upper limit for allowed log messages,
    // so just set it as high as we can
    let config = config
        .build(root.build(LevelFilter::Trace))
        .expect("Could not configure logging");

    log4rs::init_config(config).expect("Could not initialize logging");
}

#[derive(Debug, Clone)]
struct ModuleFilter {
    prefixes: Vec<&'static str>,
}

impl ModuleFilter {
    fn new(prefixes: Vec<&'static str>) -> Self {
        Self { prefixes }
    }
}

impl log4rs::filter::Filter for ModuleFilter {
    fn filter(&self, record: &log::Record) -> log4rs::filter::Response {
        if let Some(module) = record.module_path() {
            if self.prefixes.iter().any(|&pre| module.starts_with(pre)) {
                return log4rs::filter::Response::Neutral;
            }
        }

        log4rs::filter::Response::Reject
    }
}
