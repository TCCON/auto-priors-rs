use log::error;

pub(crate) trait ErrorHandler: Sync {
    fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static));
}

#[derive(Debug)]
pub(crate) struct LoggingErrorHandler {}

impl ErrorHandler for LoggingErrorHandler {
    fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) {
        error!("{err}")
    }
}