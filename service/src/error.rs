use log::error;

pub(crate) trait ErrorHandler: Clone + Sync + Send {
    fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static));
    fn report_error_with_context<S: AsRef<str>>(&self, err: &(dyn std::error::Error + Send + Sync + 'static), context: S);
}

#[derive(Debug, Clone)]
pub(crate) struct LoggingErrorHandler {}

impl ErrorHandler for LoggingErrorHandler {
    fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) {
        error!("{err:?}")
    }

    fn report_error_with_context<S: AsRef<str>>(&self, err: &(dyn std::error::Error + Send + Sync + 'static), context: S) {
        error!("{err:?}");
        error!("{}", context.as_ref())
    }
}