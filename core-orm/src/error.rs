use std::{fmt::Display, error::Error};

use chrono::NaiveDate;


#[derive(Debug)]
pub enum DefaultOptsQueryError {
    NoMatches(NaiveDate),
    MultipleMatches{date: NaiveDate, matches: Vec<String>},
    MatchesOverlap(String, String),
    Sqlx(sqlx::Error)
}



impl Display for DefaultOptsQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DefaultOptsQueryError::NoMatches(date) => write!(f, "No default option set defined for date {date}"),
            DefaultOptsQueryError::MultipleMatches { date, matches } => {
                let mstr = matches.join(", ");
                let n = matches.len();
                write!(f, "{n} default option sets matched date {date}: {mstr}")
            },
            DefaultOptsQueryError::MatchesOverlap(m1, m2) => {
                write!(f, "Two default option sets overlap in time: {m1} and {m2}")
            },
            DefaultOptsQueryError::Sqlx(e) => write!(f, "SQL error during query: {e}"),
        }
    }
}

impl Error for DefaultOptsQueryError {}

pub type JobResult<T> = Result<T, JobError>;

#[derive(Debug)]
pub enum JobError {
    QueryError(sqlx::Error),
    DeadlockError(sqlx::Error),
    InvalidState(i8),
    InvalidTar(i8),
    InvalidModFmt(String),
    InvalidVmrFmt(String),
    InvalidMapFmt(String),
    InvalidJson(serde_json::Error),
    ConfigurationError(anyhow::Error),
    InvalidSiteLocation(anyhow::Error),
    RunDirectoryError(std::io::Error),
    CancellationError(String),
    WasCancelled,
    GinputFailureError(i32),
    Other(String)
}

impl Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::QueryError(e) => write!(f, "SQL Job Error: {e}"),
            JobError::DeadlockError(e) => write!(f, "SQL transaction deadlock: {e}"),
            JobError::InvalidState(state) => write!(f, "Unknown state integer: {state}"),
            JobError::InvalidTar(choice) => write!(f, "Unknown Tar choice integer: {choice}"),
            JobError::InvalidModFmt(fmt) => write!(f, "Unknown ModFmt integer: {fmt}"),
            JobError::InvalidVmrFmt(fmt) => write!(f, "Unknown VmrFmt integer: {fmt}"),
            JobError::InvalidMapFmt(fmt) => write!(f, "Unknown MapFmt integer: {fmt}"),
            JobError::InvalidJson(e) => write!(f, "Invalid JSON found in job information: {e}"),
            JobError::ConfigurationError(e) => write!(f, "Invalid configuration: {e}"),
            JobError::InvalidSiteLocation(e) => write!(f, "Invalid site location: {e}"),
            JobError::RunDirectoryError(e) => write!(f, "There was a problem with the run directory: {e}"),
            JobError::CancellationError(msg) => write!(f, "There was a problem cancelling a job: {msg}"),
            JobError::WasCancelled => write!(f, "Job was cancelled"),
            JobError::GinputFailureError(code) => write!(f, "ginput exited with error code {code}"),
            JobError::Other(msg) => write!(f, "Other Job Error: {msg}"),
        }
    }
}

impl Error for JobError {}

impl From<sqlx::Error> for JobError {
    fn from(value: sqlx::Error) -> Self {
        if let sqlx::Error::Database(e) = &value {
            // NB: This code is the code for a deadlock in MySql version 8.0.33 on a Mac.
            // If this changes on other versions, systems, the logic here may need updated.
            // The test_next_job_with_transaction integration test should catch if that happens.
            if e.code() == Some("40001".into()) {
                return Self::DeadlockError(value)
            }
        }

        return Self::QueryError(value)
    }
}

impl From<serde_json::Error> for JobError {
    fn from(value: serde_json::Error) -> Self {
        Self::InvalidJson(value)
    }
}

#[derive(Debug)]
pub enum JobPriorityError {
    StateNotPending,
    Other(anyhow::Error),
}

impl Display for JobPriorityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobPriorityError::StateNotPending => write!(f, "Job is not pending, changing priority will have no effect."),
            JobPriorityError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<anyhow::Error> for JobPriorityError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value)
    }
}

impl Error for JobPriorityError {}


#[derive(Debug)]
pub enum EmailError {
    UnparsableEmail(String),
    UnencodableBody(String),
    SendFailure(String)
}

impl Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmailError::UnparsableEmail(email) => write!(f, "Email '{email}' is not a valid email"),
            EmailError::UnencodableBody(reason) => write!(f, "Could not encode body: {reason}"),
            EmailError::SendFailure(reason) => write!(f, "Could not send email: {reason}")
        }
    }
}

impl Error for EmailError {}