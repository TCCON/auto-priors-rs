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