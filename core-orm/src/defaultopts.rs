use std::{fmt::Display, error::Error};

use chrono::NaiveDate;
use itertools::Itertools;
use sqlx;

use crate::MySqlConn;

fn match_info_to_string(id: i32, date1: Option<&NaiveDate>, date2: Option<&NaiveDate>) -> String {
    let s1 = date1.map(|d| d.to_string()).unwrap_or_else(|| "None".to_owned());
    let s2 = date2.map(|d| d.to_string()).unwrap_or_else(|| "None".to_owned());
    format!("(id = {id}: {s1}, {s2})")
}


#[derive(Debug)]
pub enum DefaultOptsQueryError {
    NoMatches(NaiveDate),
    MultipleMatches{date: NaiveDate, matches: Vec<(i32, Option<NaiveDate>, Option<NaiveDate>)>},
    MatchesOverlap((i32, Option<NaiveDate>, Option<NaiveDate>), (i32, Option<NaiveDate>, Option<NaiveDate>)),
    Sqlx(sqlx::Error)
}



impl Display for DefaultOptsQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DefaultOptsQueryError::NoMatches(date) => write!(f, "No default option set defined for date {date}"),
            DefaultOptsQueryError::MultipleMatches { date, matches } => {
                let mstr = matches.iter()
                    .map(|(i, s, e)| match_info_to_string(*i, s.as_ref(), e.as_ref())).join(", ");
                let n = matches.len();
                write!(f, "{n} default option sets matched date {date}: {mstr}")
            },
            DefaultOptsQueryError::MatchesOverlap((i, a1, a2), (j, b1, b2)) => {
                let astr = match_info_to_string(*i, a1.as_ref(), a2.as_ref());
                let bstr = match_info_to_string(*j, b1.as_ref(), b2.as_ref());
                write!(f, "Two default option sets overlap in time: {astr} and {bstr}")
            },
            DefaultOptsQueryError::Sqlx(e) => write!(f, "SQL error during query: {e}"),
        }
    }
}

impl Error for DefaultOptsQueryError {}

impl From<sqlx::Error> for DefaultOptsQueryError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}


pub struct DefaultOptions {
    pub id: i32,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub ginput: String,
    pub met: String,
}


impl DefaultOptions {
    fn id_tuple(&self) -> (i32, Option<NaiveDate>, Option<NaiveDate>) {
        (self.id, self.start_date, self.end_date)
    }

    fn overlaps(&self, other: &Self) -> bool {
        match (self.start_date, self.end_date, other.start_date, other.end_date) {
            (None, None, _, _) => true,
            (_, _, None, None) => true,
            (None, Some(_), None, Some(_)) => true,
            (None, Some(a2), Some(b1), None) => a2 > b1,
            (None, Some(a2), Some(b1), Some(_)) => a2 > b1,
            (Some(a1), None, None, Some(b2)) => a1 < b2,
            (Some(_), None, Some(_), None) => true,
            (Some(a1), None, Some(_), Some(b2)) => a1 < b2,
            (Some(a1), Some(_), None, Some(b2)) => a1 > b2,
            (Some(_), Some(a2), Some(b1), None) => a2 < b1,
            (Some(a1), Some(a2), Some(b1), Some(b2)) => {
                if a2 < b1 || b2 < a1 { false } else { true }
            },
        }
    }

    pub async fn get_all_defaults(conn: &mut MySqlConn) -> Result<Vec<Self>, DefaultOptsQueryError> {
        let mut all_options = sqlx::query_as!(
            DefaultOptions,
            "SELECT * FROM StdOptionsByDate"
        ).fetch_all(conn)
        .await?;

        // Order by start date, treating None as the earliest possible 
        all_options.sort_by(|a, b| {
            match (a.start_date, b.start_date) {
                (None, None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(d1), Some(d2)) => d1.cmp(&d2),
            }
        });

        Ok(all_options)
    }

    pub async fn get_all_defaults_check_overlap(conn: &mut MySqlConn) -> Result<Vec<Self>, DefaultOptsQueryError> {
        let all_options = Self::get_all_defaults(conn).await?;
        for pair in all_options.iter().combinations(2) {
            if pair[0].overlaps(pair[1]) {
                return Err(DefaultOptsQueryError::MatchesOverlap(pair[0].id_tuple(), pair[1].id_tuple()))
            }
        }
        Ok(all_options)
    }


    pub async fn get_defaults_for_date(conn: &mut MySqlConn, date: NaiveDate) -> Result<Self, DefaultOptsQueryError> {
        let all_options = sqlx::query_as!(
            DefaultOptions,
            "SELECT * FROM StdOptionsByDate"
        ).fetch_all(conn)
        .await?;

        // Filter down to the rows which apply to this date. If >1 or 0, that is an error.
        let mut all_options: Vec<DefaultOptions> = all_options.into_iter()
            .filter(|o| {
                match (o.start_date, o.end_date) {
                    (None, None) => true,
                    (None, Some(end)) => date < end,
                    (Some(start), None) => date >= start,
                    (Some(start), Some(end)) => start <= date && date < end,
                }
            }).collect();

        if all_options.len() == 1 {
            Ok(all_options.pop().unwrap())
        } else if all_options.is_empty() {
            Err(DefaultOptsQueryError::NoMatches(date))
        } else {
            let matches = all_options.iter()
                .map(|o| (o.id, o.start_date, o.end_date))
                .collect_vec();
            Err(DefaultOptsQueryError::MultipleMatches { date, matches })
        }
    }

}