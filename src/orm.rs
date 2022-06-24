use std::fmt::Display;

use chrono::naive::NaiveDate;
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::sqlx::{self, FromRow};

#[derive(Database)]
#[database("tccon_priors")]
pub struct PriorsDb(sqlx::MySqlPool);

// Deriving the type with each variant having an assigned value and
// giving it a repr that matches the SQL type seems to be enough to
// decode this without needing to implement sqlx::Decode
#[derive(Debug, sqlx::Type)]
#[repr(i8)]
enum SiteState {
    Pending = 0,
    Complete = 1,
    Nonop = -1
}

impl Display for SiteState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Nonop => "nonop"
        };
        write!(f, "{}", state)
    }
}

#[derive(Debug, sqlx::Type)]
#[repr(i8)]
enum JobState {
    Pending = 0,
    Running = 1,
    Complete = 2,
    Errored = 3,
    Cleaned = 4
}

impl Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Errored => "errored",
            Self::Cleaned => "cleaned"
        };
        write!(f, "{}", state)
    }
}

#[derive(Debug, sqlx::Type)]
#[repr(i8)]
enum TarOption {
    No = 0,
    Yes = 1,
    Egi = 2
}

impl Display for TarOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::No => "no",
            Self::Yes => "yes",
            Self::Egi => "EGI"
        };
        write!(f, "{}", state)
    }
}

// FromRow: https://docs.rs/sqlx/latest/sqlx/trait.FromRow.html
// SQLX type mapping: https://docs.rs/sqlx/latest/sqlx/mysql/types/index.html
#[derive(Debug, FromRow)]
pub struct StdSiteJob {
    id: i64,
    site_id: String,
    #[sqlx(rename = "site")]
    site_fk: i64,
    date: NaiveDate,
    state: SiteState,
    #[sqlx(rename = "job")]
    job_fk: Option<i64>,
}

impl Display for StdSiteJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(j) = self.job_fk {
            write!(f, "StdSiteJob({}, {}, state = {}, job = {})", self.site_id, self.date, self.state, j)
        }else{
            write!(f, "StdSiteJob({}, {}, state = {})", self.site_id, self.date, self.state)
        }
    }
}

impl StdSiteJob {
    pub async fn get_by_id(mut db: Connection<PriorsDb>, id: i64) -> Option<Self> {
        let result = sqlx::query_as("SELECT * FROM v_StdSiteJobs WHERE id = ?")
            .bind(id)
            .fetch_one(&mut *db).await;

        result.ok()
    }

    pub async fn get_by_date_range(mut db: Connection<PriorsDb>, start_date: NaiveDate, end_date: NaiveDate) -> Option<Vec<Self>> {
        let result = sqlx::query_as("SELECT * FROM v_StdSiteJobs WHERE date >= ? AND date <= ?")
            .bind(start_date)
            .bind(end_date)
            .fetch_all(&mut *db)
            .await;

        return if let Ok(r) = result {
            Some(r)
        }else{
            eprint!("Oops: {result:?}");
            return None
        };
    }
}