use std::fmt::Display;

use chrono::naive::NaiveDate;
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::sqlx::{self, Row, FromRow};

#[derive(Database)]
#[database("tccon_priors")]
pub struct PriorsDb(sqlx::MySqlPool);

// FromRow: https://docs.rs/sqlx/latest/sqlx/trait.FromRow.html
// SQLX type mapping: https://docs.rs/sqlx/latest/sqlx/mysql/types/index.html
#[derive(Debug, FromRow)]
pub struct StdSiteJob {
    id: i64,
    site_id: String,
    #[sqlx(rename = "site")]
    site_fk: i64,
    date: NaiveDate,
    state: i8,
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
            return None
        };
    }
}