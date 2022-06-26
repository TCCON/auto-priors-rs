use anyhow;
use chrono::NaiveDate;
use sqlx::{self, FromRow, Type};

type MySqlPC = sqlx::pool::PoolConnection<sqlx::MySql>;

#[derive(Debug, Type)]
pub enum SiteType {
    Unknown = 0,
    TCCON = 1,
    EM27 = 2
}

impl From<String> for SiteType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "TCCON" => Self::TCCON,
            "EM27" => Self::EM27,
            _ => Self::Unknown
        }
    }
}


#[derive(Debug)]
pub struct StdSite {
    pub id: i32,
    pub site_id: String,
    pub site_type: SiteType
}

impl From<QStdSite> for StdSite {
    fn from(obj: QStdSite) -> Self {
        StdSite { id: obj.id, site_id: obj.site_id, site_type: SiteType::from(obj.site_type) }
    }
}

#[derive(Debug, FromRow)]
struct QStdSite {
    id: i32,
    site_id: String,
    site_type: String
}

#[derive(Debug, FromRow)]
pub struct SiteInfo {
    pub id: i32,
    pub site_id: Option<String>,
    site: i32,
    pub name: String,
    pub location: String,
    pub latitude: f32,
    pub longitude: f32,
    pub start_date: NaiveDate,
    pub end_date: Option<NaiveDate>,
    pub comment: String
}

impl SiteInfo {
    pub async fn get_std_site(&self, pool: &mut MySqlPC) -> anyhow::Result<StdSite> {
        let result = sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList WHERE id = ?",
                self.site
            ).fetch_one(pool)
            .await?;

        Ok(StdSite::from(result))
    }
}

pub async fn get_most_recent_site_location(pool: &mut MySqlPC, site_id: &str) -> anyhow::Result<SiteInfo> {
    let result = sqlx::query_as!(
            SiteInfo, 
            "SELECT * FROM v_StdSiteInfo WHERE site_id = ? ORDER BY start_date DESC LIMIT 1",
            site_id
        ).fetch_one(pool)
        .await?;

    Ok(result)
}