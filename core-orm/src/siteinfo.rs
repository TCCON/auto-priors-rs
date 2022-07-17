//! Interface to the standard site information tables
use std::{collections::HashMap, str::FromStr};

use anyhow;
use chrono::{NaiveDate, Duration};
use serde::Serialize;
use sqlx::{self, FromRow, Type};

use crate::utils;

use super::MySqlConn;

/// An enum describing the type of site
#[derive(Debug, Type)]
#[repr(i8)]
pub enum SiteType {
    /// This site is neither TCCON nor an EM27. (`i8` value = `0`.)
    Unknown = 0,
    /// This is a TCCON site. (`i8` value = `1`.)
    TCCON = 1,
    /// This is a permanent EM27 site. (`i8` value = `2`.)
    EM27 = 2
}

impl From<String> for SiteType {
    fn from(s: String) -> Self {
        return Self::from_str(&s).unwrap_or(Self::Unknown)
    }
}

impl FromStr for SiteType {
    type Err = anyhow::Error;
    /// Convert a string to [`SiteType`]
    /// 
    /// Matches the strings "tccon" or "em27" (case insensitive).
    /// Anything else returns `SiteType::Unknown`.
    /// 
    /// # Notes
    /// This will never return an `Err`, so can be safely unwrapped.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tccon" => Ok(Self::TCCON),
            "em27" => Ok(Self::EM27),
            _ => Ok(Self::Unknown)
        }
    }
}


/// A struct representing a standard (permanent) TCCON, EM27, or other site.
#[derive(Debug)]
pub struct StdSite {
    /// **\[primary key\]** the primary key in the SQL StdSiteList table.
    pub id: i32,
    /// The (generally) two-character site ID for this site
    pub site_id: String,
    /// Whether this is a TCCON, EM27, or other site type.
    pub site_type: SiteType
}

// impl<'r> FromRow<'r, sqlx::mysql::MySqlRow> for StdSite {
//     // Implementing for a concrete row type was recommended by https://stackoverflow.com/a/66713961
//     // because trying to implement for any generic row is much more work.
//     fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
//         let id = row.try_get("id")?;
//         let site_id = row.try_get("site_id")?;
//         let site_type: String = row.try_get("site_type")?;
//         // let site_type = SiteType::from(site_type_str);

//         Ok(StdSite{id, site_id, site_type})
//     }
// }

impl From<QStdSite> for StdSite {
    fn from(obj: QStdSite) -> Self {
        StdSite { id: obj.id, site_id: obj.site_id, site_type: SiteType::from(obj.site_type) }
    }
}

impl StdSite {
    pub async fn primary_key_to_site_id(conn: &mut MySqlConn, site_prim_key: i32) -> anyhow::Result<String> {
        let site = sqlx::query_as!(
            QStdSite,
            "SELECT * FROM StdSiteList WHERE id = ?",
            site_prim_key
        ).fetch_one(conn)
        .await?;

        return Ok(site.site_id)
    }

    pub async fn site_id_to_primary_key(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<i32> {
        let site = sqlx::query_as!(
            QStdSite,
            "SELECT * FROM StdSiteList WHERE site_id = ?",
            site_id
        ).fetch_one(conn)
        .await?;

        return Ok(site.id)
    }

    /// Returns a list of currently defined site IDs in alphabetical order
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `site_type` - optionally, which site type to return. If `None`, all sites are returned regardless of type.
    pub async fn get_site_ids(conn: &mut MySqlConn, site_type: Option<SiteType>) -> anyhow::Result<Vec<String>> {
        let sites = if let Some(stype) = site_type {
            sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList WHERE site_type = ? ORDER BY site_id",
                stype
            ).fetch_all(conn)
            .await?
        }else{
            sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList ORDER BY site_id"
            ).fetch_all(conn)
            .await?
        };
        

        let site_ids = sites.into_iter().map(|s| s.site_id).collect();
        return Ok(site_ids)
    }
}


/// An internal query struct that represents the result of a SQL query on the StdSiteList table.
/// 
/// This should be converted to a [`StdSite`] instance for any public-facing functions.
#[derive(Debug, FromRow)]
struct QStdSite {
    id: i32,
    site_id: String,
    site_type: String
}

/// A structure representing a single information row for a standard site
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct SiteInfo {
    #[serde(skip)]
    pub id: i32,
    /// The two-character site ID. May be `None` if a row's site foreign key failed 
    /// to match a site in the StdSiteList table.
    pub site_id: Option<String>,
    #[serde(skip)]
    site: i32,
    /// The long name of this site.
    pub name: String,
    /// The human-readable location of this site (e.g. "Park Falls, WI, USA")
    pub location: String,
    /// The latitude of this site, south is negative.
    pub latitude: f32,
    /// The longitude of this site, west is negative.
    pub longitude: f32,
    /// The first date when this site was operational at this location.
    pub start_date: NaiveDate,
    /// The last date (exclusive) when this site was operational at this location.
    /// `None` indicates that the site remains operational here.
    pub end_date: Option<NaiveDate>,
    /// A comment to describe any special considerations with this site.
    pub comment: String
}

impl SiteInfo {
    /// Return the standard site table entry associated with this site information.
    /// 
    /// If a standard site cannot be found, the returned result will be and `Err`.
    pub async fn get_std_site(&self, pool: &mut MySqlConn) -> anyhow::Result<StdSite> {
        let result = sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList WHERE id = ?",
                self.site
            ).fetch_one(pool)
            .await?;

        Ok(StdSite::from(result))
    }

    /// Create a JSON string representing a list of SiteInfo instances
    /// 
    /// # Parameters
    /// 
    /// * `infos` - a slice of `SiteInfo` instances (e.g. returned by [`SiteInfo::get_all_site_info`])
    /// * `pretty` - whether to format the JSON in pretty style or not
    /// 
    /// # Returns
    /// 
    /// A JSON string representing a sequence of `SiteInfo` instances, with each one serialized directly
    /// to JSON.
    /// 
    /// # See also
    /// 
    /// * [`SiteInfo::to_grouped_json`]
    /// 
    /// # Errors
    /// 
    /// Returns an `Err` if the serialization failed for any reason.
    pub fn to_flat_json(infos: &[SiteInfo], pretty: bool) -> anyhow::Result<String> {
        if pretty {
            Ok(serde_json::to_string_pretty(infos)?)
        }else{
            Ok(serde_json::to_string(infos)?)
        }
    }

    /// Create a JSON string representing a list of SiteInfo instances
    /// 
    /// Unlike [`SiteInfo::to_flat_json`], the JSON returned here will be a map with one
    /// entry per site ID. Each time period listed for that site will be given in a list of
    /// sub-maps. If the site's name or location changes in different time periods, then only
    /// the values in the first time period are retained.
    /// 
    /// # Parameters
    /// 
    /// * `infos` - a slice of `SiteInfo` instances (e.g. returned by [`SiteInfo::get_all_site_info`])
    /// * `pretty` - whether to format the JSON in pretty style or not
    /// 
    /// # See also
    /// 
    /// * [`SiteInfo::to_flat_json`]
    /// 
    /// # Errors
    /// 
    /// Returns an `Err` if the serialization failed for any reason.
    pub fn to_grouped_json(infos: &[SiteInfo], pretty: bool) -> anyhow::Result<String> {
        #[derive(Debug, Serialize)]
        struct SiteTimePeriod {
            latitude: f32,
            longitude: f32,
            start_date: NaiveDate,
            end_date: Option<NaiveDate>,
            comment: String
        }

        #[derive(Debug, Serialize)]
        struct Site {
            name: String,
            location: String,
            time_periods: Vec<SiteTimePeriod>
        }

        fn info_to_tp(s: &SiteInfo) -> SiteTimePeriod {
            SiteTimePeriod { latitude: s.latitude, longitude: s.longitude, start_date: s.start_date, end_date: s.end_date, comment: s.comment.clone() }
        }

        let mut json_map = HashMap::new();
        let mut i_no_id = 0;
        for this_info in infos {
            let sid = if let Some(x) = &this_info.site_id {
                x.clone()
            }else{
                let x = format!("no_id_{i_no_id}");
                i_no_id += 1;
                x
            };


            if !json_map.contains_key(&sid) {
                json_map.insert(
                    sid,
                    Site {
                        name: this_info.name.clone(),
                        location: this_info.location.clone(),
                        time_periods: vec![info_to_tp(this_info)]
                    }
                );
            }else{
                // TODO: warn if the name & location do not match
                let new_tp = info_to_tp(this_info);
                json_map.get_mut(&sid).unwrap().time_periods.push(new_tp);
            }
        }
        
        if pretty {
            return Ok(serde_json::to_string_pretty(&json_map)?)
        }else{
            return Ok(serde_json::to_string(&json_map)?);
        }
    }

    /// Get the current site information for a given site, i.e. the information with the most recent start date
    /// 
    /// # Parameters
    /// 
    /// * `pool` - the MySQL pool or connection object to perform the query with
    /// * `site_id` - the two letter site ID of the site to query, e.g. "pa"
    pub async fn get_most_recent_site_location(pool: &mut MySqlConn, site_id: &str) -> anyhow::Result<SiteInfo> {
        let result = sqlx::query_as!(
                SiteInfo, 
                "SELECT * FROM v_StdSiteInfo WHERE site_id = ? ORDER BY start_date DESC LIMIT 1",
                site_id
            ).fetch_one(pool)
            .await?;
    
        Ok(result)
    }

    pub async fn get_all_site_info(pool: &mut MySqlConn) -> anyhow::Result<Vec<SiteInfo>> {
        let result = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo"
        ).fetch_all(pool)
        .await?;

        return Ok(result)
    }

    /// Get the information about standard sites for a given date.
    /// 
    /// # Parameters
    /// 
    /// * `pool` - the MySQL pool or connection object to perform the query with
    /// * `date` - the date to get site information for.
    /// * `active` - determines whether to include sites that are not active on the requested date.
    ///   If this is `true`, then only sites whose start and end dates bracket `date` are included.
    ///   If this is `false`, then all sites are included once, and sites whose start and end dates
    ///   do not bracket `date` will use the information closest in time (forward or back).
    /// 
    /// # Notes
    /// 
    /// Whether `active` is `true` or `false`, if a site has more than 1 instance of information that
    /// brackets the given `date`, the one with the most recent start date will be returned.
    pub async fn get_site_info_for_date(conn: &mut MySqlConn, date: NaiveDate, active: bool) -> anyhow::Result<Vec<SiteInfo>> {
        if active {
            let result = sqlx::query_as!(
                SiteInfo,
                "SELECT * FROM v_StdSiteInfo WHERE start_date <= ? AND (end_date IS NULL OR end_date > ?)",
                date,
                date
            ).fetch_all(conn)
            .await?;

            let mut sites = HashMap::new();
            let mut infos = vec![];
            for this_info in result.iter() {
                if !sites.contains_key(&this_info.site) {
                    sites.insert(this_info.site, infos.len());
                    infos.push(this_info.clone())
                }else{
                    let oidx = *sites.get(&this_info.site).unwrap();
                    if this_info.start_date > infos[oidx].start_date {
                        infos[oidx] = this_info.clone();
                    }
                }
            }

            return Ok(infos)
        }else{
            let result = sqlx::query_as!(
                SiteInfo,
                "SELECT * FROM v_StdSiteInfo",
            ).fetch_all(conn)
            .await?;

            let mut final_site = HashMap::new();
            let mut site_order = vec![];
            for (idx, info) in result.iter().enumerate() {
                let sid = info.site;
                if !final_site.contains_key(&sid) {
                    final_site.insert(sid, idx);
                    site_order.push(sid); // each time we add a new site, record the order so that the return is deterministic
                }else{
                    // Choose between two site infos to decide which one gives the best match for this site. 
                    // If one's start/end dates bracket the requested date and the other doesn't use the one
                    // that brackets it. If both bracket the date, then select the one with the later start date.
                    // Otherwise, choose the one closer in time to the date.
                    let cidx = *final_site.get(&sid).unwrap();
                    let curr = &result[cidx];
                    let curr_brackets = info_brackets_date(date, curr.start_date, curr.end_date);
                    let new_brackets = info_brackets_date(date, info.start_date, info.end_date);

                    if curr_brackets && !new_brackets {
                        // do nothing, keep current info
                    }else if new_brackets && !curr_brackets {
                        // new info brackets the date, use it instead
                        final_site.insert(sid, idx);
                    }else if curr_brackets && new_brackets && curr.start_date >= info.start_date {
                        // do nothing, keep current info
                    }else if curr_brackets && new_brackets {
                        // the new info has a later start time, use it instead
                        final_site.insert(sid, idx);
                    }else if new_info_closer_in_time(date, curr, info){
                        // use whichever one is closer in time
                        final_site.insert(sid, idx);
                    }
                }
            }

            let mut infos = vec![];
            for idx in site_order {
                let ridx = *final_site.get(&idx).unwrap();
                infos.push(result[ridx].clone());
            }
            return Ok(infos)
        }
    }

    pub async fn get_one_site_location_for_date_range(conn: &mut MySqlConn, site_id: &str, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<SiteInfo> {
        let site_infos = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo WHERE site_id = ?",
            site_id
        ).fetch_all(conn)
        .await?;

        let mut matching_info = None;
        for info in site_infos.into_iter() {
            if utils::date_ranges_overlap(start_date, end_date, info.start_date, info.end_date) {
                if matching_info.is_none() { 
                    matching_info = Some(info); 
                }
                else {
                    anyhow::bail!("Multiple locations defined for site {site_id} between {start_date} and {end_date:?}");
                }
            }
        }

        if matching_info.is_none() {
            anyhow::bail!("No location defined for site {site_id} between {start_date} and {end_date:?}");
        }else{
            return Ok(matching_info.unwrap())
        }
    }

    pub async fn verify_info_available_for_site(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<bool> {
        let n_match = sqlx::query!("SELECT COUNT(*) as count FROM v_StdSiteInfo WHERE site_id = ?", site_id)
            .fetch_one(conn)
            .await?
            .count;

        return Ok(n_match > 0);
    }

    pub async fn fill_null_latlons<T: AsRef<str>>(conn: &mut MySqlConn, site_ids: &[T], lats: &[Option<f32>], lons: &[Option<f32>], start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<(Vec<f32>, Vec<f32>)> {
        if lats.len() != site_ids.len() || lons.len() != site_ids.len() {
            anyhow::bail!("site_ids, lats, and lons must be the same length; got {}, {}, and {}, respectively",
                          site_ids.len(), lats.len(), lons.len());
        }

        let mut out_lats = vec![];
        let mut out_lons = vec![];

        for (idx, sid) in site_ids.iter().enumerate() {
            let y = lats[idx];
            let x = lons[idx];
            if x.is_some() != y.is_some() {
                anyhow::bail!("The lat and lon at index {idx} are not both `None` or both `Some`");
            }

            if x.is_none() {
                let info = Self::get_one_site_location_for_date_range(&mut *conn, sid.as_ref(), start_date, end_date).await?;
                out_lats.push(info.latitude);
                out_lons.push(info.longitude);
            }else{
                out_lats.push(y.unwrap());
                out_lons.push(x.unwrap());
            }
        }

        return Ok((out_lats, out_lons))
    }

    /// Return how many locations are defined for a given site ID in a date range.
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `site_id` - the two-letter site ID for which we are checking locations
    /// * `start_date` - the beginning of the date range to check.
    /// * `end_date` - the end of the date range to check. This can be `None` to indicate
    ///   an open-ended date range. If this is `Some(date)`, then it is exclusive - that is,
    ///   if a site location's start date is the same as this end date, that is *not* considered
    ///   an overlap.
    /// 
    /// # Returns
    /// The number of locations defined for the site with `site_id` in the given date range. If
    /// `site_id` is not a known site ID, the result will be 0.
    /// 
    /// # Errors
    /// Returns an `Err` if the database query fails. 
    pub async fn check_number_locations_in_date_range(conn: &mut MySqlConn, site_id: &str, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<u32> {
        let infos = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo WHERE site_id = ?",
            site_id
        ).fetch_all(conn)
        .await?;

        let mut n = 0;
        for info in infos {
            if utils::date_ranges_overlap(start_date, end_date, info.start_date, info.end_date) {
                n += 1;
            }
        }
        
        return Ok(n)
    }
}


fn info_brackets_date(date: NaiveDate, start_date: NaiveDate, end_date: Option<NaiveDate>) -> bool {
    if let Some(end) = end_date {
        return (date >= start_date) && (date < end)
    }else{
        return date >= start_date
    }
}

fn new_info_closer_in_time(date: NaiveDate, curr: &SiteInfo, new: &SiteInfo) -> bool {
    let curr_delta = if date < curr.start_date {
        curr.start_date - date
    }else{
        if let Some(end) = curr.end_date { date - end } else { Duration::days(0) }
    };

    let new_delta = if date < new.start_date {
        new.start_date - date
    }else{
        if let Some(end) = new.end_date { date - end } else { Duration::days(0) }
    };

    return new_delta < curr_delta
}