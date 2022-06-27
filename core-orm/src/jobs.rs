use std::{path::PathBuf, str::FromStr, fmt::Display};

use anyhow;
use chrono::{NaiveDate, NaiveDateTime};
use serde::Deserialize;
use serde_json;
use sqlx::{self, FromRow, Type};

use crate::{MySqlPC, MySqlPool};

// TODO: change times from Naive to Local (needs changing SQL to timestamp?)

fn str_to_json_arr<'a, T: Deserialize<'a>> (s: &'a str) -> anyhow::Result<Vec<T>> {
    Ok(serde_json::from_str(s)?)
}

#[derive(Debug, Type, Clone, Copy)]
pub enum JobState {
    Pending = 0,
    Running = 1,
    Complete = 2,
    Errored = 3,
    Cleaned = 4,
}

impl Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Errored => "errored",
            Self::Cleaned => "cleaned"
        };

        write!(f, "{s}")
    }
}

impl Default for JobState {
    fn default() -> Self {
        JobState::Pending
    }
}

impl From<JobState> for i8 {
    fn from(v: JobState) -> Self {
        match v {
            JobState::Pending => 0,
            JobState::Running => 1,
            JobState::Complete => 2,
            JobState::Errored => 3,
            JobState::Cleaned => 4
        }
    }
}

impl TryFrom<i8> for JobState {
    type Error = anyhow::Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Complete),
            3 => Ok(Self::Errored),
            4 => Ok(Self::Cleaned),
            _ => Err(anyhow::anyhow!("Unknown value for JobState: {value}"))
        }
    }
}

#[derive(Debug, Type, Clone, Copy)]
pub enum TarChoice {
    No = 0,
    Yes = 1,
    Egi = 2
}

impl Display for TarChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::No => "no",
            Self::Yes => "yes",
            Self::Egi => "EGI"
        };

        write!(f, "{s}")
    }
}

impl Default for TarChoice {
    fn default() -> Self {
        Self::Yes
    }
}

impl From<TarChoice> for i8 {
    fn from(v: TarChoice) -> Self {
        match v {
            TarChoice::No => 0,
            TarChoice::Yes => 1,
            TarChoice::Egi => 2
        }
    }
}

impl TryFrom<i8> for TarChoice {
    type Error = anyhow::Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::No),
            1 => Ok(Self::Yes),
            2 => Ok(Self::Egi),
            _ => Err(anyhow::anyhow!("Unknown value for TarChoice: {value}"))
        }
    }
}

#[derive(Debug, Type)]
pub enum ModFmt {
    None,
    Text
}

impl Default for ModFmt {
    fn default() -> Self {
        Self::Text
    }
}

impl Display for ModFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s =match self {
            ModFmt::None => "None".to_owned(),
            ModFmt::Text => "Text".to_owned(),
        };
        write!(f, "{s}")
    }
}

impl FromStr for ModFmt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            _ => Err(anyhow::anyhow!("Unknown value for ModFmt: {s}"))
        }
    }
}

#[derive(Debug, Type)]
pub enum VmrFmt {
    None,
    Text
}

impl Default for VmrFmt {
    fn default() -> Self {
        Self::Text
    }
}

impl Display for VmrFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            VmrFmt::None => "None".to_owned(),
            VmrFmt::Text => "Text".to_owned(),
        };
        
        write!(f, "{s}")
    }
}

impl FromStr for VmrFmt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            _ => Err(anyhow::anyhow!("Unknown value for VmrFmt: {s}"))
        }
    }
}

#[derive(Debug, Type)]
pub enum MapFmt {
    None,
    Text,
    NetCDF
}

impl Default for MapFmt {
    fn default() -> Self {
        Self::Text
    }
}

impl Display for MapFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MapFmt::None => "None".to_owned(),
            MapFmt::Text => "Text".to_owned(),
            MapFmt::NetCDF => "NetCDF".to_owned()
        };

        write!(f, "{s}")
    }
}

impl FromStr for MapFmt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            "netcdf" => Ok(Self::NetCDF),
            _ => Err(anyhow::anyhow!("Unknown value for MapFmt: {s}"))
        }
    }
}

#[derive(Debug, FromRow)]
struct QJob { 
    job_id: i32,
    state: i8,
    site_id: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lat: String,
    lon: String,
    email: Option<String>,
    delete_time: Option<NaiveDateTime>,
    priority: i32,
    save_dir: String,
    save_tarball: i8,
    mod_fmt: String,
    vmr_fmt: String,
    map_fmt: String,
    submit_time: NaiveDateTime,
    complete_time: Option<NaiveDateTime>,
    output_file: Option<String>
}

impl TryFrom<Job> for QJob {
    type Error = anyhow::Error;

    fn try_from(j: Job) -> Result<Self, Self::Error> {
        let save_dir = j.save_dir
            .to_str()
            .ok_or(anyhow::anyhow!("Failed to convert save_dir to a UTF string"))?
            .to_owned();
        
        let output_file = if let Some(o) = j.output_file {
            Some(o.to_str()
                  .ok_or(anyhow::anyhow!("Failed to convert output_file to UTF string"))?
                  .to_owned())
        }else{
            None
        };

        Ok(QJob { 
            job_id: j.job_id,
            state: j.state as i8,
            site_id: serde_json::to_string(&j.site_id)?,
            start_date: j.start_date,
            end_date: j.end_date,
            lat: serde_json::to_string(&j.lat)?,
            lon: serde_json::to_string(&j.lon)?,
            email: j.email,
            delete_time: j.delete_time,
            priority: j.priority,
            save_dir: save_dir,
            save_tarball: j.save_tarball as i8,
            mod_fmt: j.mod_fmt.to_string(),
            vmr_fmt: j.vmr_fmt.to_string(),
            map_fmt: j.map_fmt.to_string(),
            submit_time: j.submit_time,
            complete_time: j.complete_time,
            output_file: output_file
        })
    }
}

pub struct Job {
    pub job_id: i32,
    pub state: JobState,
    pub site_id: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub lat: Vec<Option<f32>>,
    pub lon: Vec<Option<f32>>,
    pub email: Option<String>,
    pub delete_time: Option<NaiveDateTime>,
    pub priority: i32,
    pub save_dir: PathBuf,
    pub save_tarball: TarChoice,
    pub mod_fmt: ModFmt,
    pub vmr_fmt: VmrFmt,
    pub map_fmt: MapFmt,
    pub submit_time: NaiveDateTime,
    pub complete_time: Option<NaiveDateTime>,
    pub output_file: Option<PathBuf>
}

impl TryFrom<QJob> for Job {
    type Error = anyhow::Error;

    fn try_from(q: QJob) -> Result<Self, Self::Error> {
        Ok(Job {
            job_id: q.job_id,
            state: JobState::try_from(q.state)?,
            site_id: str_to_json_arr(&q.site_id)?,
            start_date: q.start_date,
            end_date: q.end_date,
            lat: str_to_json_arr(&q.lat)?,
            lon: str_to_json_arr(&q.lon)?,
            email: q.email,
            delete_time: q.delete_time,
            priority: q.priority,
            save_dir: PathBuf::from(q.save_dir),
            save_tarball: TarChoice::try_from(q.save_tarball)?,
            mod_fmt: ModFmt::from_str(&q.mod_fmt)?,
            vmr_fmt: VmrFmt::from_str(&q.vmr_fmt)?,
            map_fmt: MapFmt::from_str(&q.map_fmt)?,
            submit_time: q.submit_time,
            complete_time: q.complete_time,
            output_file: q.output_file.and_then(|p| Some(PathBuf::from(p)))
        })
    }
}

impl Job {
    pub async fn get_job_with_id(pool: &mut MySqlPC, id: i32) -> anyhow::Result<Job> {
        let result = sqlx::query_as!(
                QJob,
                "SELECT * FROM Jobs WHERE job_id = ?",
                id
            ).fetch_one(pool)
            .await?;
    
        return Ok(Job::try_from(result)?)
    }

    pub async fn add_job_from_args(
        pool: &mut MySqlPC,
        site_id: Vec<String>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        save_dir: PathBuf,
        email: Option<String>,
        lat: Vec<Option<f32>>,
        lon: Vec<Option<f32>>,
        mod_fmt: Option<ModFmt>,
        vmr_fmt: Option<VmrFmt>,
        map_fmt: Option<MapFmt>,
        priority: Option<i32>,
        delete_time: Option<NaiveDateTime>,
        save_tarball: TarChoice
    ) -> anyhow::Result<i32> {

        if site_id.len() != lat.len() || site_id.len() != lon.len() {
            anyhow::bail!("site_id, lat, and lon must all be the same length");
        }

        let now = chrono::Local::now().naive_local();
        let mod_fmt: String = mod_fmt.unwrap_or_default().to_string();
        let vmr_fmt: String = vmr_fmt.unwrap_or_default().to_string();
        let map_fmt: String = map_fmt.unwrap_or_default().to_string();
        let save_tarball: i8 = save_tarball.into();
        let complete_time: Option<NaiveDateTime> = None;
        let output_file: Option<String> = None;

        let new_id = sqlx::query!(
            r#"INSERT INTO Jobs (state, site_id, start_date, end_date, lat, lon, email, delete_time, priority, save_dir, save_tarball, mod_fmt, vmr_fmt, map_fmt, submit_time, complete_time, output_file)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            JobState::Pending as i8, // state
            serde_json::to_string(&site_id)?, // site_id
            start_date, // start_date
            end_date, // end_date
            serde_json::to_string(&lat)?, // lat
            serde_json::to_string(&lon)?, // lon
            email, // email
            delete_time, // delete_time
            priority.unwrap_or(0), // priority
            save_dir.to_str().ok_or(anyhow::anyhow!("Could not convert save_dir to UTF string"))?, // save_dir
            save_tarball, // save_tarball
            mod_fmt,
            vmr_fmt,
            map_fmt,
            now, // submit_time
            complete_time, // complete_time
            output_file, // output_file
        ).execute(pool)
        .await?
        .last_insert_id();

        Ok(new_id as i32)
    }

    pub async fn delete_job_with_id(pool: &mut MySqlPool, id: i32) -> anyhow::Result<i64> {
        // TODO: Can we reuse a connection (MySqlPC) instead of the pool? Taking pool as &mut MySqlPc and 
        // passing it to fetch_one would cause a moved error later.
        // must rename COUNT(*) to a valid field name
        let pre_count = sqlx::query!("SELECT COUNT(*) as count FROM Jobs")
            .fetch_one(&mut pool.acquire().await?)
            .await?
            .count;
        
        let pending: i8 = JobState::Pending.into();
        sqlx::query!(
            "DELETE FROM Jobs WHERE job_id = ? AND state = ?",
            id,
            pending
        ).execute(&mut pool.acquire().await?)
        .await?;

        let post_count = sqlx::query!("SELECT COUNT(*) as count FROM Jobs")
            .fetch_one(&mut pool.acquire().await?)
            .await?
            .count;

        return Ok(pre_count - post_count)
    }
}
