//! The main ORM interface to the jobs queue.
//! 
//! 
use std::{path::PathBuf, str::FromStr, fmt::Display};

use anyhow;
use chrono::{NaiveDate, NaiveDateTime};
use serde::Deserialize;
use serde_json;
use sqlx::{self, FromRow, Type};

use crate::{MySqlPC, MySqlPool, siteinfo};

// TODO: change times from Naive to Local (needs changing SQL to timestamp?)

/// Deserialize a JSON string into a vector of a deserializable type
fn str_to_json_arr<'a, T: Deserialize<'a>> (s: &'a str) -> anyhow::Result<Vec<T>> {
    Ok(serde_json::from_str(s)?)
}

/// An enum representing possible states for a priors job
#[derive(Debug, Type, Clone, Copy)]
pub enum JobState {
    /// **\[default\]** This job is queued but has not begun to execute. `i8` value = `0`.
    Pending = 0,
    /// This job is currently processing. `i8` value = `1`.
    Running = 1,
    /// This job has finished successfully. `i8` value = `2`.
    Complete = 2,
    /// This job failed while running. `i8` value = `3`.
    Errored = 3,
    /// The output from the job has been deleted. `i8` value = `4`.
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
    /// Return the default [`JobState`], `Pending`.
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

    /// Convert an i8 to a [`JobState`]
    /// 
    /// # Errors
    /// 
    /// An `Err` is returned if the i8 value does not correspond to
    /// one of the variants of [`JobState`]
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


/// An enum representing the possible options for creating a tarball of job output
#[derive(Debug, Type, Clone, Copy)]
pub enum TarChoice {
    /// Do not make a tarball of the job output. `i8` value = `0`.
    No = 0,
    /// **\[default\]** Do make a tarball of the job output. `i8` value = `1`.
    Yes = 1,
    /// Make a tarball with a special name, compatible with the EGI automation
    /// for EM27s. `i8` value = `2`.
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
    /// Return the default variant of [`TarChoice`] (`Yes`)
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

    /// Convert a `i8` into the equivalent [`TarChoice`] variant
    /// 
    /// # Errors
    /// 
    /// An `Err` is returned is the `i8` value does not correspond to
    /// any [`TarChoice`] variant.
    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::No),
            1 => Ok(Self::Yes),
            2 => Ok(Self::Egi),
            _ => Err(anyhow::anyhow!("Unknown value for TarChoice: {value}"))
        }
    }
}

/// An enum representing the possible output file types for the model (`.mod`) files.
#[derive(Debug, Type)]
pub enum ModFmt {
    /// Do not create `.mod` files. String representation = `"None"`.
    None,
    /// **\[default\]** Create text `.mod` files. String representation = `"Text"`.
    Text
}

impl Default for ModFmt {
    /// Return the default variant of [`ModFmt`], `Text`.
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

    /// Convert a string into a [`ModFmt`] variant.
    /// 
    /// Possible values are "none" and "text" (case insensitive).
    /// 
    /// # Errors
    /// An `Err` is returned if the given string does not match any of the
    /// [`ModFmt`] variants.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            _ => Err(anyhow::anyhow!("Unknown value for ModFmt: {s}"))
        }
    }
}

/// An enum representing the possible output file types for the `.vmr` files.
#[derive(Debug, Type)]
pub enum VmrFmt {
    /// Do not create `.vmr` files. String representation = `"None"`.
    None,
    /// **\[default\]** Create text `.vmr` files. String representation = `"Text"`.
    Text
}

impl Default for VmrFmt {
    /// Return the default variant of [`VmrFmt`], `Text`.
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

    /// Convert a string into a [`VmrFmt`] variant.
    /// 
    /// Possible values are "none" and "text" (case insensitive).
    /// 
    /// # Errors
    /// An `Err` is returned if the given string does not match any of the
    /// [`VmrFmt`] variants.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            _ => Err(anyhow::anyhow!("Unknown value for VmrFmt: {s}"))
        }
    }
}

/// An enum representing the possible output file types for the model a priori (`.map`) files.
#[derive(Debug, Type)]
pub enum MapFmt {
    /// Do not create `.map` files. String representation = `"None"`.
    None,
    /// **\[default\]** Create text `.map` files. String representation = `"Text"`.
    Text,
    /// Create netCDF4 `.map` files. String representation = `"NetCDF"`.
    NetCDF
}

impl Default for MapFmt {
    /// Return the default variant of [`MapFmt`], `Text`.
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

    /// Convert a string into a [`MapFmt`] variant.
    /// 
    /// Possible values are "none", "text", and "netcdf" (case insensitive).
    /// 
    /// # Errors
    /// An `Err` is returned if the given string does not match any of the
    /// [`MapFmt`] variants.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            "netcdf" => Ok(Self::NetCDF),
            _ => Err(anyhow::anyhow!("Unknown value for MapFmt: {s}"))
        }
    }
}

/// An intermediate job representation that maps directly to the MySQL table.
/// 
/// External crates should interact with the [`Job`] struct, and that should
/// have methods that internally work with a `QJob` instance as needed to
/// interface with the MySQL table.
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

    /// Try making a `QJob` instance from a [`Job`]
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * Could not convert the `save_dir` or `output_file` paths to UTF strings
    /// * Could not serialize the `site_id`, `lat`, or `lon` vectors to JSON strings.
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

/// The public interface to the Jobs MySQL table.
pub struct Job {
    /// **\[primary key]** The unique integer ID of this job
    pub job_id: i32,

    /// State of the job, i.e. pending, running, etc.
    pub state: JobState,

    /// The site IDs (generally two characters each) of each location to generate priors
    /// for in this job. The MySQL table enforces that this and the `lat`/`lon` vectors
    /// are the same length.
    pub site_id: Vec<String>,

    /// First date to generate priors for
    pub start_date: NaiveDate,

    /// Date after the last one to generate priors for, i.e. an exclusive end date
    pub end_date: NaiveDate,

    /// Latitudes to generate priors for. Must be the same length as the `site_id` field. 
    /// If one of these is `None`, the job will use the latitude defined in the `StdSitesInfo`
    /// table.
    pub lat: Vec<Option<f32>>,

    /// Longitudes to generate priors for. Same caveats as the `lat` field.
    pub lon: Vec<Option<f32>>,

    /// Email to contact when the job is complete. May be `None` for automatic/background jobs.
    pub email: Option<String>,

    /// Local date & time to clean up output from this job. If `None`, output will never be deleted.
    pub delete_time: Option<NaiveDateTime>,

    /// Priority to give this job, greater values will run first.
    pub priority: i32,

    /// Where to save the output.
    pub save_dir: PathBuf,

    /// Whether to generate a tarball of the output or not.
    /// May also indicate to give it an EGI-compatible name.
    pub save_tarball: TarChoice,

    /// Format to save the `.mod` files in.
    pub mod_fmt: ModFmt,

    /// Format to save the `.vmr` files in.
    pub vmr_fmt: VmrFmt,

    /// Format to save the `.map` files in.
    pub map_fmt: MapFmt,

    /// Time that this job was submitted to the queue.
    pub submit_time: NaiveDateTime,

    /// Time that this job was completed. `None` indicates the job is waiting.
    pub complete_time: Option<NaiveDateTime>,

    /// Location of the output data, either a directory or tarball.
    pub output_file: Option<PathBuf>
}

impl TryFrom<QJob> for Job {
    type Error = anyhow::Error;

    /// Try creating a `Job` instance from the SQL-mapped `QJob`.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * the job state was an unknown integer
    /// * the `site_id` string could not be deserialized to an array
    /// * the `lat` or `lon` values could not be deserialized to arrays
    /// * the `save_tarball` value was an unknown integer
    /// * the `mod_fmt`, `vmr_fmr`, or `map_fmt` value was an unknown string.
    /// 
    /// Generally, errors from this function are fatal and should trigger a message
    /// to the admins, since that indicates the mapping between SQL types and Rust
    /// types has gotten out of sync.
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
    /// Return a `Job` instance with the given `job_id`.
    /// 
    /// # Parameters
    /// * `conn` - a connection to the MySQL database.
    /// * `id` - the ID value to search for.
    /// 
    /// # Errors
    /// Returns an `Err` if
    /// 
    /// * no job with that ID was found.
    /// * the query could not be converted into the Rust `Job` type.
    pub async fn get_job_with_id(conn: &mut MySqlPC, id: i32) -> anyhow::Result<Job> {
        let result = sqlx::query_as!(
                QJob,
                "SELECT * FROM Jobs WHERE job_id = ?",
                id
            ).fetch_one(conn)
            .await?;
    
        return Ok(Job::try_from(result)?)
    }

    /// Convert a user-inputted string of site IDs into a proper vector of site IDs
    /// 
    /// # Parameters
    /// * `site_id_str` - a comma-separated list of site IDs, e.g. "pa,oc,ci"
    pub fn parse_site_id_str(site_id_str: &str) -> Vec<String> {
        return site_id_str
                .split(',')
                .map(|s| s.to_owned())
                .collect();
    }

    /// Convert a user-inputted string of latitudes into a proper vector of latitudes
    /// 
    /// # Parameters
    /// * `lat_str` - a comma-separated list of latitudes, e.g. "45,12.3,-8"
    /// 
    /// # Returns
    /// If the input string was empty, the inner return type will be `None`. Otherwise,
    /// it will be a vector of `Some<f32>`s.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * Any of the values could not be parsed into a float
    /// * Any of the values is outside the range \[-90, 90\].
    /// 
    /// # Notes
    /// This does *not* allow for a mix of numeric and null values; currently users
    /// must either input coordinates for all sites or none of them. In the latter case,
    /// an empty string is the only acceptable input.
    pub fn parse_lat_str(lat_str: &str) -> anyhow::Result<Option<Vec<Option<f32>>>> {
        return Self::parse_latlon_str(lat_str, 90.0, "Latitudes");
    }

    /// Convert a user-inputted string of longitudes into a proper vector of longitudes
    /// 
    /// # Parameters
    /// * `lon_str` - a comma-separated list of longitudes, e.g. "45,12.3,-8"
    /// 
    /// # Returns
    /// If the input string was empty, the inner return type will be `None`. Otherwise,
    /// it will be a vector of `Some<f32>`s.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * Any of the values could not be parsed into a float
    /// * Any of the values is outside the range \[-180, 180\].
    /// 
    /// # Notes
    /// This does *not* allow for a mix of numeric and null values; currently users
    /// must either input coordinates for all sites or none of them. In the latter case,
    /// an empty string is the only acceptable input.
    pub fn parse_lon_str(lon_str: &str) -> anyhow::Result<Option<Vec<Option<f32>>>> {
        return Self::parse_latlon_str(lon_str, 180.0, "Longitudes");
    }

    /// Inner helper function for [`Job::parse_lat_str`] and [`Job::parse_lon_str`]
    /// 
    /// # Parameters
    /// * `coord_str` - the comma-separated list of coordinates
    /// * `limit` - the absolute (positive) limit for the float values; values outside `\[-limit, limit\]` return an `Err`
    /// * `varname` - "Latitude" or "Longitude", to use in the error message.
    fn parse_latlon_str(coord_str: &str, limit: f32, varname: &str) -> anyhow::Result<Option<Vec<Option<f32>>>> {
        if coord_str.len() == 0 {
            return Ok(None)
        }

        let mut values = vec![];
        for s in coord_str.split(',') {
            let v = s.parse()?;
            if v < -limit || v > limit {
                anyhow::bail!("{varname} must be between -{limit:.1} and +{limit:.1}")
            }
            values.push(Some(v))
        }

        return Ok(Some(values));
    }

    /// Convert vectors of site IDs, latitudes, and longitudes to equal lengths.
    /// 
    /// The Jobs SQL table requires that the input vectors of site IDs, latitudes, and longitudes
    /// be of equal length. This function expands user-inputted vectors of these values to be
    /// equal length according to the following rules:
    /// 
    ///  1. Lat and lon must either both or neither be given
    ///  2. If lat/lon not given, they default to vectors of `None`s the same length as site IDs;
    ///     this means that we will infer their lat/lon from the site ID. 
    ///  3. If lat/lon are given, then the site ID vector must be length 1 *or* the same length as the lat/lons
    /// 
    /// # Returns
    /// * The vector of site IDs
    /// * The vector of optional latitudes
    /// * The vector of optional longitudes
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * one of `lat` and `lon` is `None`, but not both
    /// * the lengths of `lat` and `lon` are not equal
    /// * `site_id` is not length 1 or the same length as `lat` and `lon`
    pub fn expand_site_lat_lon(site_id: Vec<String>, lat: Option<Vec<Option<f32>>>, lon: Option<Vec<Option<f32>>>) 
    -> anyhow::Result<(Vec<String>, Vec<Option<f32>>, Vec<Option<f32>>)> {
        // Rules:
        // 

        let lat_given = lat.is_some();
        let lon_given = lon.is_some();

        if lat_given != lon_given {
            anyhow::bail!("lat and lon must both be given or not, cannot have one given and not the other")
        }

        if !lat_given && !lon_given {
            let lat = vec![None; site_id.len()];
            let lon = vec![None; site_id.len()];
            return Ok((site_id, lat, lon))
        }

        let lat = lat.unwrap();
        let lon = lon.unwrap();

        if lat.len() != lon.len() {
            anyhow::bail!("If given, lat and lon must have the same number of elements.")
        }

        if site_id.len() == lat.len() {
            return Ok((site_id, lat, lon))
        }

        if site_id.len() == 1 {
            let site_id = vec![site_id[0].clone(); lat.len()];
            return Ok((site_id, lat, lon))
        }

        anyhow::bail!("site_id must have length 1 or the same number of elements as lat & lon (got {} site ID, {} lat/lon)", 
                      site_id.len(), lat.len());
    }

    /// Add a new job to the database
    /// 
    /// # Parameters
    /// * `conn` - a connection to the MySQL database with the Jobs table
    /// * `site_id` - the vector of site IDs to generate priors for
    /// * `start_date` - first date to generate priors for
    /// * `end_date` - Date after the last one to generate priors for, i.e. an exclusive end date
    /// * `save_dir` - location to save the output
    /// * `email` - optional email to contact when the job is complete or errors
    /// * `lat`, `lon` - vectors of latitude and longitude to generate priors at. May contain `None`s
    ///   if the corresponding site ID is a known standard site with a defined lat/lon. These and
    ///   `site_id` must be vectors of equal length, use [`Job::expand_site_lat_lon`] to expand them
    ///   before passing them in if needed.
    /// * `mod_fmt`, `vmr_fmt`, `map_fmt` - the output formats of the `.mod`, `.vmr`, and `.map` files.
    ///   If `None`, then the default format (usually text) is used.
    /// * `priority` - job priority (greater = higher). If `None`, then a default of 0 is used.
    /// * `delete_time` - date & time after which to delete the output from this job. If `None`, the output
    ///   will never be deleted.
    /// * `save_tarball` - whether to save the output as a tarball or a directory. If `None`, the default
    ///   of [`TarChoice`] is used.
    /// 
    /// # Returns
    /// If successful, returns the `job_id` of the new Job.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * `site_id`, `lat`, and `lon` are not all the same length.
    /// * a lat/lon pair is `None` in one vector but not both.
    /// * a lat/lon pair is `None` but no standard site information is available
    ///   for the corresponding site ID or access to the standard site information table
    ///   fails.
    /// * serializing the `site_id`, `lat`, or `lon` vectors to the SQL table fails
    /// * converting the `save_dir` path to UTF-8 fails
    /// * the INSERT query fails (e.g. if any constraints are violated)
    pub async fn add_job_from_args(
        conn: &mut MySqlPC,
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
        save_tarball: Option<TarChoice>
    ) -> anyhow::Result<i32> {

        // Verify that we have matching site_id, lat, lon vectors. Any expansion needs to be done outside of this function.
        if site_id.len() != lat.len() || site_id.len() != lon.len() {
            anyhow::bail!("site_id, lat, and lon must all be the same length (got {}, {}, {})",
                site_id.len(), lat.len(), lon.len());
        }

        // Also verify that any site_ids for which we do not have defined lat/lons in the inputs are
        // standard sites with at least one time period defined. At the same time, check that we don't 
        // have any lat/lon pairs where only one is None.
        let mut unknown_sids = vec![];
        for (sid, x, y) in itertools::izip!(site_id.iter(), lat.iter(), lon.iter()) {
            if x.is_none() != y.is_none() {
                anyhow::bail!("At least one lat/lon pair has a value for one coordinate but not the other");
            }

            if x.is_none() {
                if !siteinfo::SiteInfo::verify_info_available_for_site(conn, sid).await? {
                    unknown_sids.push(&sid[..]);
                }
            }
        }

        if unknown_sids.len() > 0 {
            let unknown_ids = unknown_sids.join(", ");
            anyhow::bail!("The site IDs {unknown_ids} do not have standard lat/lons associated with them");
        }


        let now = chrono::Local::now().naive_local();
        let mod_fmt: String = mod_fmt.unwrap_or_default().to_string();
        let vmr_fmt: String = vmr_fmt.unwrap_or_default().to_string();
        let map_fmt: String = map_fmt.unwrap_or_default().to_string();
        let save_tarball: i8 = save_tarball.unwrap_or_default().into();
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
        ).execute(conn)
        .await?
        .last_insert_id();

        Ok(new_id as i32)
    }

    /// Delete a pending job from the queue
    /// 
    /// # Parameters
    /// `pool` - a pool of connections to the MySQL database. See Notes.
    /// `id` - the numeric ID (primary key) of the job to delete.
    /// 
    /// # Returns
    /// If no errors encountered, returns the number of jobs deleted (0 or 1).
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * querying for the number of jobs in the table fails
    /// * deleting the row in the SQL table fails
    /// 
    /// # Notes
    /// Unlike most job functions, this needs a pool of database connections, rather than a single
    /// connection. This is an internal implementation detail that can hopefully be addressed in the
    /// future to use a single connection, to be consistent with other job functions.
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
