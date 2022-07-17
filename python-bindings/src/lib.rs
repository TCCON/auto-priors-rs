use std::path::PathBuf;
use futures::executor;
use pyo3::prelude::*;
use pyo3::exceptions::{PyIOError, PyValueError};
use orm;
use orm::jobs::{Job, JobState, TarChoice, ModFmt, VmrFmt, MapFmt};
use orm::siteinfo::SiteInfo;
use pyo3_chrono::{NaiveDate,NaiveDateTime};


#[pyclass]
pub struct PyJob {
    job_id: i32,
    #[pyo3(get)]
    pub state: JobState,
    #[pyo3(get)]
    pub site_id: Vec<String>,
    #[pyo3(get)]
    pub start_date: NaiveDate,
    #[pyo3(get)]
    pub end_date: NaiveDate,
    #[pyo3(get)]
    pub lat: Vec<f32>,
    #[pyo3(get)]
    pub lon: Vec<f32>,
    #[pyo3(get)]
    pub save_dir: PathBuf,
    #[pyo3(get)]
    pub save_tarball: TarChoice,
    #[pyo3(get)]
    pub mod_fmt: ModFmt,
    #[pyo3(get)]
    pub vmr_fmt: VmrFmt,
    #[pyo3(get)]
    pub map_fmt: MapFmt,
    #[pyo3(get)]
    pub complete_time: Option<NaiveDateTime>,
    #[pyo3(get)]
    pub output_file: Option<PathBuf>
}

#[pymethods]
impl PyJob {
    pub fn set_running(&mut self) -> PyResult<()> {
        self.set_state(JobState::Running)?;
        Ok(())
    }

    pub fn set_errored(&mut self) -> PyResult<()> {
        self.set_state(JobState::Errored)?;
        Ok(())
    }

    pub fn set_completed(&mut self, output_file: PathBuf) -> PyResult<()> {
        let mut conn = get_db_connection()?;
        let (nrows, complete_time) = match executor::block_on(Job::set_completed_by_id(&mut *conn, self.job_id, &output_file, None)) {
            Ok((n, t)) => (n, t),
            Err(e) => {
                let msg = format!("Unable to set job {} state to completed: {}", self.job_id, e.to_string());
                return Err(PyIOError::new_err(msg))
            }
        };

        if nrows == 0 {
            let msg = format!("Failed to set job {} to completed; no rows matched it's ID", self.job_id);
            return Err(PyIOError::new_err(msg))
        }

        self.state = JobState::Complete;
        self.output_file = Some(output_file.to_owned());
        self.complete_time = Some(complete_time.into());

        Ok(())
    }

    fn set_state(&mut self, state: JobState) -> PyResult<()> {
        let mut conn = get_db_connection()?;
        if let Err(e) = executor::block_on( Job::set_state_by_id(&mut *conn, self.job_id, state) ) {
            let msg = format!("Unable to set job {} state to {state}: {}", self.job_id, e.to_string());
            return Err(PyIOError::new_err(msg))
        }
        self.state = state;
        Ok(())
    }
}

impl TryFrom<Job> for PyJob {
    type Error = pyo3::PyErr;

    fn try_from(job: Job) -> Result<Self, Self::Error> {
        let complete_time = if let Some(t) = job.complete_time {
            Some(t.into())
        }else{
            None
        };

        let mut conn = get_db_connection()?;

        let (lats, lons) = match executor::block_on(SiteInfo::fill_null_latlons(
                &mut conn, &job.site_id, &job.lat, &job.lon, job.start_date, Some(job.end_date)
            )) {
                Ok((y,x)) => (y, x),
                Err(e) => {
                    let msg = format!("Unable to ensure lats and lons are all not None: {}", e.to_string());
                    return Err(PyIOError::new_err(msg))
            }
        };

        return Ok(Self { 
            job_id: job.job_id,
            state: job.state,
            site_id: job.site_id,
            start_date: job.start_date.into(),
            end_date: job.end_date.into(),
            lat: lats,
            lon: lons,
            save_dir: job.save_dir,
            save_tarball: job.save_tarball,
            mod_fmt: job.mod_fmt,
            vmr_fmt: job.vmr_fmt,
            map_fmt: job.map_fmt,
            complete_time: complete_time,
            output_file: job.output_file 
        })
    }
}


/// Formats the sum of two numbers as string.
#[pyfunction]
fn get_next_jobs(njobs: u32) -> PyResult<Vec<PyJob>> {
    let mut conn = get_db_connection()?;
    let orm_jobs = match executor::block_on( Job::get_next_jobs(&mut *conn, Some(njobs)) ) {
        Ok(jobs) => jobs,
        Err(e) => {
            let msg = format!("Unable to get next jobs: {}", e.to_string());
            return Err(PyIOError::new_err(msg));
        }
    };

    let mut py_jobs = vec![];
    for job in orm_jobs {
        py_jobs.push(job.try_into()?);
    }
    return Ok(py_jobs);
}

/// A Python module implemented in Rust.
#[pymodule]
fn py_ginput_bindings(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_next_jobs, m)?)?;
    Ok(())
}

fn get_db_connection() -> PyResult<orm::MySqlPC> {
    let db = match executor::block_on(orm::get_database_pool(None)) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("Unable to connect to the ORM database: {}", e.to_string());
            return Err(PyIOError::new_err(msg))
        }
    };

    let conn = match executor::block_on(db.acquire()) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Unable to acquire a connection with the ORM database: {}", e.to_string());
            return Err(PyIOError::new_err(msg))
        }
    };

    Ok(conn)
}