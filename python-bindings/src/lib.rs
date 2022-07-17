use std::ops::DerefMut;
use std::path::PathBuf;
use futures::executor;
use lazy_static::lazy_static;
use pyo3::prelude::*;
use pyo3::exceptions::{PyIOError, PyValueError};
use orm;
use orm::jobs::{Job, JobState, TarChoice, ModFmt, VmrFmt, MapFmt};
use orm::siteinfo::SiteInfo;
use pyo3_chrono::{NaiveDate,NaiveDateTime};
use std::sync::{Mutex,MutexGuard};
use tokio::runtime::Runtime;

lazy_static! {
    static ref BRIDGE: Mutex<TokioBridge> = Mutex::new(TokioBridge::connect().expect("Unable to establish a Tokio runtime"));
}

#[derive(Debug)]
struct TokioBridge {
    conn: orm::MySqlPC,
    rt: Runtime
}

impl TokioBridge {
    fn connect() -> PyResult<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let conn = rt.block_on(Self::get_db_connection())?;
        
        return Ok(Self{conn, rt})
    }

    async fn get_db_connection() -> PyResult<orm::MySqlPC> {
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
}


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
    /// Set the state for this job to running.
    /// 
    /// This will update both the database and this instance. It is
    /// probably a good idea to handle the query for pending jobs and
    /// setting those jobs to running within a transaction, just to 
    /// ensure that no other process tries to start the same job between
    /// the query and the status update.
    /// 
    /// # Raises
    /// Will return a `PyIOError` if it cannot access the database. The
    /// instance's state will not be updated in that case.
    pub fn set_running(&mut self) -> PyResult<()> {
        self.set_state(JobState::Running)?;
        Ok(())
    }

    /// Set the state for this job to errored in both the database and this instance.
    /// 
    /// # Raises
    /// Will return a `PyIOError` if it cannot access the database. The
    /// instance's state will not be updated in that case.
    pub fn set_errored(&mut self) -> PyResult<()> {
        self.set_state(JobState::Errored)?;
        Ok(())
    }

    /// Set the state for this job to complete in both the database and this instance.
    /// 
    /// This will need to know that path to the output file (either the directory in a
    /// non-tarball job or the tarball file in a tarball job). The completion time will
    /// be set to the current local time by the ORM automatically, and this instance will
    /// receive that time and update its fields accordingly.
    /// 
    /// # Raises
    /// Will return a `PyIOError` if it cannot access the database. The
    /// instance's fields will not be updated in that case.
    pub fn set_completed(&mut self, output_file: PathBuf) -> PyResult<()> {
        let mut guard = acquire_runtime()?;
        let bridge = guard.deref_mut();
        let (nrows, complete_time) = match bridge.rt.block_on(Job::set_completed_by_id(&mut *bridge.conn, self.job_id, &output_file, None)) {
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

    /// Internal helper function to set the job state.
    /// 
    /// Note that this should not be used to set the state to "completed"
    /// since that requires additional information.
    fn set_state(&mut self, state: JobState) -> PyResult<()> {
        let mut guard = acquire_runtime()?;
        let bridge = guard.deref_mut();
        if let Err(e) = bridge.rt.block_on( Job::set_state_by_id(&mut *bridge.conn, self.job_id, state) ) {
            let msg = format!("Unable to set job {} state to {state}: {}", self.job_id, e.to_string());
            return Err(PyIOError::new_err(msg))
        }
        self.state = state;
        Ok(())
    }
}

impl TryFrom<Job> for PyJob {
    type Error = pyo3::PyErr;

    /// Try converting an [`orm::jobs::Job`] instance to a [`PyJob`] instance.
    /// 
    /// This mostly copies the relevant fields from the [`orm::jobs::Job`] instance
    /// to the [`PyJob`] instance, but also converts any date/datetime fields into [`pyo3_chrono`]
    /// types and replaces any `None`s in the lats/lons with the correct value
    /// 
    /// # Errors
    /// Returns a Python-compatible error if it annot acquire the ORM connection 
    /// *or* if the correct lat/lons cannot be determined (i.e. mismatched `None`s or 
    /// there is not a unique location defined for a site ID over this job's date range).
    fn try_from(job: Job) -> Result<Self, Self::Error> {
        let complete_time = if let Some(t) = job.complete_time {
            Some(t.into())
        }else{
            None
        };

        let mut guard = acquire_runtime()?;
        let bridge = guard.deref_mut();

        let (lats, lons) = match bridge.rt.block_on(SiteInfo::fill_null_latlons(
                &mut *bridge.conn, &job.site_id, &job.lat, &job.lon, job.start_date, Some(job.end_date)
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


/// Return a list of the next *n* jobs in the order they should be run
/// 
/// # Parameters
/// * `njobs` - the maximum number of jobs to return. The actual number may
///   be less than this if there are fewer jobs pending.
/// 
/// # Returns
/// A list of [`PyJob`] instances
/// 
/// # Raises
/// A `PyIOError` if it (a) cannot acquire a connection to the MySQL database or
/// (b) cannot fill in any NULL lat/lons either because it could not connect to
/// the database, the lat/lon vectors have mismatched NULLs, or a site has multiple
/// locations defined for the job's date range.
#[pyfunction]
fn get_next_jobs(njobs: u32) -> PyResult<Vec<PyJob>> {
    let mut guard = acquire_runtime()?;
    let bridge = guard.deref_mut();
    let orm_jobs = match bridge.rt.block_on( Job::get_next_jobs(&mut *bridge.conn, Some(njobs)) ) {
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

/// Get how many jobs are waiting to be run.
#[pyfunction]
fn get_num_pending_jobs() -> PyResult<u32> {
    let mut guard = acquire_runtime()?;
    let bridge = guard.deref_mut();
    let jobs = match bridge.rt.block_on(Job::get_next_jobs(&mut *bridge.conn, None)) {
        Ok(j) => j,
        Err(e) => {
            let msg = format!("Failed to retrieve the number of pending jobs: {}", e.to_string());
            return Err(PyIOError::new_err(msg));
        }
    };
    return Ok(jobs.len() as u32)
}

/// Take ownership of the static [`TokioBridge`] instance
/// 
/// Because the core ORM uses a Tokio runtime (needed for the Rocket app),
/// any calls to ORM functions must be made from within a Tokio runtime.
/// The [`TokioBridge`] struct will establish the correct runtime and
/// provide a connection to the database. Rather than instantiate one
/// of these instances each time we call a function, this module has 
/// a static reference to one in a [`std::sync::Mutex`]. This function
/// attempts to acquire to lock on that mutex (blocking until it can)
/// and converts a returned `Err` to a [`PyIOError`] so that it can be
/// called with the `?` operator inside a Python function.
/// 
/// One subtlety about the returned value is that it is a [`MutexGuard`],
/// and since you access the bridge itself through dereferencing, you can
/// run into issues where accessing the runtime is an immutable deref, while
/// the connection requires a mutable deref. Thus a typical use of this 
/// function is:
/// 
/// ```no_run
/// # use std::ops::DerefMut;
/// # pyo3::exceptions::PyErr;
/// # use py_ginput_bindings::*;
/// 
/// let guard = acquire_runtime()?;
/// let bridge = guard.deref_mut();
/// let jobs = match bridge.rt.block_on(Job::get_next_jobs(&mut *bridge.conn, None)) {
///     Ok(j) => j,
///     Err(e) => {
///         let msg = format!("Problem getting jobs: {}", e.to_string());
///         return Err(PyErr::new_err(msg));
///     }
/// };
/// println!("There are {} jobs pending", jobs.len());
/// # Ok::<(), PyErr>(())
/// ```
/// 
/// Any code that calls the ORM must run in the `bridge.rt.block_on` call, and since most
/// ORM functions expect a [`sqlx::MySqlConnection`], the bridge's pool connection must be
/// dereferences and mutably referenced to create a matching connection.
fn acquire_runtime() -> PyResult<MutexGuard<'static, TokioBridge>> {
    let bridge = match BRIDGE.lock() {
        Ok(b) => b,
        Err(e) => {
            let msg = format!("Unable to acquire lock on Tokio connection: {}", e.to_string());
            return Err(PyIOError::new_err(msg));
        }
    };

    Ok(bridge)
}

/// A Python module implemented in Rust.
#[pymodule]
fn py_ginput_bindings(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_num_pending_jobs, m)?)?;
    m.add_function(wrap_pyfunction!(get_next_jobs, m)?)?;
    Ok(())
}