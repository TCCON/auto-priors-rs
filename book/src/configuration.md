# Configuration

## Execution

This section has the largest number of options.
Here we will go through them in groups.
An example

```toml
[execution]
# General options
error_handler.type = "EmailAdmins"
max_numpy_threads = 2

# Input file options
input_file_pattern = '/data/ftp/input_ingest/*.txt'
status_request_file_pattern = '/data/ftp/input_ingest/ginput_status*.txt'
success_input_file_dir = '/data/ftp/input_success'
failure_input_file_dir = '/data/ftp/input_failure'

# Download location options
hours_to_keep = 168
ftp_download_server = 'ftp://ccycle.gps.caltech.edu/'
ftp_download_root = '/data/ftp/'
output_path = '/data/ftp/outputs'
std_sites_tar_output = '/data/ftp/std-tarballs'
std_sites_output_base = '/data/ftp/std-runs'

# Job run options
job_max_days = 180
job_split_into_days = 3
simulate = false
simulation_delay = 10
submitted_job_queue = 'submitted'
std_site_job_queue = 'std-sites'

# Site information options
flat_stdsite_json_file = "/data/ftp/outputs/stdsites_flat.json"
grouped_stdsite_json_file = "/data/ftp/outputs/stdsites_grouped.json"

[execution.queues.std-sites]
max_num_procs = 4

[execution.queues.submitted]
max_num_procs = 4
fair_share_policy = {type = "RoundRobin", time_period_days=14}

[execution.ginput."v1.0.6"]
type = 'Script'
entry_point_path = '/home/tccon/auto-priors-rs/python/ginput/run_ginput.py'

[execution.ginput."v1.2.0"]
type = 'Script'
entry_point_path = '/home/tccon/auto-priors-rs/python/ginput-geos-it/run_ginput.py'
```

The following subsections will describe the available options in more detail.

### General

These are settings that control miscellaneous operation of the automation.

- `error_handler`: this controls how the automation responds to an error. The `type` subfield controls which version is used. The options are "Logging" and "EmailAdmins". "Logging" will log the error to the local log file. "EmailAdmins" will also send an error report by email to the individuals specified in the `[email]` section.
- `max_numpy_threads`: this limits how many CPU threads Numpy will use in `ginput`. This can be useful to avoid `ginput` overwhelming your CPU. Note that Numpy can be built with different libraries which may not respect this setting; if you see `ginput` is using too many threads, this is a bug in the Numpy thread limiting in `ginput` itself.

### Input files

These are settings that control how the automation handles input files.

- `input_file_pattern`: a glob style pattern that must match the names of text files users upload by FTP to request jobs.
- `status_request_file_pattern`: a glob style pattern that must match the names of text files users upload to check the status of their jobs.
- `success_input_file_dir`: the directory where the automation will copy input files that are successfully parsed.
- `failure_input_file_dir`: the directory where the automation will copy input files that cannot be successfully parsed.

### Download options

These are settings that relate to how users can download their priors.

- `hours_to_keep`: how many hours the `.tgz` files made in request to job requests will be kept.
- `ftp_download_server`: the name that users use to connect to the FTP server to upload requests and download jobs.
- `ftp_download_root`: the file path _on your server_ where users start (by default) when they connect to the server. This will be used to make paths communicated to the users correctly represent the path they access from the FTP server (as such paths must be relative to this root).
- `output_path`: The directory where user requested jobs will be output. This must be under `ftp_download_root`.
- `std_sites_tar_output`: The directory where the tarballs for the standard sites will be output. This must be under `ftp_download_root`.
- `std_sites_output_base`: The directory where the standard site runs will be carried out. This does _not_ need to be under `ftp_download_root`; it is an intermediate directory.

### Jobs

These are settings that relate to how jobs are run.

- `job_max_days`: The maximum number of days a single user request can ask to generate. This is intended to prevent excessively long jobs clogging up the system.
- `job_split_into_days`: If set, then jobs longer than this length will be split into smaller jobs each running at most this many days. This allows users to submit larger jobs that will be divided into smaller tasks so that the queue can refresh more quickly and allow other users' jobs to start earlier.
- `simulate`: a boolean, that if `true`, will simulate running jobs rather than actually calling `ginput`. Useful for testing.
- `simulation_delay`: how long in seconds to pretend a simulated job takes to run (for testing).

### Site information

These are settings that control where standard site information is provided.

- `flat_stdsite_json_file`: The file path for a JSON file listing the standard sites in "flat" format to be written. "Flat" format means that sites that have different coordinates in different time periods will have a separate top-level entry for each time period.
- `grouped_stdsite_json_file`: The file path for a JSON file listing the standard sites in "grouped" format to be written. "Grouped" means that each site has one top-level entry; if it has multiple coordinates, they will be listed within that same top-level entry.

Ideally, these will be placed somewhere that users can download them.

### Queues

There are two options that set the queues to use for user submitted jobs and standard site jobs:

- `submitted_job_queue`: the name of the queue to use for user-submitted jobs.
- `std_site_job_queue`: the name of the queue to use for jobs generating the standard TCCON/COCCON/etc. sites.

Under `[execution.queues]`, each of these can have their own settings:

```toml
[execution.queues.std-sites]
max_num_procs = 4

[execution.queues.submitted]
max_num_procs = 4
fair_share_policy = {type = "RoundRobin", time_period_days=14}
```

Within [`execution.queues`], the keys must match the queue names; here, this is "std-sites" and "submitted".
Each queue has the following options:

- `max_num_procs`: the maximum number of jobs in this queue that can run simultaneously. Default is 1.
- `fair_share_policy`: controls how jobs are prioritized. The options for the `type` value and additional keys for each type are:
    - `type = "Simple"`: Jobs are prioritized by the order they are submitted and their "priority" value. The priority value can only be set when submitting a job via the CLI. No additional options.
    - `type = "RoundRobin"`: Jobs are initially prioritized following the same rules as "Simple", but users will have their priority reduced by 1 for each job run recently. The additional `time_period_days` option controls that time period from which completed jobs are counted. In the example above for the "submitted" queue, the `time_period_days=14` will reduce user priority for each job run in the preceding two weeks (14 days). 14 days is the default.

### Ginput

The `[execution.ginput]` section defines different ginput versions that the automation can use to generate the priors.
The keys within this section can be anything, and will be used to refer to the `ginput` versions in the [`[[default_options]]`](#default-options) sections.

At present, "Script" is the only possible value for `type`.
This means that the automation calls `ginput` through its `run_ginput.py` CLI.
The "Script" type requires the `entry_point_path` option that points to the `run_ginput.py` script used to access ginput.

## Data

This section relates to input data `ginput` requires to produce priors.
An example data section is:

```toml
[data]
zgrid_file = '/home/tccon/ggg-stable/levels/ap_51_level_0_to_70km.gnd'
base_vmr_file = '/home/tccon/ggg-stable/vmrs/gnd/summer_35N.vmr'


[[data.download.geosfpit]]
product = 'geosfpit'
data_type = 'met'
levels = 'eta'
url_pattern = 'https://dummy.org/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.%Y%m%d_%H%M.V01.nc4'
# basename_pattern = '(omit to infer from url_pattern)'
file_freq_min = 180
earliest_date = '2000-01-01'
download_dir = '/data/GEOS/Nv'
ginput_met_key = "fpit-eta"
ginput_output_subdir = "fpit"
days_latency = 1

[[data.download.geosfpit]]
product = 'geosfpit'
data_type = 'chm'
levels = 'eta'
url_pattern = 'https://dummy.org/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.%Y%m%d_%H%M.V01.nc4'
# basename_pattern = '(omit to infer from url_pattern)'
file_freq_min = 180
earliest_date = '2000-01-01'
download_dir = '/data/GEOS/Nv'
ginput_met_key = "fpit-eta"
ginput_output_subdir = "fpit"
days_latency = 1

[[data.download.geosfpit]]
product = 'geosfpit'
data_type = 'met'
levels = 'surf'
url_pattern = 'https://dummy.org/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.%Y%m%d_%H%M.V01.nc4'
# basename_pattern = '(omit to infer from url_pattern)'
file_freq_min = 180
earliest_date = '2000-01-01'
download_dir = '/data/GEOS/Nx'
ginput_met_key = "fpit-eta"
ginput_output_subdir = "fpit"
days_latency = 1
```

The first two keys point to GGG files that `ginput` uses to ensure that the `.vmr` files it produces are consistent with what GGG expects:

- `zgrid_file`: this is the "levels" file that defines the altitude grid that GGG executes the retrieval on. `ginput` uses this for several reasons, including putting the `.vmr` file on those levels and accounting for the CO column above the top retrieval altitude. For standard TCCON GGG2020, use, this should **always** be the `ap_51_level_0_to_70km.gnd` file.
- `base_vmr_file`: this is the climatological `.vmr` file included with GGG. `ginput` uses this to fill in the secondary gases. For standard TCCON GGG2020, this should **always** be the `summer_35N.vmr` file.

The `[[data.download]]` subsections are more complicated.
Each key under `data.download` represents a type of meteorology that the automation can download and provide to `ginput` as a source of met priors.
These are arrays to support a given data product being split across multiple files.
Here, we see that GEOS FP-IT has 3 subproducts: 3D met, 2D met, and 3D chemistry files.
When the automation downloads the met product specified by the subsection key (here, "geosfpit" for example), it will download all the file types specified by the array entries.
(E.g., for GEOS FP-IT, that will be the 3D met, 2D met, and 3D chemistry files.)

The options within each download section are:

- `product`: this will be entered into the database as the product for these files. For now, it is best if this matches the subsection key.
- `data_type`: a string describing the type of data contained in these files, either "met" for meteorological or "chm" for chemical. Currently, this is used to keep track of subproducts (i.e., how the GEOS products are split into met and chem files).
- `levels`: a string describing the vertical levels on which these met files are organized. For GEOS, this will be "eta", "surf", or "pres". Currently, this is used to keep track of subproducts (i.e., how the GEOS products are split into 3D and 2D files).
- `url_pattern`: a string that, when formatted following [strftime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) rules, resolved to the download URL for this met file type for a given time. Note the use of `strftime` formatting symbols in the examples; these can be used any where in the URL so that if, for example, the data are organized into year/month/day directories in the URL, that information can be inserted into the URL appropriately.
- `basename_pattern`: a string that, also when formatted following [strftime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) rules, resolves to the local file name for this file type for the given time. If omitted, this is taken as the final component of the `url_pattern`.
- `file_freq_min`: the temporal spacing, in minutes, between the model output files. In the example, GEOS files are output every 3 hours starting at midnight UTC, so this value is set to 180 minutes. Note that there is no way to specify an "offset", so a model with output at 0100, 0700, 1300, and 1900 cannot be handled with the current version of the automation.
- `earliest_date`: the first date, in `yyyy-mm-dd` format, for which this product is available.
- `download_dir`: the directory to download this product to.

```admonish warning
`ginput` expects that files on hybrid eta levels will be in an `Nv` subdirectory, 2D files in an `Nx` subdirectory, and files on fixed pressure level files (if ever used) will be in an `Np` subdirectory.
Therefore, your `download_dir` paths must have one of these as the terminal component.
```

- `ginput_met_key`: this will be the value given to the `--mode` option for the `run_ginput.py mod` subcommand. For standard GGG2020 TCCON use, this must be either "fpit-eta" or "it-eta".
- `ginput_output_subdir`: this is the subdirectory that `ginput` puts the `.mod` files in within its output directory. This will be "fpit" for GEOS FP-IT and "it" for GEOS IT.
- `days_latency`: this is how many days it takes for this met product to be available. When the automation tries to download met, if it cannot find files for today - `days_latency` days, it will not consider than an error.


## Default options

This section controls which met data and ginput version are used by default for requests for priors in different date ranges.
An example is:

```toml
[[default_options]]
start_date = '2000-01-01'
end_date = '2024-04-01'
ginput = 'v1.0.6'
met = 'geosfpit'

[[default_options]]
start_date = '2024-04-01'
end_date = '2100-01-01'
ginput = 'v1.2.0'
met = 'geosit'
```

In each section, `start_date` and `end_date` give the first and last date this section applies to.
(`end_date` is exclusive.)
`ginput` must then be the key for one of the subsections in `[execution.ginput]` and `met` one of the subsections within `[[data.download]]`.
In the example above, requests for priors covering 1 Jan 2000 to 31 Mar 2024 will use ginput v1.0.6 and GEOS FP-IT met data.
Requests for priors covering 1 Apr 2024 and later will use ginput v1.2.0 and GEOS IT met data.

## Email

This section controls email-related aspects, including both who to email in certain circumstances and how to send emails.
An example is:

```toml
[email]
from_address = 'noreply@test.com'
admin_emails = ['flynn@tron.com', 'sheridan@babylon5.eps']
report_emails = ['test@test.net']
std_site_req_emails = ['reviewer1@rev.net', 'reviewer2@rev.net']
extra_submitters = ['gucamelee@test.net']

[email.backend]
type = 'Internal'

[email.backend.smtp.TlsPassword]
host = 'smtp.gmail.com'
user = 'netrc'
password = 'netrc'
```

In the first section, the options are:

- `from_address`: the address to send emails from, if allowed to set that.
- `admin_emails`: a list of emails to which to send administrative-related emails, usually error reports.
- `report_emails`: a list of emails to which to send the daily/weekly reports.
- `std_site_req_emails`: a list of emails to which to send summaries of requests to be added as a standard site (via a command line tool).
- `extra_submitters`: a list of emails to add to the list of people to send bulk emails to that do not have their email in the database of past jobs.

The `[email.backend]` section controls how emails are sent.
The `type` key can have one of the following four values:

- "Internal": sends emails by directly connecting to an SMTP server.
- "Mailx": calls the `mailx` command line client to send an email.
- "Mock": simply prints the email to the logs (intended for development)
- "Testing": stores the email in a queue to be inspected later (intended for testing)

These have their own sub-options.

### Internal

If there are no additional options specified, this will establish an unencrypted connection to a local SMTP server.
Otherwise, if the `[email.backend.smtp.TlsPassword]` section is defined, it uses those settings to connect to a remote server.
In that case, you must specify:

- `host`: the SMTP host to connect to,
- `user`: the username to authenticate with, and
- `password`: the password to authenticate with.

If user and/or password are the string "netrc", then this will read that value from an entry in your user's `~/.netrc` file with a machine name that matches the `host` value.
For the example above using `smtp.gmail.com` that would look like:

```
machine smtp.gmail.com
login TestUser
password CorrectHorseBatteryStaple
```

(with the actual username and password in place of the examples here).

### Mailx

The only option here is `exec`, which is the path to the `mailx` program.
If omitted, the automation will look for `mail` on your PATH.

### Mock

This has no additional settings.

### Testing

This has no additional settings.

## Timing

This section controls when each of the automation actions are run by the service program.
An example is:

```toml
[timing]
disable_met_download = false
met_download_hours = 6

disable_job = false
job_start_seconds = 60
status_report_seconds = 60
lut_regen_days = 1
lut_regen_at = '00:00:00'
delete_expired_jobs_hours = 12
delete_expired_jobs_offset_minutes = 15

disable_std_site_gen = false
std_site_gen_hours = 24
std_site_gen_offset_minutes = 180
std_site_tar_minutes = 30
std_site_json_hours = 1

disable_reports = false
```

These options can be divided into groups for met download, job execution, standard site generation, and reports:

### Met download

- `disable_met_download`: set to `true` to prevent the service from downloading met data
- `met_download_hours`: how often the service will try to download met data. In the example, it will try every six hours, so 00:00, 06:00, 12:00, and 18:00.

### Job execution

- `disable_job`: set to `true` to prevent running `ginput` jobs.
- `job_start_seconds`: how many seconds between times when the service checks for user-uploaded input files. This should be reasonably frequent to avoid collisions between input files with the same names.
- `status_report_seconds`: how many seconds between times when the service checks for user-uploaded status request files. This should also be reasonably frequent.
- `lut_regen_days`: how often the service will run a special job to regenerate certain look up tables `ginput` uses. These should be updated at least once per month. Running the job only has `ginput` check if they do need updated; if the tables are complete, this job will exit quickly. Therefore, running this every day is safest.
- `lut_regen_at`: the time, in `HH:MM:SS` format, at which the special job to regenerate those `ginput` look up tables will run.
- `delete_expired_jobs_hours`: how often, in hours, the service will delete tarballs for jobs ready to be cleaned up.
- `delete_expired_jobs_offset_minutes`: the offset from the top of the hour in minutes at which the service will delete expired jobs' tarballs. Setting this to e.g., 15 when `delete_expired_jobs_hours` is 1 would run this task at 00:15, 01:15, etc.

### Standard sites

- `disable_std_site_gen`: set to `true` to prevent tasks needed to generate standard sites' priors. Note that producing those priors relies on job execution, so setting `disable_job = true` will also prevent standard sites' priors from being created. (Settings `disable_jobs = true` and `disable_std_site_gen = false` is not recommended.)
- `std_site_gen_hours`: how often the service will submit jobs to create priors for the standard sites.
- `std_site_gen_offset_minutes`: how many minutes from midnight to offset the first run of standard site submission.
- `std_site_tar_minuts`: how often to run a task to make tarballs of the standard sites' priors from the regular job output.
- `std_site_json_hours`: how often to create the grouped and flat JSON files with site coordinates.

### Reports

- `disable_reports`: set to `true` to prevent the service from sending daily and weekly reports.
- `daily_report_time`: local time, in `HH:MM:SS` format, to send daily reports of jobs currently running. The default, if omitted, is midnight.
- `weekly_report_time`: local time, in `HH:MM:SS` format, to send weekly reports of all jobs completed in the past week. The default, if omitted, is midnight.


## Blacklist

The configuration can include a blacklist of users who are no longer permitted
to request priors.
This is primarily for users that abuse the system and request a large volume of
priors without a good reason to do so.
(For example, a COCCON maintainer who needs to reprocess many sites' data with a
new PROFFAST version is expected to request a large volume of priors, but someone
attempting to produce a grid of priors for comparison with a model for their own
research can be blacklisted.)

In the code, a blacklist entry is modeled by the `orm::config::BlacklistEntry` struct.
In the configuration file, a blacklist entry will look like so:

```toml
[[blacklist]]
identifier = { type = "SubmitterEmail", submitter = "did_not_read_the_wiki@sorry.com" }
silent = true
reason = "ignored the restrictions on use given at https://tccon-wiki.caltech.edu/Main/ObtainingGinputData"
```

Each entry must contain all three fields:

- `identifier` (more details below) specifies the user to which this entry applies.
- `silent` specifies whether the user receives an email (`false`) or not (`true`) indicating
that their request has been blocked.
- `reason` is a string recording why the user has been blocked. When `silent` is `false`, it will
be included in the email sent to the user. Otherwise, it is useful as a record of why this user was
blocked.

The identifier is modeled in code by the `orm::config::BlacklistIdentifier` enum.
It was written as an enum in case other variants are required in the future.
Currently, the "SubmitterEmail" type is the only one available, which will reject any
input files with the email specified by the `submitter` field included in the input file.
This is rather weak, as the user can specify any email.
(The system does not check the input file email against the allowed list of FTP users.)


```admonish info
The blacklist is expected to be deprecated in the future, as it is essentially defining
permissions for jobs submitted via the FTP site. Work is underway to replace this with
a more modern and robust web form and API system, which will include a proper permission
system.
```
