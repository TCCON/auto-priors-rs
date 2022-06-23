CREATE TABLE IF NOT EXISTS GeosPaths (
    path_id INT PRIMARY KEY,
    root_path TEXT NOT NULL,
    product VARCHAR(8) NOT NULL,
    levels VARCHAR(8) NOT NULL,
    data_type VARCHAR(8) NOT NULL
);

CREATE TABLE IF NOT EXISTS GeosFiles (
    file_id INT PRIMARY KEY,
    file_path TEXT NOT NULL,
    product VARCHAR(8) NOT NULL,
    filedate DATETIME NOT NULL,
    levels VARCHAR(8) NOT NULL,
    data_type VARCHAR(8) NOT NULL
);

CREATE TABLE IF NOT EXISTS Jobs (
    job_id INT PRIMARY KEY,
    state TINYINT NOT NULL,
    site_id TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    lat TEXT NOT NULL,
    lon TEXT NOT NULL,
    email VARCHAR(64),
    delete_time DATETIME,
    priority INTEGER DEFAULT 0,
    save_dir TEXT,
    save_tarball TINYINT DEFAULT 1,
    mod_fmt VARCHAR(8) NOT NULL,
    vmr_fmt VARCHAR(8) NOT NULL,
    map_fmt VARCHAR(8) NOT NULL,
    submit_time DATETIME NOT NULL,
    complete_time DATETIME,
    output_file TEXT
);

CREATE TABLE IF NOT EXISTS StdSiteList (
    id INT PRIMARY KEY,
    site_id CHAR(2) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS StdSiteJobs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    site INT NOT NULL,
    date DATE NOT NULL,
    state TINYINT DEFAULT -1,
    job INT,
    FOREIGN KEY (site) REFERENCES StdSiteList(id),
    FOREIGN KEY (job) REFERENCES Jobs(job_id)
);
