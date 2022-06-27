CREATE TABLE IF NOT EXISTS GeosPaths (
    path_id INT PRIMARY KEY AUTO_INCREMENT,
    root_path TEXT NOT NULL,
    product VARCHAR(8) NOT NULL,
    levels VARCHAR(8) NOT NULL,
    data_type VARCHAR(8) NOT NULL
);

CREATE TABLE IF NOT EXISTS GeosFiles (
    file_id INT PRIMARY KEY AUTO_INCREMENT,
    file_path TEXT NOT NULL,
    product VARCHAR(8) NOT NULL,
    filedate DATETIME NOT NULL,
    levels VARCHAR(8) NOT NULL,
    data_type VARCHAR(8) NOT NULL
);

CREATE TABLE IF NOT EXISTS Jobs (
    job_id INT PRIMARY KEY AUTO_INCREMENT,
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
    id INT PRIMARY KEY AUTO_INCREMENT,
    site_id CHAR(2) NOT NULL UNIQUE,
    site_type ENUM('Unknown', 'TCCON', 'EM27') NOT NULL DEFAULT 'Unknown'
);

CREATE TABLE IF NOT EXISTS StdSiteInfo (
    id INT PRIMARY KEY AUTO_INCREMENT, 
    site INT NOT NULL,
    name VARCHAR(32) NOT NULL,
    location VARCHAR(64) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    comment TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (site) REFERENCES StdSiteList(id)
);

CREATE OR REPLACE VIEW v_StdSiteInfo
AS
SELECT StdSiteInfo.*, StdSiteList.site_id FROM
StdSiteInfo LEFT JOIN StdSiteList ON StdSiteInfo.site = StdSiteList.id;

CREATE TABLE IF NOT EXISTS StdSiteJobs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    site INT NOT NULL,
    date DATE NOT NULL,
    state TINYINT DEFAULT -1,
    job INT,
    CONSTRAINT U_Site_Date UNIQUE (site, date),
    FOREIGN KEY (site) REFERENCES StdSiteList(id),
    FOREIGN KEY (job) REFERENCES Jobs(job_id)
);

CREATE OR REPLACE VIEW v_StdSiteJobs
AS
SELECT StdSiteJobs.*, StdSiteList.site_id FROM
StdSiteJobs LEFT JOIN StdSiteList ON StdSiteJobs.site = StdSiteList.id;

-- If we knew what sites were possible ahead of time
-- SELECT date, 
-- min(IF(site_id='ae', state, NULL)) as site_ae, 
-- min(IF(site_id='ny', state, NULL)) as site_ny 
-- FROM v_StdSiteJobs GROUP BY date LIMIT 5;
