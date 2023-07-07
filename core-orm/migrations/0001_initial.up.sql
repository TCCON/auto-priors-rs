--
-- Table structure for table `MetFiles`
--

CREATE TABLE IF NOT EXISTS `MetFiles` (
  `file_id` int(11) NOT NULL AUTO_INCREMENT,
  `file_path` text NOT NULL,
  `product` varchar(8) NOT NULL,
  `filedate` datetime NOT NULL,
  `levels` varchar(8) NOT NULL,
  `data_type` varchar(8) NOT NULL,
  PRIMARY KEY (`file_id`),
  UNIQUE (`file_path`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `GeosPaths`. May no longer be needed?
--

CREATE TABLE IF NOT EXISTS `GeosPaths` (
  `path_id` int(11) NOT NULL AUTO_INCREMENT,
  `root_path` text NOT NULL,
  `product` varchar(8) NOT NULL,
  `levels` varchar(8) NOT NULL,
  `data_type` varchar(8) NOT NULL,
  PRIMARY KEY (`path_id`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `Jobs`
--

CREATE TABLE IF NOT EXISTS `Jobs` (
  `job_id` int(11) NOT NULL AUTO_INCREMENT,
  `state` tinyint(4) NOT NULL,
  `site_id` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `start_date` date NOT NULL,
  `end_date` date NOT NULL,
  `lat` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `lon` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `email` varchar(64) DEFAULT NULL,
  `delete_time` datetime DEFAULT NULL,
  `priority` int(11) NOT NULL DEFAULT 0,
  `save_dir` text NOT NULL,
  `save_tarball` tinyint(4) NOT NULL DEFAULT 1,
  `mod_fmt` varchar(8) NOT NULL,
  `vmr_fmt` varchar(8) NOT NULL,
  `map_fmt` varchar(8) NOT NULL,
  `submit_time` datetime NOT NULL,
  `complete_time` datetime DEFAULT NULL,
  `output_file` text DEFAULT NULL,
  PRIMARY KEY (`job_id`),
  CONSTRAINT `Eq_Id_Lat_Lon` CHECK (json_length(`site_id`) = json_length(`lat`) and json_length(`site_id`) = json_length(`lon`))
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `StdSiteList`
--

CREATE TABLE IF NOT EXISTS `StdSiteList` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `site_id` char(2) NOT NULL,
  `site_type` enum('Unknown','TCCON','EM27') NOT NULL DEFAULT 'Unknown',
  PRIMARY KEY (`id`),
  UNIQUE KEY `site_id` (`site_id`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `StdSiteInfo`
--

CREATE TABLE IF NOT EXISTS `StdSiteInfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `site` int(11) NOT NULL,
  `name` varchar(32) NOT NULL,
  `location` varchar(64) NOT NULL,
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  `start_date` date NOT NULL,
  `end_date` date DEFAULT NULL,
  `comment` text NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `site` (`site`),
  FOREIGN KEY (site) REFERENCES StdSiteList(id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `StdSiteJobs`
--

CREATE TABLE IF NOT EXISTS `StdSiteJobs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `site` int(11) NOT NULL,
  `date` date NOT NULL,
  `state` tinyint(4) NOT NULL DEFAULT -1,
  `job` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `U_Site_Date` (`site`,`date`),
  KEY `job` (`job`),
  FOREIGN KEY (`site`) REFERENCES `StdSiteList`(`id`),
  FOREIGN KEY (`job`) REFERENCES `Jobs`(`job_id`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for view `v_StdSiteInfo`
--

CREATE OR REPLACE VIEW `v_StdSiteInfo` AS select 
  `StdSiteInfo`.`id` AS `id`,
  `StdSiteInfo`.`site` AS `site`,
  `StdSiteInfo`.`name` AS `name`,
  `StdSiteInfo`.`location` AS `location`,
  `StdSiteInfo`.`latitude` AS `latitude`,
  `StdSiteInfo`.`longitude` AS `longitude`,
  `StdSiteInfo`.`start_date` AS `start_date`,
  `StdSiteInfo`.`end_date` AS `end_date`,
  `StdSiteInfo`.`comment` AS `comment`,
  `StdSiteList`.`site_id` AS `site_id` from (`StdSiteInfo` left join `StdSiteList` on(`StdSiteInfo`.`site` = `StdSiteList`.`id`));

--
-- Table structure for view `v_StdSiteJobs`
--

CREATE OR REPLACE VIEW `v_StdSiteJobs` AS select 
  `StdSiteJobs`.`id` AS `id`,
  `StdSiteJobs`.`site` AS `site`,
  `StdSiteJobs`.`date` AS `date`,
  `StdSiteJobs`.`state` AS `state`,
  `StdSiteJobs`.`job` AS `job`,
  `StdSiteList`.`site_id` AS `site_id` from (`StdSiteJobs` left join `StdSiteList` on(`StdSiteJobs`.`site` = `StdSiteList`.`id`));