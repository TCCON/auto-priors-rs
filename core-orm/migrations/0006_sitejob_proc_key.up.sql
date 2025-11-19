-- A NULL default is fine, we will continue to use that to mean
-- "process using the default", to stay consistent with the jobs
-- table.
ALTER TABLE `StdSiteJobs` ADD COLUMN `processing_key` VARCHAR(64);

-- However, we'll need to update the view as well to ensure it has
-- the processing key.

CREATE OR REPLACE VIEW `v_StdSiteJobs` AS
select
    `StdSiteJobs`.`id` AS `id`,
    `StdSiteJobs`.`site` AS `site`,
    `StdSiteJobs`.`date` AS `date`,
    `StdSiteJobs`.`processing_key` AS `processing_key`,
    `StdSiteJobs`.`state` AS `state`,
    `StdSiteJobs`.`job` AS `job`,
    `StdSiteJobs`.`tarfile` AS `tarfile`,
    `StdSiteList`.`site_id` AS `site_id`,
    `StdSiteList`.`site_type` AS `site_type`,
    `StdSiteList`.`output_structure` AS `output_structure`
from (
        `StdSiteJobs`
        left join `StdSiteList` on (
            `StdSiteJobs`.`site` = `StdSiteList`.`id`
        )
    );