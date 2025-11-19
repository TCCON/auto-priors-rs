-- Initially, I thought I would leave this as a NULL default.
-- On reflection, I decided that doesn't make sense - we don't
-- rely on the default for the automatic site generation, and
-- we want to know if a set of dates was generated with a processing
-- config that is not being run automatically anymore. The problem
-- is we don't know what the existing row's processing key needs to
-- be, that depends on the configuration file. The best we can do
-- is to set them to placeholder strings and have a program to update them
-- based on the config.
ALTER TABLE `StdSiteJobs` ADD COLUMN `processing_key` VARCHAR(64);

UPDATE `StdSiteJobs` SET `processing_key` = 'PLACEHOLDER';

ALTER TABLE `StdSiteJobs`
MODIFY COLUMN `processing_key` VARCHAR(64) NOT NULL;

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