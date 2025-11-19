-- As far as I can see, there is no way to just remove a column from a view,
-- so we alter the site jobs view to return it to the old list of columns
-- from migration 0001.
ALTER VIEW `v_StdSiteJobs` AS
select
    `StdSiteJobs`.`id` AS `id`,
    `StdSiteJobs`.`site` AS `site`,
    `StdSiteJobs`.`date` AS `date`,
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

-- Do the alteration to the underlying table second so that the view no
-- longer expects this column.
ALTER TABLE `StdSiteJobs` DROP COLUMN `processing_key`;