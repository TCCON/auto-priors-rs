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

-- Good grief: updating the unique date+site constraint turned out to be
-- a lot more painful because site is also a foreign key, and I didn't give
-- it a fixed name. This next part is all just to temporarily undo that FK
-- constraint.

-- 1. DECLARE a user variable to hold the foreign key name. We may not know
-- what this is because the initial migration sets it automatically.
SET @fk_name = NULL;

-- 2. FIND and CAPTURE the system-generated Foreign Key name for the 'site' column.
SELECT constraint_name INTO @fk_name
FROM information_schema.key_column_usage
WHERE
    table_schema = DATABASE()
    AND table_name = 'StdSiteJobs'
    AND column_name = 'site'
    AND referenced_table_name = 'StdSiteList' -- Ensures we grab the right FK
    AND constraint_name LIKE 'stdsitejobs_ibfk_%' -- Pattern for auto-named FKs
LIMIT 1;

-- Check if the variable was set (optional, but good for debugging)
-- SELECT @fk_name;

-- 3. DROP the Foreign Key dependency using the captured name.
-- This is necessary because the single-column FK on 'site' prevents dropping the composite UNIQUE KEY.
SET
    @sql_drop_fk = CONCAT(
        'ALTER TABLE StdSiteJobs DROP FOREIGN KEY ',
        @fk_name,
        ';'
    );

PREPARE stmt FROM @sql_drop_fk;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

-- 4. DROP the old Unique Key constraint.
ALTER TABLE StdSiteJobs DROP INDEX U_Site_Date;

-- 5. ADD the new composite unique key constraint
ALTER TABLE `StdSiteJobs`
ADD CONSTRAINT `U_Site_Date_Proc` UNIQUE (
    `site`,
    `date`,
    `processing_key`
);

-- 6. RECREATE the Foreign Key using the captured name, restoring the original dependency.
-- Reuse the original name to make reverting easier if needed.
SET
    @sql_readd_fk = CONCAT(
        'ALTER TABLE StdSiteJobs ADD CONSTRAINT ',
        @fk_name,
        ' FOREIGN KEY (site) REFERENCES StdSiteList(id);'
    );

PREPARE stmt FROM @sql_readd_fk;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

-- We'll need to update the view as well to ensure it has
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