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

-- As in the "up" migration, we have to temporarily undo the
-- site foreign key constraint to change our unique constraint.

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

-- 4. Remove the new unique constraint
ALTER TABLE `StdSiteJobs` DROP INDEX `U_Site_Date_Proc`;

-- 5. Restore the old unique constraint
ALTER TABLE `StdSiteJobs`
ADD CONSTRAINT `U_Site_Date` UNIQUE (`site`, `date`);

-- 6. RECREATE the Foreign Key using the captured name, restoring the original dependency.
SET
    @sql_readd_fk = CONCAT(
        'ALTER TABLE StdSiteJobs ADD CONSTRAINT ',
        @fk_name,
        ' FOREIGN KEY (site) REFERENCES StdSiteList(id);'
    );

PREPARE stmt FROM @sql_readd_fk;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

-- Do the alteration to the underlying table second so that the view no
-- longer expects this column.
ALTER TABLE `StdSiteJobs` DROP COLUMN `processing_key`;