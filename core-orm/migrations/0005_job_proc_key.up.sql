ALTER TABLE `Jobs` ADD COLUMN `processing_key` VARCHAR(64);

-- We use CONCAT here because we want the processing_key to be
-- NULL if met_key or ginput_key was. We need to keep that NULL
-- because the way the job runner works allows it to handle jobs
-- that do not define a specific processing key and cross the boundary
-- between two defaults.
UPDATE `Jobs`
SET
    `processing_key` = CONCAT(met_key, '-', ginput_key);

ALTER TABLE `Jobs` DROP COLUMN `met_key`;

ALTER TABLE `Jobs` DROP COLUMN `ginput_key`;