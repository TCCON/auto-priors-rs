ALTER TABLE `Jobs` ADD COLUMN `met_key` varchar(32);

ALTER TABLE `Jobs` ADD COLUMN `ginput_key` varchar(32);

-- This assumes that we are undoing the migration and don't have any
-- new jobs that do not follow the metkey-ginputkey key structure.
-- If that's not the case, we will end up with some unsound rows, but
-- there's nothing we can do about that.
UPDATE `Jobs`
SET
    `met_key` = SUBSTRING_INDEX(processing_key, '-', 1),
    `ginput_key` = SUBSTRING_INDEX(processing_key, '-', -1)
WHERE
    `processing_key` IS NOT NULL;

-- The rest of the rows should leave met_key and ginput_key as NULL,
-- which is the old way of specifying that they used the dynamic defaults.

ALTER TABLE `Jobs` DROP COLUMN `processing_key`;