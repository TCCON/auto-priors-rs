ALTER TABLE `MetFiles` ADD COLUMN `product` varchar(8);

ALTER TABLE `MetFiles` ADD COLUMN `levels` varchar(8);

ALTER TABLE `MetFiles` ADD COLUMN `data_type` varchar(8);

-- This assumes that we are undoing the migration and don't have any
-- new files that do not follow the product-levels-datatype key structure.
-- If that's not the case, we will end up with some unsound rows, but
-- there's nothing we can do about that.
UPDATE `MetFiles`
SET
    `product` = SUBSTRING_INDEX(product_key, '-', 1),
    -- get everything up to the second dash, then the part from that after the first,
    -- this will yield the middle element.
    `levels` = SUBSTRING_INDEX(
        SUBSTRING_INDEX(product_key, '-', 2),
        '-',
        -1
    ),
    `data_type` = SUBSTRING_INDEX(product_key, '-', -1);

ALTER TABLE `MetFiles` MODIFY COLUMN `product` VARCHAR(8) NOT NULL;

ALTER TABLE `MetFiles` MODIFY COLUMN `levels` VARCHAR(8) NOT NULL;

ALTER TABLE `MetFiles` MODIFY COLUMN `data_type` VARCHAR(8) NOT NULL;

ALTER TABLE `MetFiles` DROP COLUMN `product_key`;