ALTER TABLE `MetFiles` ADD COLUMN `product_key` VARCHAR(64);

UPDATE `MetFiles`
SET
    `product_key` = CONCAT_WS(
        '-',
        product,
        levels,
        data_type
    );

ALTER TABLE `MetFiles`
MODIFY COLUMN `product_key` VARCHAR(64) NOT NULL;

ALTER TABLE `MetFiles` DROP COLUMN `product`;

ALTER TABLE `MetFiles` DROP COLUMN `levels`;

ALTER TABLE `MetFiles` DROP COLUMN `data_type`;