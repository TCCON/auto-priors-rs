-- Map the user ID from the Django database view to their API keys
-- used in the refresh/login tokens
CREATE TABLE IF NOT EXISTS `auth_api_user` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `user_id` int(11) NOT NULL,
    `api_key` char(44) NOT NULL,
    `expires` datetime NOT NULL,
    `nickname` text NOT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

-- The list of permissions available, with a short tag used to refer to them
-- in the code, and a description to use in interfaces.
CREATE TABLE IF NOT EXISTS `auth_prior_permissions` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `tag` varchar(16) NOT NULL,
    `description` text NOT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

-- Since the permissions are intricately linked with the code, we define
-- them here rather than through a web interface. These should only be
-- the standard user permissions, admin/superuser privileges will be defined
-- on the Django side.
-- TODO: I should make an enum that represents these as well with a unit test
-- to confirm that the enum and the list of permissions below

INSERT INTO
    `auth_prior_permissions` (`tag`, `description`)
VALUES (
        'QUERY',
        'Permission to check the status of requests, automatic generation, met, etc.'
    );

INSERT INTO
    `auth_prior_permissions` (`tag`, `description`)
VALUES (
        'SUBMIT',
        'Permission to submit requests for priors generation'
    );

INSERT INTO
    `auth_prior_permissions` (`tag`, `description`)
VALUES (
        'DOWNLOAD',
        'Permission to download priors files'
    );

-- Map user IDs to the permission IDs, to know what permissions each user has.
CREATE TABLE IF NOT EXISTS `auth_prior_user_permissions` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `user_id` int(11) NOT NULL,
    `perm_id` int(11) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `U_User_Perm` (`user_id`, `perm_id`),
    FOREIGN KEY (`perm_id`) REFERENCES `auth_prior_permissions` (`id`) ON DELETE CASCADE
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;
