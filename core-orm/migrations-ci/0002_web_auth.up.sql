-- Normally, this should be a view to the Django auth table.
-- However, for automated testing, we don't have that, so
-- we create the subset of it we need for the other
CREATE TABLE IF NOT EXISTS `v_auth_user` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `password` varchar(128) NOT NULL,
    `username` varchar(150) NOT NULL,
    `email` varchar(254) NOT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;