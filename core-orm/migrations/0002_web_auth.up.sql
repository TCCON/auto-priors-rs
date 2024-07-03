-- This assumes that the Django database is named "djopstat".
-- The "SQL SECURITY INVOKER" means that grants are based on the user which actually tries
-- to access the view, rather than who created the view.
-- Since these should probably be read only, will need to grant permission to the priors
-- user to select from "djopstat" tables with: GRANT SELECT ON `djopstat`.* TO `priors`@`localhost`;
-- (assuming the user is `priors`@`localhost`).
CREATE OR REPLACE SQL SECURITY INVOKER VIEW `v_auth_group` AS SELECT * FROM `djopstat`.`auth_group`;
CREATE OR REPLACE SQL SECURITY INVOKER VIEW `v_auth_group_permissions` AS SELECT * FROM `djopstat`.`auth_group_permissions`;
CREATE OR REPLACE SQL SECURITY INVOKER VIEW `v_auth_permission` AS SELECT * FROM `djopstat`.`auth_permission`;
CREATE OR REPLACE SQL SECURITY INVOKER VIEW `v_auth_user` AS SELECT * FROM `djopstat`.`auth_user`;
CREATE OR REPLACE SQL SECURITY INVOKER VIEW `v_auth_user_groups` AS SELECT * FROM `djopstat`.`auth_user_groups`;
CREATE OR REPLACE SQL SECURITY INVOKER VIEW `v_auth_user_user_permissions` AS SELECT * FROM `djopstat`.`auth_user_user_permissions`;
