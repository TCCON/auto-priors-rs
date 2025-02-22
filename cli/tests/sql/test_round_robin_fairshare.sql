INSERT INTO Jobs 
    (state, site_id, start_date, end_date, lat, lon, email, save_dir, mod_fmt, vmr_fmt, map_fmt, submit_time, complete_time)
VALUES
-- These first few will be before the round robin consideration period so should not count
    (2, '["xx"]', "2009-01-01", "2010-01-02", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 60 DAY, NOW()),
    (2, '["xx"]', "2009-01-02", "2010-01-03", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 60 DAY, NOW()),
    (2, '["xx"]', "2009-01-03", "2010-01-04", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 60 DAY, NOW()),
-- These 10 should count against user1 
    (2, '["xx"]', "2010-01-01", "2010-01-02", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-02", "2010-01-03", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-03", "2010-01-04", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-04", "2010-01-05", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-05", "2010-01-06", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-06", "2010-01-07", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-07", "2010-01-08", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-08", "2010-01-09", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-09", "2010-01-10", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (2, '["xx"]', "2010-01-10", "2010-01-11", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),

-- These 3 should count against user2 even though some are currently running
    (2, '["yy"]', "2012-01-01", "2012-01-02", "[0.0]", "[0.0]", "user2@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NOW()),
    (1, '["yy"]', "2012-01-02", "2012-01-03", "[0.0]", "[0.0]", "user2@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NULL),
    (1, '["yy"]', "2012-01-03", "2012-01-04", "[0.0]", "[0.0]", "user2@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 HOUR, NULL),
    
-- Give all users at least one pending job
    (0, '["xx"]', "2010-01-11", "2010-01-12", "[0.0]", "[0.0]", "user1@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 30 MINUTE, NULL),
    (0, '["yy"]', "2012-01-04", "2012-01-05", "[0.0]", "[0.0]", "user2@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 25 MINUTE, NULL),
    (0, '["zz"]', "2014-01-01", "2014-01-02", "[0.0]", "[0.0]", "user3@test.net", "/dev/null", "Text", "Text", "Text", NOW() - INTERVAL 1 MINUTE, NULL),
    (0, '["zz"]', "2014-01-02", "2014-01-03", "[0.0]", "[0.0]", "user3@test.net", "/dev/null", "Text", "Text", "Text", NOW(), NULL);