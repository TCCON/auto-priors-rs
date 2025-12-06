-- We will need some sites defined for the job's foreign keys, so this
-- should always be run after init_test_sites.sql.
-- Then for each site, we'll want a variety of dates and processing
-- keys so that the tests for site ID, date, and proc key have some
-- entries to slice.

INSERT INTO
    StdSiteJobs (
        site,
        date,
        processing_key,
        state
    )
VALUES (1, "2023-05-31", "ALPHA", 0),
    (1, "2023-06-01", "BETA", 0),
    (1, "2023-06-02", "BETA", 0),
    (2, "2023-05-31", "BETA", 0),
    (2, "2023-06-01", "ALPHA", 0),
    (2, "2023-06-02", "BETA", 0);