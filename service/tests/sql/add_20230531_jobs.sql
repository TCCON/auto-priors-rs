-- Insert both the standard and alternate processing configurations
-- for Park Fall (site = 1) and Lamont (site = 2) for May 31st, set
-- to complete (state = 2)
INSERT INTO
    StdSiteJobs (
        site,
        date,
        state,
        processing_key
    )
VALUES (
        1,
        "2023-05-31",
        2,
        "std-geosfpit"
    ),
    (
        1,
        "2023-05-31",
        2,
        "altco-geosfpit"
    ),
    (
        2,
        "2023-05-31",
        2,
        "std-geosfpit"
    ),
    (
        2,
        "2023-05-31",
        2,
        "altco-geosfpit"
    );