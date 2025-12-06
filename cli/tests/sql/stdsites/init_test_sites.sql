-- Defines two test sites for the standard site jobs along with periods that they are "active"
-- so that we can request jobs for them.
INSERT INTO
    StdSiteList (
        id,
        site_id,
        name,
        site_type,
        output_structure
    )
VALUES (
        1,
        "pa",
        "Park Falls",
        "TCCON",
        "FlatModVmr"
    ),
    (
        2,
        "oc",
        "Lamont",
        "TCCON",
        "FlatModVmr"
    );

-- We will use the true locations for Park Falls and Lamont, since this will
-- make it easier to compare to past runs of ginput.

INSERT INTO
    StdSiteInfo (
        site,
        location,
        latitude,
        longitude,
        start_date
    )
VALUES (
        1,
        "Wisconsin, USA",
        45.945,
        -90.273,
        "2004-05-01"
    ),
    (
        2,
        "Oklahoma, USA",
        36.604,
        -97.486,
        "2008-07-01"
    );