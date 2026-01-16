-- Insert entries for the met data for the days around the configuration transition.
-- In the future, this might be able to be unified with some of the met test files.
-- I used Google Gemini to figure out how to do this "recursive series" thing so
-- I don't have to hand write dozens of rows.

--    DateSeriesPost AS (
--        -- First date that we want
--        SELECT CAST('2023-06-01' AS DATETIME) AS gen_date
--        UNION ALL
--        -- Recusion to generate the full sequence of dates
--        SELECT DATE_ADD(gen_date, INTERVAL 3 HOUR)
--        FROM DateSeriesPost
--            -- Stop before June 3rd
--        WHERE
--            gen_date < CAST('2023-06-03' AS DATETIME)
--    )

INSERT INTO
    MetFiles (
        file_path,
        product_key,
        filedate
    )
WITH RECURSIVE
    DateSeriesPre AS (
        -- First date that we want
        SELECT CAST(
                '2023-05-30 00:00:00' AS DATETIME
            ) AS gen_date
        UNION ALL
        -- Recusion to generate the full sequence of dates
        SELECT DATE_ADD(gen_date, INTERVAL 3 HOUR)
        FROM DateSeriesPre
            -- Stop before June 1st - this behaves like a "while < X" clause, so the
            -- date here gets included.
        WHERE
            gen_date < CAST(
                '2023-05-31 21:00:00' AS DATETIME
            )
    )
SELECT DATE_FORMAT(
        gen_date, 'GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.%Y%m%d_%H%i.V01.nc4'
    ), 'geosfpit-surf-met', gen_date
FROM DateSeriesPre
UNION ALL
SELECT DATE_FORMAT(
        gen_date, 'GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.%Y%m%d_%H%i.V01.nc4'
    ), "geosfpit-eta-met", gen_date
FROM DateSeriesPre
UNION ALL
SELECT DATE_FORMAT(
        gen_date, 'GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.%Y%m%d_%H%i.V01.nc4'
    ), "geosfpit-eta-chm", gen_date
FROM DateSeriesPre
UNION ALL
SELECT DATE_FORMAT(
        gen_date, 'GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.%Y-%m-%dT%H%i.V01.nc4'
    ), "geosit-eta-chm", gen_date
FROM DateSeriesPre;

INSERT INTO
    MetFiles (
        file_path,
        product_key,
        filedate
    )
WITH RECURSIVE
    DateSeriesPost AS (
        -- First date that we want
        SELECT CAST(
                '2023-06-01 00:00:00' AS DATETIME
            ) AS gen_date
        UNION ALL
        -- Recusion to generate the full sequence of dates
        SELECT DATE_ADD(gen_date, INTERVAL 3 HOUR)
        FROM DateSeriesPost
            -- Stop before June 3rd - this behaves like a "while < X" clause, so the
            -- date here gets included.
        WHERE
            gen_date < CAST(
                '2023-06-02 21:00:00' AS DATETIME
            )
    )
SELECT DATE_FORMAT(
        gen_date, 'GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.%Y-%m-%dT%H%i.V01.nc4'
    ), 'geosit-surf-met', gen_date
FROM DateSeriesPost
UNION ALL
SELECT DATE_FORMAT(
        gen_date, 'GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.%Y-%m-%dT%H%i.V01.nc4'
    ), "geosit-eta-met", gen_date
FROM DateSeriesPost
UNION ALL
SELECT DATE_FORMAT(
        gen_date, 'GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.%Y-%m-%dT%H%i.V01.nc4'
    ), "geosit-eta-chm", gen_date
FROM DateSeriesPost;