-- Insert Jan 1 2020 as a complete day for FPIT. 
-- The files paths shouldn't matter, since we don't actually need the files.

INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES ("geos_test_20200101_0000.nc", "fpit", "2020-01-01 00:00:00", "surf", "met"),
    ("geos_test_20200101_0300.nc", "fpit", "2020-01-01 03:00:00", "surf", "met"),
    ("geos_test_20200101_0600.nc", "fpit", "2020-01-01 06:00:00", "surf", "met"),
    ("geos_test_20200101_0900.nc", "fpit", "2020-01-01 09:00:00", "surf", "met"),
    ("geos_test_20200101_1200.nc", "fpit", "2020-01-01 12:00:00", "surf", "met"),
    ("geos_test_20200101_1500.nc", "fpit", "2020-01-01 15:00:00", "surf", "met"),
    ("geos_test_20200101_1800.nc", "fpit", "2020-01-01 18:00:00", "surf", "met"),
    ("geos_test_20200101_2100.nc", "fpit", "2020-01-01 21:00:00", "surf", "met"),
    ("geos_test_20200101_0000.nc", "fpit", "2020-01-01 00:00:00", "eta", "met"),
    ("geos_test_20200101_0300.nc", "fpit", "2020-01-01 03:00:00", "eta", "met"),
    ("geos_test_20200101_0600.nc", "fpit", "2020-01-01 06:00:00", "eta", "met"),
    ("geos_test_20200101_0900.nc", "fpit", "2020-01-01 09:00:00", "eta", "met"),
    ("geos_test_20200101_1200.nc", "fpit", "2020-01-01 12:00:00", "eta", "met"),
    ("geos_test_20200101_1500.nc", "fpit", "2020-01-01 15:00:00", "eta", "met"),
    ("geos_test_20200101_1800.nc", "fpit", "2020-01-01 18:00:00", "eta", "met"),
    ("geos_test_20200101_2100.nc", "fpit", "2020-01-01 21:00:00", "eta", "met"),
    ("geos_test_20200101_0000.nc", "fpit", "2020-01-01 00:00:00", "eta", "chm"),
    ("geos_test_20200101_0300.nc", "fpit", "2020-01-01 03:00:00", "eta", "chm"),
    ("geos_test_20200101_0600.nc", "fpit", "2020-01-01 06:00:00", "eta", "chm"),
    ("geos_test_20200101_0900.nc", "fpit", "2020-01-01 09:00:00", "eta", "chm"),
    ("geos_test_20200101_1200.nc", "fpit", "2020-01-01 12:00:00", "eta", "chm"),
    ("geos_test_20200101_1500.nc", "fpit", "2020-01-01 15:00:00", "eta", "chm"),
    ("geos_test_20200101_1800.nc", "fpit", "2020-01-01 18:00:00", "eta", "chm"),
    ("geos_test_20200101_2100.nc", "fpit", "2020-01-01 21:00:00", "eta", "chm");


-- Use Feb 1 as a test for missing one surf met file
INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES 
    ("geos_test_20200201_0000.nc", "fpit", "2020-02-01 00:00:00", "surf", "met"),
    ("geos_test_20200201_0300.nc", "fpit", "2020-02-01 03:00:00", "surf", "met"),
    ("geos_test_20200201_0600.nc", "fpit", "2020-02-01 06:00:00", "surf", "met"),
    ("geos_test_20200201_0900.nc", "fpit", "2020-02-01 09:00:00", "surf", "met"),
    ("geos_test_20200201_1200.nc", "fpit", "2020-02-01 12:00:00", "surf", "met"),
    ("geos_test_20200201_1500.nc", "fpit", "2020-02-01 15:00:00", "surf", "met"),
    ("geos_test_20200201_1800.nc", "fpit", "2020-02-01 18:00:00", "surf", "met"),
    ("geos_test_20200201_0000.nc", "fpit", "2020-02-01 00:00:00", "eta", "met"),
    ("geos_test_20200201_0300.nc", "fpit", "2020-02-01 03:00:00", "eta", "met"),
    ("geos_test_20200201_0600.nc", "fpit", "2020-02-01 06:00:00", "eta", "met"),
    ("geos_test_20200201_0900.nc", "fpit", "2020-02-01 09:00:00", "eta", "met"),
    ("geos_test_20200201_1200.nc", "fpit", "2020-02-01 12:00:00", "eta", "met"),
    ("geos_test_20200201_1500.nc", "fpit", "2020-02-01 15:00:00", "eta", "met"),
    ("geos_test_20200201_1800.nc", "fpit", "2020-02-01 18:00:00", "eta", "met"),
    ("geos_test_20200201_2100.nc", "fpit", "2020-02-01 21:00:00", "eta", "met"),
    ("geos_test_20200201_0000.nc", "fpit", "2020-02-01 00:00:00", "eta", "chm"),
    ("geos_test_20200201_0300.nc", "fpit", "2020-02-01 03:00:00", "eta", "chm"),
    ("geos_test_20200201_0600.nc", "fpit", "2020-02-01 06:00:00", "eta", "chm"),
    ("geos_test_20200201_0900.nc", "fpit", "2020-02-01 09:00:00", "eta", "chm"),
    ("geos_test_20200201_1200.nc", "fpit", "2020-02-01 12:00:00", "eta", "chm"),
    ("geos_test_20200201_1500.nc", "fpit", "2020-02-01 15:00:00", "eta", "chm"),
    ("geos_test_20200201_1800.nc", "fpit", "2020-02-01 18:00:00", "eta", "chm"),
    ("geos_test_20200201_2100.nc", "fpit", "2020-02-01 21:00:00", "eta", "chm");

-- Use Feb 2 for missing one eta met file
INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES 
    ("geos_test_20200202_0000.nc", "fpit", "2020-02-02 00:00:00", "surf", "met"),
    ("geos_test_20200202_0300.nc", "fpit", "2020-02-02 03:00:00", "surf", "met"),
    ("geos_test_20200202_0600.nc", "fpit", "2020-02-02 06:00:00", "surf", "met"),
    ("geos_test_20200202_0900.nc", "fpit", "2020-02-02 09:00:00", "surf", "met"),
    ("geos_test_20200202_1200.nc", "fpit", "2020-02-02 12:00:00", "surf", "met"),
    ("geos_test_20200202_1500.nc", "fpit", "2020-02-02 15:00:00", "surf", "met"),
    ("geos_test_20200202_1800.nc", "fpit", "2020-02-02 18:00:00", "surf", "met"),
    ("geos_test_20200202_2100.nc", "fpit", "2020-02-02 21:00:00", "surf", "met"),
    ("geos_test_20200202_0000.nc", "fpit", "2020-02-02 00:00:00", "eta", "met"),
    ("geos_test_20200202_0300.nc", "fpit", "2020-02-02 03:00:00", "eta", "met"),
    ("geos_test_20200202_0600.nc", "fpit", "2020-02-02 06:00:00", "eta", "met"),
    ("geos_test_20200202_0900.nc", "fpit", "2020-02-02 09:00:00", "eta", "met"),
    ("geos_test_20200202_1200.nc", "fpit", "2020-02-02 12:00:00", "eta", "met"),
    ("geos_test_20200202_1500.nc", "fpit", "2020-02-02 15:00:00", "eta", "met"),
    ("geos_test_20200202_2100.nc", "fpit", "2020-02-02 21:00:00", "eta", "met"),
    ("geos_test_20200202_0000.nc", "fpit", "2020-02-02 00:00:00", "eta", "chm"),
    ("geos_test_20200202_0300.nc", "fpit", "2020-02-02 03:00:00", "eta", "chm"),
    ("geos_test_20200202_0600.nc", "fpit", "2020-02-02 06:00:00", "eta", "chm"),
    ("geos_test_20200202_0900.nc", "fpit", "2020-02-02 09:00:00", "eta", "chm"),
    ("geos_test_20200202_1200.nc", "fpit", "2020-02-02 12:00:00", "eta", "chm"),
    ("geos_test_20200202_1500.nc", "fpit", "2020-02-02 15:00:00", "eta", "chm"),
    ("geos_test_20200202_1800.nc", "fpit", "2020-02-02 18:00:00", "eta", "chm"),
    ("geos_test_20200202_2100.nc", "fpit", "2020-02-02 21:00:00", "eta", "chm");

-- Use Feb 3 for missing one eta chem file
INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES 
    ("geos_test_20200203_0000.nc", "fpit", "2020-02-03 00:00:00", "surf", "met"),
    ("geos_test_20200203_0300.nc", "fpit", "2020-02-03 03:00:00", "surf", "met"),
    ("geos_test_20200203_0600.nc", "fpit", "2020-02-03 06:00:00", "surf", "met"),
    ("geos_test_20200203_0900.nc", "fpit", "2020-02-03 09:00:00", "surf", "met"),
    ("geos_test_20200203_1200.nc", "fpit", "2020-02-03 12:00:00", "surf", "met"),
    ("geos_test_20200203_1500.nc", "fpit", "2020-02-03 15:00:00", "surf", "met"),
    ("geos_test_20200203_1800.nc", "fpit", "2020-02-03 18:00:00", "surf", "met"),
    ("geos_test_20200203_2100.nc", "fpit", "2020-02-03 21:00:00", "surf", "met"),
    ("geos_test_20200203_0000.nc", "fpit", "2020-02-03 00:00:00", "eta", "met"),
    ("geos_test_20200203_0300.nc", "fpit", "2020-02-03 03:00:00", "eta", "met"),
    ("geos_test_20200203_0600.nc", "fpit", "2020-02-03 06:00:00", "eta", "met"),
    ("geos_test_20200203_0900.nc", "fpit", "2020-02-03 09:00:00", "eta", "met"),
    ("geos_test_20200203_1200.nc", "fpit", "2020-02-03 12:00:00", "eta", "met"),
    ("geos_test_20200203_1500.nc", "fpit", "2020-02-03 15:00:00", "eta", "met"),
    ("geos_test_20200203_1800.nc", "fpit", "2020-02-03 18:00:00", "eta", "met"),
    ("geos_test_20200203_2100.nc", "fpit", "2020-02-03 21:00:00", "eta", "met"),
    ("geos_test_20200203_0300.nc", "fpit", "2020-02-03 03:00:00", "eta", "chm"),
    ("geos_test_20200203_0600.nc", "fpit", "2020-02-03 06:00:00", "eta", "chm"),
    ("geos_test_20200203_0900.nc", "fpit", "2020-02-03 09:00:00", "eta", "chm"),
    ("geos_test_20200203_1200.nc", "fpit", "2020-02-03 12:00:00", "eta", "chm"),
    ("geos_test_20200203_1500.nc", "fpit", "2020-02-03 15:00:00", "eta", "chm"),
    ("geos_test_20200203_1800.nc", "fpit", "2020-02-03 18:00:00", "eta", "chm"),
    ("geos_test_20200203_2100.nc", "fpit", "2020-02-03 21:00:00", "eta", "chm");

-- Use Mar 1 for missing all surf met files
INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES 
    ("geos_test_20200301_0000.nc", "fpit", "2020-03-01 00:00:00", "eta", "met"),
    ("geos_test_20200301_0300.nc", "fpit", "2020-03-01 03:00:00", "eta", "met"),
    ("geos_test_20200301_0600.nc", "fpit", "2020-03-01 06:00:00", "eta", "met"),
    ("geos_test_20200301_0900.nc", "fpit", "2020-03-01 09:00:00", "eta", "met"),
    ("geos_test_20200301_1200.nc", "fpit", "2020-03-01 12:00:00", "eta", "met"),
    ("geos_test_20200301_1500.nc", "fpit", "2020-03-01 15:00:00", "eta", "met"),
    ("geos_test_20200301_1800.nc", "fpit", "2020-03-01 18:00:00", "eta", "met"),
    ("geos_test_20200301_2100.nc", "fpit", "2020-03-01 21:00:00", "eta", "met"),
    ("geos_test_20200301_0000.nc", "fpit", "2020-03-01 00:00:00", "eta", "chm"),
    ("geos_test_20200301_0300.nc", "fpit", "2020-03-01 03:00:00", "eta", "chm"),
    ("geos_test_20200301_0600.nc", "fpit", "2020-03-01 06:00:00", "eta", "chm"),
    ("geos_test_20200301_0900.nc", "fpit", "2020-03-01 09:00:00", "eta", "chm"),
    ("geos_test_20200301_1200.nc", "fpit", "2020-03-01 12:00:00", "eta", "chm"),
    ("geos_test_20200301_1500.nc", "fpit", "2020-03-01 15:00:00", "eta", "chm"),
    ("geos_test_20200301_1800.nc", "fpit", "2020-03-01 18:00:00", "eta", "chm"),
    ("geos_test_20200301_2100.nc", "fpit", "2020-03-01 21:00:00", "eta", "chm");

-- Use Mar 2 for missing all eta met files
INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES 
    ("geos_test_20200302_0000.nc", "fpit", "2020-03-02 00:00:00", "surf", "met"),
    ("geos_test_20200302_0300.nc", "fpit", "2020-03-02 03:00:00", "surf", "met"),
    ("geos_test_20200302_0600.nc", "fpit", "2020-03-02 06:00:00", "surf", "met"),
    ("geos_test_20200302_0900.nc", "fpit", "2020-03-02 09:00:00", "surf", "met"),
    ("geos_test_20200302_1200.nc", "fpit", "2020-03-02 12:00:00", "surf", "met"),
    ("geos_test_20200302_1500.nc", "fpit", "2020-03-02 15:00:00", "surf", "met"),
    ("geos_test_20200302_1800.nc", "fpit", "2020-03-02 18:00:00", "surf", "met"),
    ("geos_test_20200302_2100.nc", "fpit", "2020-03-02 21:00:00", "surf", "met"),
    ("geos_test_20200302_0000.nc", "fpit", "2020-03-02 00:00:00", "eta", "chm"),
    ("geos_test_20200302_0300.nc", "fpit", "2020-03-02 03:00:00", "eta", "chm"),
    ("geos_test_20200302_0600.nc", "fpit", "2020-03-02 06:00:00", "eta", "chm"),
    ("geos_test_20200302_0900.nc", "fpit", "2020-03-02 09:00:00", "eta", "chm"),
    ("geos_test_20200302_1200.nc", "fpit", "2020-03-02 12:00:00", "eta", "chm"),
    ("geos_test_20200302_1500.nc", "fpit", "2020-03-02 15:00:00", "eta", "chm"),
    ("geos_test_20200302_1800.nc", "fpit", "2020-03-02 18:00:00", "eta", "chm"),
    ("geos_test_20200302_2100.nc", "fpit", "2020-03-02 21:00:00", "eta", "chm");


-- Use Mar 3 for missing all eta chem files
INSERT INTO GeosFiles (file_path, product, filedate, levels, data_type)
VALUES 
    ("geos_test_20200303_0000.nc", "fpit", "2020-03-03 00:00:00", "surf", "met"),
    ("geos_test_20200303_0300.nc", "fpit", "2020-03-03 03:00:00", "surf", "met"),
    ("geos_test_20200303_0600.nc", "fpit", "2020-03-03 06:00:00", "surf", "met"),
    ("geos_test_20200303_0900.nc", "fpit", "2020-03-03 09:00:00", "surf", "met"),
    ("geos_test_20200303_1200.nc", "fpit", "2020-03-03 12:00:00", "surf", "met"),
    ("geos_test_20200303_1500.nc", "fpit", "2020-03-03 15:00:00", "surf", "met"),
    ("geos_test_20200303_1800.nc", "fpit", "2020-03-03 18:00:00", "surf", "met"),
    ("geos_test_20200303_2100.nc", "fpit", "2020-03-03 21:00:00", "surf", "met"),
    ("geos_test_20200303_0000.nc", "fpit", "2020-03-03 00:00:00", "eta", "met"),
    ("geos_test_20200303_0300.nc", "fpit", "2020-03-03 03:00:00", "eta", "met"),
    ("geos_test_20200303_0600.nc", "fpit", "2020-03-03 06:00:00", "eta", "met"),
    ("geos_test_20200303_0900.nc", "fpit", "2020-03-03 09:00:00", "eta", "met"),
    ("geos_test_20200303_1200.nc", "fpit", "2020-03-03 12:00:00", "eta", "met"),
    ("geos_test_20200303_1500.nc", "fpit", "2020-03-03 15:00:00", "eta", "met"),
    ("geos_test_20200303_1800.nc", "fpit", "2020-03-03 18:00:00", "eta", "met"),
    ("geos_test_20200303_2100.nc", "fpit", "2020-03-03 21:00:00", "eta", "met");