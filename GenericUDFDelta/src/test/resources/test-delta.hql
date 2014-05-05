ADD JAR GenericUDFDelta-1.0.jar;
CREATE TEMPORARY FUNCTION delta as 'nz.net.nzrs.hive.udf.GenericUDFDelta';

SELECT cat1, cat2, delta(HASH(cat1, cat2), random_value) as diff FROM (
    SELECT cat1, cat2, random_value FROM genericUDFDelta_test_data
    DISTRIBUTE BY HASH(cat1, cat2)
    SORT BY cat1, cat2, random_value ) t0
ORDER BY cat1, cat2;
