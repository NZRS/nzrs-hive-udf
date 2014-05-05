# Introduction
This is a UDF that wraps some of the
[Simmetrics](http://sourceforge.net/projects/simmetrics/) functions.
There are implementations for:

JaroWinkler
MongeElkan
NeedlemanWunch

# Usage

```
ADD JAR Simmilarity-0.1.1-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION jw_dist as 'nz.net.nzrs.hive.udf.JaroWinklerDistance';
CREATE TEMPORARY FUNCTION me_dist as 'nz.net.nzrs.hive.udf.MongeElkanDistance';
CREATE TEMPORARY FUNCTION nw_dist as 'nz.net.nzrs.hive.udf.NeedlemanWunchDistance';

select text1, text2, jw_dist(text1, text2), me_dist(text1, text2),
nw_dist(text1, text2) FROM test_simmetrics_data;
```

A schema to create a test table, some random strings and a query are
available in the src/test/resources directory.
