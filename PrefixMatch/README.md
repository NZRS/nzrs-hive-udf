# nzrs-hive-udf/PrefixMatch

## Description

Hive UDF to check if a given IP address (provided as a string) matches
the network prefix given (in format network/mask). This works for both
IPv4 and IPv6.

## How to build

```
mvn package
```

will generate a single jar file that can be used within a Hadoop
cluster.

## Usage (for busy people)
```
ADD JAR PrefixMatch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION prefix_match as 'nz.net.nzrs.hive.udf.PrefixMatch';

SELECT address FROM tmp_prefix_match_data WHERE
    prefix_match(address,'172.16.0.0/12');
SELECT address FROM tmp_prefix_match_data WHERE
    prefix_match(address, 'fe80::/16');
```

## Usage (complete example)
For convenience, some test scripts and data is provided to test.

1. Create a temporary table for the data
```
hive -f src/test/resources/create-prefixmatch-test-data.hql
```
2. Load the data to the table created in the previous table
```
hdfs dfs -copyFromLocal src/test/resources/prefixmatch-data.txt /tmp/test_prefix_match_data
```
3. Run some of the sample queries
```
hive -f src/test/resources/queries-prefix-match.hql
```
