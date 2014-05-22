# nzrs-hive-udf/isValidTld

## Description

Hive UDF to check if a given string (presumably representing a TLD) is
part of the list of validly recognized TLDs by IANA. If the TLD is part
of the list, the function returns true, false otherwise.

## How to build
```
mvn package
```

will generate a single jar file that can be used within a Hadoop cluster

## Usage (for busy people)
```
wget http://data.iana.org/TLD/tlds-alpha-by-domain.txt
hive
hive> ADD FILE tlds-alpha-by-domain.txt;
hive> ADD JAR isValidTld-1.0-SNAPSHOT.jar;
hive> CREATE TEMPORARY FUNCTION is_valid_tld as 'nz.net.nzrs.hive.udf.isValidTld';
hive> SELECT count(*) FROM dns_queries WHERE is_valid_tld(tld, './tlds-alpha-by-domain.txt');

## Usage (complete example)
For convenience, test script and data are provided to test.

1. Create a temporary table for data
```
hive -f src/test/resources/create-validtld-test-data.hql
```
2. Load data into table created in the previous step
```
hdfs dfs -copyFromLocal src/test/resources/valid-tld-data.txt /tmp/valid_tld_data
```
3. Run sample query
```
hive -f src/test/resources/query-valid-tld.hql
```
