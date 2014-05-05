# nzrs-hive-udf/GenericUDFDelta

If you are running Hive 0.10 or less, you'll be interested on using this
function. Using a set of grouped values, it will calculate the
arithmetic different between consecutive values.

It will work for any primitive numeric type (TINYINT, SMALLINT, INT,
BIGINT, FLOAT, DOUBLE, DECIMAL)

## USAGE

A script to create a table, load the sample data and run the query has
been included.

### Create table
hive -f src/test/resources/create-genericUDFDelta_test_table.hql

### Load some data
hdfs dfs -copyFromLocal src/test/resources/delta-sample.txt /tmp/genericudfdelta_test

### Run a query (need to adjust location of JAR)
hive -f src/test/resources/test-delta.hql

If you see some 'NULL' values, it's because the key used to group values
only appeared once.

