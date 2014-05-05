DROP TABLE IF EXISTS test_simmetrics_data;
CREATE EXTERNAL TABLE test_simmetrics_data (
    text1 string,
    text2 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/test_simmetrics_data';
