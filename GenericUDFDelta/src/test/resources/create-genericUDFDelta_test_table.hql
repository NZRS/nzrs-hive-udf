DROP TABLE IF EXISTS genericUDFDelta_test_data;
CREATE EXTERNAL TABLE genericUDFDelta_test_data (
  cat1 string,
  cat2 string,
  random_value float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/genericudfdelta_test';
