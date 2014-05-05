DROP TABLE IF EXISTS tmp_prefix_match_data;
CREATE EXTERNAL TABLE tmp_prefix_match_data (
    address string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/test_prefix_match_data';
