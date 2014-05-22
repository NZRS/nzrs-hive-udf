DROP TABLE IF EXISTS tmp_valid_tld_data;
CREATE EXTERNAL TABLE tmp_valid_tld_data (
    qname string,
    tld string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/valid_tld_data';
