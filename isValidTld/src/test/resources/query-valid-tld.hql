ADD FILE tlds-alpha-by-domain.txt;
ADD JAR isValidTld-1.0-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION is_valid_tld as 'nz.net.nzrs.hive.udf.isValidTld';

SELECT qname FROM tmp_valid_tld_data
    WHERE is_valid_tld(tld, './tlds-alpha-by-domain.txt');

SELECT tld, count(*) FROM (
    SELECT if(is_valid_tld(tld, './tlds-alpha-by-domain.txt'), tld,
    "INVALID") as tld FROM tmp_valid_tld_data
) t1
GROUP BY tld
;
