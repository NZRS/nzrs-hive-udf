ADD JAR PrefixMatch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION prefix_match as 'nz.net.nzrs.hive.udf.PrefixMatch';

SELECT address FROM tmp_prefix_match_data WHERE
    prefix_match(address,'172.16.0.0/12');
SELECT address FROM tmp_prefix_match_data WHERE
    prefix_match(address, 'fe80::/16');
SELECT address FROM tmp_prefix_match_data WHERE
    prefix_match(address, 'fe80:0:0:0:200::/72');
