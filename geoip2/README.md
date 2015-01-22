nzrs-hive-udf/geoip2
====================

GeoIP Functions for hive

    ADD JAR geoip2-1.0-SNAPSHOT-jar-with-dependencies.jar;
    ADD FILE GeoLite2-City.mmdb;
    create temporary function geoip as 'nz.net.nzrs.hive.udf.GeoIPLookupUDF';
    select geoip(addr, 'COUNTRY_CODE',  './GeoLite2-City.mmdb' ) from table;

You can either use a licensed version of the GeoIP2 database, or the GeoLite version
