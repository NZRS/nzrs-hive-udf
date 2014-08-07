hive-geoip
==========

GeoIP Functions for hive

    add file GeoIP.dat;
    add jar hive-udf-geo-ip-jtg.jar;
    create temporary function geoip as 'nz.net.nzrs.hive.udf.GeoIPLookupUDF';
    select geoip(addr, 'COUNTRY_NAME',  './GeoIP.dat' ) from table;

You need a geoip database, for which we have a license
