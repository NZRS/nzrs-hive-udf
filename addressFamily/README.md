# nzrs-hive-udf/addressFamily

Provided with a string, it will try to parse it as IPv4 first, and in
case of failure, as IPv6. It will return 4 (as integer) if the address
is IPv4, 6 if it is IPv6 or 0 in case of error.

Depends on Apache Commons Validator for the IPv4 stuff, and Java IPv6
for the IPv6 stuff. Also requires SnakeYAML for testing

## USAGE

```
hive> ADD JAR addressFamily-0.1-jar-with-dependencies.jar;
Added addressFamily-0.1-jar-with-dependencies.jar to class path
Added resource: addressFamily-0.1-jar-with-dependencies.jar
hive> CREATE TEMPORARY FUNCTION addr_fam as 'nz.net.nzrs.hive.udf.addressFamily';
OK
Time taken: 0.04 seconds

CREATE EXTERNAL TABLE addressFamily_test_data ( address string );
-- Import some data
hive> select * from addressFamily_test_data;
OK
127.0.0.1
192.168.1.1
192.168..1.1
10.20.30.40.50
www.google.com
fe80::221:ccff:fe4a:ab36
2001:db8:0:0:0::1
::1
FF01:0:0:0:0:0:0:101

select addr_fam(address), address from addressFamily_test_data;
4   127.0.0.1
4   192.168.1.1
0   192.168..1.1
0   10.20.30.40.50
0   www.google.com
6   fe80::221:ccff:fe4a:ab36
6   2001:db8:0:0:0::1
6   ::1
6   FF01:0:0:0:0:0:0:101
```
