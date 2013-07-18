This Hive UDF provides the functionality of returning components (called
labels) from a string separated by dots (like a domain name or an IPv4
address in text representation).

I used this in conjunction with the Hadoop PCAP serde, that provides
access to PCAP files in Hive.

It was written to avoid using regular expression when extracting labels
of a domain name.

To use:

1) Run 'mvn compile'
2) Run 'mvn package', this will generate a JAR file located under
target/getLabel-0.1.jar
3) Copy target/getLabel-0.1.jar to your Hadoop installation
4) On your .hiverc file add

    ADD JAR <JAR_DIR>/getLabel-0.1.jar
    CREATE TEMPORARY FUNCTION get_label as 'nz.net.nzrs.hive.udf.getLabel';

5) In Hive, you now can run

    SELECT get_label(dns_qname, "0,1") FROM pcap;

    if dns_qname contains "nzrs.net.nz" it will return "net.nz"


Sebastian Castro
sebastian@nzrs.net.nz

