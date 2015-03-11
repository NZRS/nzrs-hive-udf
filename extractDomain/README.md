# Introduction

extractDomain is a Hive UDF to extract the "domain name" component from
a DNS query name, using rules specific to the .nz registry.

Currently there are 16 approved second level domains, and registrations
at the second levels are accepted.

## USAGE

1. Run 'mvn compile'
2. Run 'mvn package', this will generate a JAR file located under
target/extractDomain-0.1.jar
3. Copy target/extractDomain-0.1.jar to your Hadoop installation
4. On your .hiverc file add

```
    ADD JAR <JAR_DIR>/extractDomain-0.1.jar
    CREATE TEMPORARY FUNCTION extractDomain as 'nz.net.nzrs.hive.udf.extractDomain';
```

5. In Hive, you now can run

```
    SELECT dns_qname, extractDomain(dns_qname) FROM pcap_test limit 4;

    www.terabyte.co.nz. terabyte.co.nz
    ns5.dns.net.nz. dns.net.nz
    www.wakatipu.school.nz. wakatipu.school.nz
    www.butto.nz.   butto.nz
```
