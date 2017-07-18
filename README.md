# marklogic-md5sync

Synchronise content in two MarkLogic Databases according to document MD5 hashes on both the master and target systems

## Quick Start

1. Configure the properties file (`/src/main/resources/config.properties`):
```bash
source.uri = xcc://username:password@source-hostname:PORT/DATABASE-NAME
target.uri = xcc://username:password@target-hostname:PORT/DATABASE-NAME
```

2. Customise the cts:uris query to target the subset of the data that you want to synchronise or leave as-is if you want to check the entire contents of the database:

```xquery
for $URI in cts:uris( (), ('limit=5000') )
```

3. To run the application from the commandline:
```bash
./gradlew run
```

## Logging

After the run has completed, you should see a file called `md5sync.log` that will provide a full record of all changes and MD5 checksums on both source and target databases:

```
Last URI in batch of 4999 URI(s): /18298134893951942727.xml
Last URI in batch of 5000 URI(s): /9998482284032762447.xml
Last URI in batch of 3 URI(s): /9999735505635108791.xml
Source and target number of documents match:    (Source: 10000)         (Target: 10000)
Generating report
URI:    /2866957652531112369.xml        Source MD5:     799d77a32c72cd1bcd3bb42ac92a28d8        Target MD5:     799d77a32c72cd1bcd3bb42ac92a28d8
URI:    /13120278568998147220.xml       Source MD5:     f599e07c8dbdb73721734bd8f1b40cae        Target MD5:     f599e07c8dbdb73721734bd8f1b40cae
URI:    /17166464939234192116.xml       Source MD5:     c0ceafe32cdc81dff838a7a08a138264        Target MD5:     c0ceafe32cdc81dff838a7a08a138264
```