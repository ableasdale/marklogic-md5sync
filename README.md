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