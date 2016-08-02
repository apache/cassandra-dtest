## Running Upgrade Tests

#### Against a local version of Cassandra:
- export CASSANDRA_DIR=/your/cassandra/location
- export LOCAL_GIT_REPO=/your/cassandra/location/
- run nosetests:
> nosetests -vs upgrade_tests/
- to preview tests names, use:
> nosetests --collect-only upgrade_tests/
