Cassandra Distributed Tests
===========================

Tests for [Apache Cassandra](http://apache.cassandra.org) clusters.

Prerequisites
------------

An up to date copy of ccm should be installed for starting and stopping Cassandra.
The tests are run using nosetests.
These tests require the datastax python driver.
A few tests still require the deprecated python CQL over thrift driver.

 * [ccm](https://github.com/pcmanus/ccm)
 * [nosetests](http://readthedocs.org/docs/nose/en/latest/)
 * [Python Driver](http://datastax.github.io/python-driver/installation.html)
 * [CQL over Thrift Driver](http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/)

Usage
-----

The tests are run by nosetests. The only thing the framework needs to know is
the location of the (compiled) sources for Cassandra. There are two options:

Use existing sources:

    CASSANDRA_DIR=~/path/to/cassandra nosetests

Use ccm ability to download/compile released sources from archives.apache.org:

    CASSANDRA_VERSION=1.0.0 nosetests

A convenient option if tests are regularly run against the same existing
directory is to set a `default_dir` in `~/.cassandra-dtest`. Create the file and
set it to something like:

    [main]
    default_dir=~/path/to/cassandra

The tests will use this directory by default, avoiding the need for any
environment variable (that still will have precedence if given though).

Existing tests are probably the best place to start to look at how to write
tests.

Each test spawns a new fresh cluster and tears it down after the test, unless
`REUSE_CLUSTER` is set to true. Then some tests will share cassandra instances. If a
test fails, the logs for the node are saved in a `logs/<timestamp>` directory
for analysis (it's not perfect but has been good enough so far, I'm open to
better suggestions).

To run the upgrade tests, you have must both JDK7 and JDK8 installed. Paths
to these installations should be defined in the environment variables
JAVA7_HOME and JAVA8_HOME, respectively.

Installation Instructions
-------------------------

See more detailed instructions in the included [INSTALL file](https://github.com/riptano/cassandra-dtest/blob/master/INSTALL.md).

Writing Tests
-------------

- Most of the time when you start a cluster with `cluster.start()`, you'll want to pass in `wait_for_binary_proto=True` so the call blocks until the cluster is ready to accept CQL connections. We tried setting this to `True` by default once, but the problems caused there (e.g. when it waited the full timeout time on a node that was deliberately down) were more unpleasant and more difficult to debug than the problems caused by having it `False` by default.
- If you're using JMX via [the `jmxutils` module](jmxutils.py), make sure to call `remove_perf_disable_shared_mem` on the node or nodes you want to query with JMX _before starting the nodes_. `remove_perf_disable_shared_mem` disables a JVM option that's incompatible with JMX (see [this JMX ticket](https://github.com/rhuss/jolokia/issues/198)). It works by performing a string replacement in the node's Cassandra startup script, so changes will only propagate to the node at startup time.

If you'd like to know what to expect during a code review, please see the included [CONTRIBUTING file](CONTRIBUTING.md).
