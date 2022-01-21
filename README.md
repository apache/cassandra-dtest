Cassandra Distributed Tests (DTests)
====================================

Cassandra Distributed Tests (or better known as "DTests") are a set of Python-based 
tests for [Apache Cassandra](http://apache.cassandra.org) clusters. DTests aim to 
test functionality that requires multiple Cassandra instances. Functionality that
of code that can be tested in isolation should ideally be a unit test (which can be
found in the actual Cassandra repository). 

Setup and Prerequisites
------------

Some environmental setup is required before you can start running DTests.

### Native Dependencies
DTests requires the following native dependencies:
 * Python 3
 * PIP for Python 3 
 * libev
 * git
 * JDK 8 (Java)
 
#### Linux
1. ``apt-get install git-core python3 python3-pip python3-dev libev4 libev-dev``
2. (Optional - solves warning: "jemalloc shared library could not be preloaded to speed up memory allocations"): 
``apt-get install -y --no-install-recommends libjemalloc1``

#### Mac
On Mac, the easiest path is to install the latest [Xcode and Command Line Utilities](https://developer.apple.com) to 
bootstrap your development environment and then use [Homebrew](https://brew.sh)

1. (Optional) Make sure brew is in a good state on your system ``brew doctor``
2. ``brew install python3 libev``

### Python Dependencies
There are multiple external Python dependencies required to run DTests. 
The current Python dependency list is maintained in a file named
[requirements.txt](https://github.com/apache/cassandra-dtest/blob/trunk/requirements.txt)
in the root of the `cassandra-dtest` repository.

The easiest way to install these dependencies is with pip and virtualenv. 

**Note**: While virtualenv isn't strictly required, using virtualenv is almost always the quickest 
path to success as it provides common base setup across various configurations.

1. Install virtualenv: ``pip install virtualenv``
2. Create a new virtualenv: ``virtualenv --python=python3 --no-site-packages ~/dtest``
3. Switch/Activate the new virtualenv: ``source ~/dtest/bin/activate``
4. Install remaining DTest Python dependencies: ``pip install -r /path/to/cassandra-dtest/requirements.txt``


Usage
-----

The tests are executed by the pytest framework which includes a helpful [Usage and
Invocations](https://docs.pytest.org/en/latest/usage.html) document which is a great place to start
for basic invocation options when using pytest.

At minimum, 

  The only thing the framework needs to know is
the location of the (compiled (hint: ``ant clean jar``)) sources for Cassandra. There are two options:

Use existing sources:

    pytest --cassandra-dir=~/path/to/cassandra

Use ccm ability to download/compile released sources from archives.apache.org:

    pytest --cassandra-version=1.0.0

To run a specific test file, class or individual test, you only have to 
pass its path as an argument:

    pytest --cassandra-dir=~/path/to/cassandra pending_range_test.py
    pytest --cassandra-dir=~/path/to/cassandra pending_range_test.py::TestPendingRangeMovements
    pytest --cassandra-dir=~/path/to/cassandra pending_range_test.py::TestPendingRangeMovements::test_pending_range
    
When adding a new test or modifying an existing one, it's always a good idea to
run it several times to make sure it is stable. This can be easily done with 
the ``--count`` option. For example, to run a test class 10 times:

    pytest --count=10 --cassandra-dir=~/path/to/cassandra pending_range_test.py

Existing tests are probably the best place to start to look at how to write
tests.

Each test spawns a new fresh cluster and tears it down after the test. If a
test fails, the logs for the node are saved in a `logs/<timestamp>` directory
for analysis (it's not perfect but has been good enough so far, I'm open to
better suggestions).

In case you want to run tests using your own CCM branch, please, refer to the comments in requirements.txt for details how to do that.

Writing Tests
-------------

If you'd like to know what to expect during a code review, please see the included [CONTRIBUTING file](CONTRIBUTING.md).

Debugging Tests
-------------
Some general tips for debugging dtest/pytest tests

#### pytest.set_trace()
If there is an unexpected value being asserted on and you'd like to inspect the state of all the tests variables just before a paricular assert, add ``pytest.set_trace()`` right before the problematic code. The next time you execute the test, when that line of code is reached pytest will drop you into an interactive python debugger (pdb). From there you can use standard python options to inspect various methods and variables for debugging.

#### Hung tests/hung pytest framework
Debugging hung tests can be very difficult but thanks to improvements in Python 3 it's now pretty painless to get a python thread dump of all the threads currently running in the pytest process.

```python
import faulthandler
faulthandler.enable()
```
Adding the above code will install a signal handler into your process. When the process recieves a *SIGABRT* signal, python will dump python thread dumps for all running threads in the process. DTests installs this by default with the install_debugging_signal_handler fixture.

The following is an example of what you might see if you send a *SIGABRT* signal to the pytest process while in a hung state during the test teardown phase after the successful completion of the actual dtest.

```bash
(env) cassandra-dtest vcooluser$ kill -SIGABRT 24142

Fatal Python error: Aborted

Thread 0x000070000f739000 (most recent call first):
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 295 in wait
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 551 in wait
  File "/Users/mkjellman/src/cassandra-dtest/tools/data.py", line 31 in query_c1c2
  File "/Users/mkjellman/src/cassandra-dtest/bootstrap_test.py", line 91 in <lambda>
  File "/Users/mkjellman/src/cassandra-dtest/dtest.py", line 245 in run
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 916 in _bootstrap_inner
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 884 in _bootstrap

Thread 0x000070000e32d000 (most recent call first):
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/asyncore.py", line 183 in poll2
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/asyncore.py", line 207 in loop
  File "/Users/mkjellman/env3/src/cassandra-driver/cassandra/io/asyncorereactor.py", line 119 in loop
  File "/Users/mkjellman/env3/src/cassandra-driver/cassandra/io/asyncorereactor.py", line 258 in _run_loop
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 864 in run
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 916 in _bootstrap_inner
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 884 in _bootstrap

Current thread 0x00007fffa00dd340 (most recent call first):
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 1072 in _wait_for_tstate_lock
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 1056 in join
  File "/Users/mkjellman/src/cassandra-dtest/dtest.py", line 253 in stop
  File "/Users/mkjellman/src/cassandra-dtest/dtest.py", line 580 in tearDown
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/unittest/case.py", line 608 in run
  File "/usr/local/Cellar/python3/3.6.3/Frameworks/Python.framework/Versions/3.6/lib/python3.6/unittest/case.py", line 653 in __call__
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/unittest.py", line 174 in runtest
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/runner.py", line 107 in pytest_runtest_call
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/callers.py", line 180 in _multicall
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 216 in <lambda>
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 222 in _hookexec
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 617 in __call__
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/flaky/flaky_pytest_plugin.py", line 273 in <lambda>
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/runner.py", line 191 in __init__
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/flaky/flaky_pytest_plugin.py", line 274 in call_runtest_hook
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/flaky/flaky_pytest_plugin.py", line 118 in call_and_report
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/runner.py", line 77 in runtestprotocol
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/runner.py", line 63 in pytest_runtest_protocol
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/flaky/flaky_pytest_plugin.py", line 81 in pytest_runtest_protocol
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/callers.py", line 180 in _multicall
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 216 in <lambda>
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 222 in _hookexec
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 617 in __call__
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/main.py", line 164 in pytest_runtestloop
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/callers.py", line 180 in _multicall
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 216 in <lambda>
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 222 in _hookexec
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 617 in __call__
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/main.py", line 141 in _main
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/main.py", line 103 in wrap_session
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/main.py", line 134 in pytest_cmdline_main
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/callers.py", line 180 in _multicall
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 216 in <lambda>
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 222 in _hookexec
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/pluggy/__init__.py", line 617 in __call__
  File "/Users/mkjellman/env3/lib/python3.6/site-packages/_pytest/config.py", line 59 in main
  File "/Users/mkjellman/env3/bin/pytest", line 11 in <module>
Abort trap: 6
```

#### Debugging Issues with Fixtures and Test Setup/Teardown
pytest can appear to be doing "magic" more often than not. One place it may be hard to follow what actual code will get executed by normal code inspection alone is determining which fixtures will run for a given test and in what order. pytest provides a ``--setup-plan`` command line argument. When pytest is invoked with this argument it will print a execution plan including all fixtures and tests that actually running the test will invoke. The below is an example for the current execution plan pytest generates for dtest *auth_test.py::TestAuthRoles::test_create_drop_role*

```bash
(env3) Michaels-MacBook-Pro:cassandra-dtest mkjellman$ pytest --cassandra-dir=/Users/mkjellman/src/mkjellman-oss-github-cassandra-trunk auth_test.py::TestAuthRoles::test_create_drop_role --setup-plan
====================================================================== test session starts ======================================================================
platform darwin -- Python 3.6.3, pytest-3.3.0, py-1.5.2, pluggy-0.6.0
rootdir: /Users/mkjellman/src/cassandra-dtest, inifile: pytest.ini
plugins: timeout-1.2.1, raisesregexp-2.1, nose2pytest-1.0.8, flaky-3.4.0
collected 1 item                                                                                                                                                

auth_test.py 
SETUP    S install_debugging_signal_handler
    SETUP    C fixture_logging_setup
      SETUP    F fixture_dtest_setup_overrides
      SETUP    F fixture_log_test_name_and_date
      SETUP    F fixture_maybe_skip_tests_requiring_novnodes
      SETUP    F parse_dtest_config
      SETUP    F fixture_dtest_setup (fixtures used: fixture_dtest_setup_overrides, fixture_logging_setup, parse_dtest_config)
      SETUP    F fixture_since (fixtures used: fixture_dtest_setup)
      SETUP    F fixture_dtest_config (fixtures used: fixture_logging_setup)
      SETUP    F set_dtest_setup_on_function (fixtures used: fixture_dtest_config, fixture_dtest_setup)
        auth_test.py::TestAuthRoles::()::test_create_drop_role (fixtures used: fixture_dtest_config, fixture_dtest_setup, fixture_dtest_setup_overrides, fixture_log_test_name_and_date, fixture_logging_setup, fixture_maybe_skip_tests_requiring_novnodes, fixture_since, install_debugging_signal_handler, parse_dtest_config, set_dtest_setup_on_function)
      TEARDOWN F set_dtest_setup_on_function
      TEARDOWN F fixture_dtest_config
      TEARDOWN F fixture_since
      TEARDOWN F fixture_dtest_setup
      TEARDOWN F parse_dtest_config
      TEARDOWN F fixture_maybe_skip_tests_requiring_novnodes
      TEARDOWN F fixture_log_test_name_and_date
      TEARDOWN F fixture_dtest_setup_overrides
    TEARDOWN C fixture_logging_setup
TEARDOWN S install_debugging_signal_handler
===Flaky Test Report===


===End Flaky Test Report===

====================================================================== 0 tests deselected =======================================================================
================================================================= no tests ran in 0.12 seconds ==================================================================
```

#### Instances Failing to Start (Unclean Test Teardown)
Getting into a state (especially while writing new tests or debugging problamatic ones) where pytest/dtest fails to fully tear-down all local C* instancse that were started. You can use this handy one liner to kill all C* instances in one go:

```bash
ps aux | grep -ie CassandraDaemon | grep java | awk '{print $2}' | xargs kill
```

Links
-------
 * [ccm](https://github.com/pcmanus/ccm)
 * [pytest](https://docs.pytest.org/)
 * [Python Driver](http://datastax.github.io/python-driver/installation.html)
 * [CQL over Thrift Driver](http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/)
