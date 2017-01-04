Setup instructions for cassandra-dtest
======================================

These are instructions for setting up dtests on a fresh install of Ubuntu Linux 12.04 LTS. If you use something else, you'll need to adapt these for your particular situation (or better yet, append to this file with your platform's requirements and send a pull request.)

## Prerequisite Software:
* Update software repositories:

        sudo apt-get update

* python

        sudo apt-get install python python-setuptools python-dev python-pip

* git

        sudo apt-get install git

* windows

        python: https://www.python.org/downloads/
        git:https://msysgit.github.io/
        gnuwin32: http://gnuwin32.sourceforge.net/
        apache ant: https://ant.apache.org/bindownload.cgi

## Install Oracle Java 8:
* java and misc tools:

        sudo apt-get install software-properties-common
        sudo add-apt-repository ppa:webupd8team/java
        sudo apt-get update
        sudo apt-get install oracle-java8-installer

        Windows: http://www.oracle.com/technetwork/java/javase/downloads/index.html

* Ensure that java is a HotSpot 1.8.x version:

        # java -version
        java version "1.8.0_73"
        Java(TM) SE Runtime Environment (build 1.8.0_73)
        Java HotSpot(TM) 64-Bit Server VM (build 24.0-b56, mixed mode)

* install ant

        sudo apt-get install ant

## Create a git directory for holding several projects we'll use:

        mkdir -p ~/git/cstar

## Install companion tools / libraries:
It's best to download the git source tree for these libraries as you
will often need to modify them in some fashion at some later point:

* ccm:

        cd ~/git/cstar
        git clone git://github.com/pcmanus/ccm.git
        sudo apt-get install libyaml-dev
        sudo pip install -e ccm
        sudo pip install pyyaml

* python-driver

        cd ~/git/cstar
        Cassandra 2.x:
        sudo pip install cassandra-driver
        Cassandra 3.x (requires latest python-driver):
        sudo pip install git+git://github.com/datastax/python-driver@cassandra-test  # install dedicated test branch for new Cassandra features
        sudo pip install --pre cassandra-driver  # fallback driver for new features
        For more instructions on how to install the python-driver,
        see http://datastax.github.io/python-driver/installation.html

* cql

        sudo pip install cql

* cassandra-dtest

        cd ~/git/cstar
        git clone git://github.com/riptano/cassandra-dtest.git

* nose

        sudo apt-get install python-nose

* flaky

		sudo pip install flaky

* cassandra

        cd ~/git/cstar
        git clone http://git-wip-us.apache.org/repos/asf/cassandra.git
        cd cassandra
        ant clean jar

 Optionally, you can self-check cassandra at this point by running
 it's unit tests:

        ant test

 Note: you may need to install ant-optional to get junit working:

        sudo apt-get install ant-optional

## Setup and run dtests
* Install current python dependencies:

        sudo pip install decorator

* Set CASSANDRA_DIR environment variable.
  Set the variable in your ~/.bashrc file once so that you don't have to keep setting it everytime you run dtests:

        export CASSANDRA_DIR=~/git/cstar/cassandra

* Run the full dtest suite (takes multiple hours, depending on your hardware):

         cd ~/git/cstar/cassandra-dtest
         nosetests

* Run the full dtest suite, retrying tests decorated with `flaky` (see [the `flaky` plugin](https://github.com/box/flaky) for more documentation):

         cd ~/git/cstar/cassandra-dtest
         nosetests --with-flaky

* Run a single dtest, printing debug info, stopping at the first error encountered (if any):

         cd ~/git/cstar/cassandra-dtest
         PRINT_DEBUG=true nosetests -x -s -v putget_test.py

* To reuse cassandra clusters when possible, set the environment variable REUSE_CLUSTER

        REUSE_CLUSTER=true nosetests -s -v cql_tests.py

* Some tests will not run with vnodes enabled (you'll see a "SKIP: Test disabled for vnodes" message in that case). Use the provided runner script instead:

        ./run_dtests.py --vnodes false --nose-options "-x -s -v" topology_test.py:TestTopology.movement_test
