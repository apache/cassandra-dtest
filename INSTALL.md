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

## Install Oracle Java 7:
* java and misc tools:

        sudo apt-get install software-properties-common
        sudo add-apt-repository ppa:webupd8team/java
        sudo apt-get update
        sudo apt-get install oracle-java7-installer

* Ensure that java is a HotSpot 1.7.x version:

        # java -version
        java version "1.7.0_40"
        Java(TM) SE Runtime Environment (build 1.7.0_40-b43)
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
        sudo pip install cassandra-driver
        For more instructions on how to install the python-driver,
        see http://datastax.github.io/python-driver/installation.html

* cql

        cd ~/git/cstar
        git clone https://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/
        sudo pip install -e cassandra-dbapi2
        This is needed for a few legacy tests, and for cql over thrift

* cassandra-dtest

        cd ~/git/cstar
        git clone git://github.com/riptano/cassandra-dtest.git

* nose

        sudo apt-get install python-nose

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

* Run a single dtest, printing debug info, stopping at the first error encountered (if any):

         cd ~/git/cstar/cassandra-dtest
         PRINT_DEBUG=true nosetests -x -s -v putget_test.py

* To reuse cassandra clusters when possible, set the environment variable REUSE_CLUSTER

        REUSE_CLUSTER=true nosetests -s -v cql_tests.py