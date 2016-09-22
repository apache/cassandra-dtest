# Running Upgrade Tests

#### Against a local version of Cassandra:
These instructions will get upgrade tests running locally, upgrading to your local source code.
Tests which are not relevant to your local cassandra version (in CASSANDRA_DIR) will be automatically skipped.
This procedure closely mirrors how CI jobs are configured to run upgrade tests.

- export CASSANDRA_DIR=/your/cassandra/location
- export LOCAL_GIT_REPO=/your/cassandra/location/
- run nosetests:
> nosetests -vs upgrade_tests/
- to preview tests names, use:
> nosetests -v --collect-only upgrade_tests/

Note: Only define the LOCAL_GIT_REPO env var if you are testing upgrade to a _single_ local version. For more complicated cases, such as upgrading using multiple local versions, leave this unset and read the section on the upgrade manifest below.

#### Customizing the upgrade path
In most cases the above instructions are what you probably need to do. However, in some instances you may need to further customize the upgrade paths being used, or point to non-local code. This simple [example pr](https://github.com/riptano/cassandra-dtest/pull/1282) demonstrates the basic procedure for building custom upgrade paths; these paths will supercede the normal upgrade tests when run in this fashion.

# How the tests work

### High level

The tests in this submodule are designed to have a single implementation,
which is then automatically run across a variety of upgrade paths. This means
we can implement a single test and get "free" testing of it on all upgrade paths
which should be supported. This gives us testing with a pattern of "write once, run many"
(i.e. write one test and reuse it several times on different configurations).

### The manifest

[upgrade_manifest.py](upgrade_manifest.py) contains metadata and tools concerned with
which upgrade paths to test, and where to find their source. This centralized
location is updated when we need to make changes to which version paths are
upgradeable, and how to find those versions (e.g. via a git tag). This file also
holds useful metadata about particular versions, such as which binary protocol versions
they support, and which java versions they can run on.

There are two important concepts in the manifest. The first is known as a
__variant__. There are two types of variants, 'indev' and 'current'. 'Indev' stands
for 'in development' which just means a version being worked on, such as an actively
developed branch which will eventually become a release. 'Current' stands
for 'current release', which just means the most recently released version. For example,
the manifest might have information about testing an actively developed branch
such as cassandra-3.9. This would have a variant value of 'indev' since it is
referring to a version being actively developed. The most common case is that
testing begins on released versions ('current') and ends on an version being actively
developed ('indev'). This helps ensure that users picking up released versions will
be able to upgrade to the code actively being developed should it become the next
release. If a bug should be discovered then the indev version can be patched since
it is not yet released code.

The second useful concept is that of a __version family__. This simply describes which
'version line' a particular version belongs to. For example, Cassandra 3.0.7 belongs
to the '3.0.x' version family. Organizing specific versions into families allows
us to generalize about support, so we can say, for example that: "2.2.x versions should be
able to upgrade to 3.0.x versions, or 2.2.x versions can skip 3.0.x versions and upgrade directly to a 3.x version".

### Code generation

As described above, the upgrade testing tools allow a "write once, run many"
pattern, which is to say one test can be written and then reused over a number
of different upgrade paths and configurations. This is made possible through code generation.

For example, a hypothetical test called 'test_authentication_during_upgrade',
would be implemented as a method in an existing class. At runtime we
effectively treat that class as a template, and generate subclasses of it to cover
upgrade paths and configurations of interest. So that singular test once written
can effectively create many tests.

This system greatly reduces the amount of custom code and lowers maintenance debt
significantly as compared to the alternative (the alternative being lots of similar code duplicated many times to test each desired case).

The downside of code generation is that a single Cassandra bug or programming
error can potentially manifest in a noisy fashion with many duplicated error reports.
The good news is, of course, the noise can usually all be fixed with a single
bug fix or test code adjustment.

### Manifest revisited

The manifest and code generation are used together to create the test cases
we are interested in.

First, we query the manifest to determine which upgrade pairs to test. This is done
by calling the __build_upgrade_pairs__ method, which returns a list of upgrade
'pairs'. Each pair represents a starting version (typically a released version, or
in upgrade nomenclature a variant value of 'current'), and a ending version (typically
an unreleased version, or in upgrade nomenclature a variant value of 'indev').

Finally, with each upgrade pair known, we then generate a class to test that upgrade.

### Impact of test runtime environment

Testing practices likely have a separate continuous integration job for
each version of software being tested. So, naturally when we run a test job on 3.0.x,
we should skip any upgrade test cases which aren't relevant to 3.0.x.

To get the correct test cases running, we inspect the test environment to infer
which version family is under test. We then generate _every_ possible upgrade test
case, but skip the non-relevant cases.

When inspecting the test environment to determine which test cases are relevant, the
code also grabs the current git sha, and that particular sha is used as the version
to upgrade to in each relevant case.

(Prior to this system of 'pinning' to the current env's git sha, the tests instead would upgrade to some static code ref like 'cassandra-3.0', which usually differed from the actual
code under test. This old way of testing static versions made interpreting results
confusing.)

# Building a new test

The upgrade testing toolkit makes for relatively painless addition of tests in
existing classes. The basic procedure is to find the correct class, and create a
new method there. For example, to test a specific cql behavior, edit the
[cql_tests.py](cql_tests.py) module and add a new method to the TestCQL class. The
example below assumes the method is being added to a class which extends
[upgrade_base.py](upgrade_base.py):UpgradeTester (this isn't strictly required; one could also choose
to implement their own class and helpers):

    def test_some_new_feature(self):
        cursor = self.prepare()
        # set up some data
        # add some assertions to check state
        # enumerate nodes returning an exclusive connection to each one
        # only the first node is upgraded, the second is left on the beginning version
        for is_upgraded, cursor in self.do_upgrade(cursor):
            # test the system by
            # querying the sessions and make assertions on each one

To preview the test cases, be sure your environment is set up according to the
instructions at the beginning of this document. In your CASSANDRA_DIR check out
the code you are interested in testing. With the env set up and the right
code checked out, you can then preview the created test cases:

    nosetests -v --collect-only upgrade_tests/cql_tests.py

Though that can be very verbose! To reduce the output a bit, you can use something like:

    nosetests -v --collect-only upgrade_tests/cql_tests.py |& grep 'some_test_name'

# Adding a new test module

This procedure is very similar to adding a single test, but requires a little bit more work.
In the above example, the [cql_tests.py](cql_tests.py) module has already been built to do the necessary code generation covering the various upgrade paths, and that module is a good reference for seeing how the code generation works (see the end of the file).

The basic module creation procedure:

- Add the new module file.
- Add one or many classes to the module file, most typically extending [upgrade_base.py](upgrade_base.py):UpgradeTester
- If you don't extend UpgradeTester, set ```__test__ = False```. This will prevent
nosetests from directly running the base class as a test, since the class is intended
to act as a template and not be directly executed.
- At the end of the module, you'll query the supported upgrade manifest using
[upgrade_manifest.py](upgrade_manifest.py):build_upgrade_pairs. Similar to [cql_tests.py](cql_tests.py) you'll probably want to combine the upgrade paths with other configurations for a more complete test matrix. Don't forget to do the class gen
for each class in your module.
- To accomplish the actual code gen, it's easiest to look at some examples and borrow
logic from another implemented module. The code is a bit difficult to follow, but
it's purpose is pretty straightforward which is just to build a bunch of classes
at runtime that are specialized to each test case.
