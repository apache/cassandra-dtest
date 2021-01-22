#!/usr/bin/env python
"""
usage: run_dtests.py [-h] [--use-vnodes] [--use-off-heap-memtables] [--num-tokens=NUM_TOKENS] [--data-dir-count-per-instance=DATA_DIR_COUNT_PER_INSTANCE]
                     [--force-resource-intensive-tests] [--skip-resource-intensive-tests] [--cassandra-dir=CASSANDRA_DIR] [--cassandra-version=CASSANDRA_VERSION]
                     [--delete-logs] [--execute-upgrade-tests] [--execute-upgrade-tests-only] [--disable-active-log-watching] [--keep-test-dir]
                     [--enable-jacoco-code-coverage] [--dtest-enable-debug-logging] [--dtest-print-tests-only] [--dtest-print-tests-output=DTEST_PRINT_TESTS_OUTPUT]
                     [--pytest-options=PYTEST_OPTIONS] [--dtest-tests=DTEST_TESTS]

optional arguments:
  -h, --help                                                 show this help message and exit
  --use-vnodes                                               Determines wither or not to setup clusters using vnodes for tests (default: False)
  --use-off-heap-memtables                                   Enable Off Heap Memtables when creating test clusters for tests (default: False)
  --num-tokens=NUM_TOKENS                                    Number of tokens to set num_tokens yaml setting to when creating instances with vnodes enabled (default: 256)
  --data-dir-count-per-instance=DATA_DIR_COUNT_PER_INSTANCE  Control the number of data directories to create per instance (default: 3)
  --force-resource-intensive-tests                           Forces the execution of tests marked as resource_intensive (default: False)
  --skip-resource-intensive-tests                            Skip all tests marked as resource_intensive (default: False)
  --cassandra-dir=CASSANDRA_DIR
  --cassandra-version=CASSANDRA_VERSION
  --delete-logs
  --execute-upgrade-tests                                    Execute Cassandra Upgrade Tests (e.g. tests annotated with the upgrade_test mark) (default: False)
  --execute-upgrade-tests-only                               Execute Cassandra Upgrade Tests without running any other tests (e.g. tests annotated with the upgrade_test mark) (default: False)
  --disable-active-log-watching                              Disable ccm active log watching, which will cause dtests to check for errors in the logs in a single operation instead of semi-realtime
                                                             processing by consuming ccm _log_error_handler callbacks (default: False)
  --keep-test-dir                                            Do not remove/cleanup the test ccm cluster directory and it's artifacts after the test completes (default: False)
  --enable-jacoco-code-coverage                              Enable JaCoCo Code Coverage Support (default: False)
  --dtest-enable-debug-logging                               Enable debug logging (for this script, pytest, and during execution of test functions) (default: False)
  --dtest-print-tests-only                                   Print list of all tests found eligible for execution given the provided options. (default: False)
  --dtest-print-tests-output=DTEST_PRINT_TESTS_OUTPUT        Path to file where the output of --dtest-print-tests-only should be written to (default: False)
  --pytest-options=PYTEST_OPTIONS                            Additional command line arguments to proxy directly thru when invoking pytest. (default: None)
  --dtest-tests=DTEST_TESTS                                  Comma separated list of test files, test classes, or test methods to execute. (default: None)
"""
import subprocess
import sys
import os
import re
import logging

from os import getcwd
from tempfile import NamedTemporaryFile

from _pytest.config.argparsing import Parser
import argparse

from conftest import pytest_addoption

logger = logging.getLogger(__name__)


class RunDTests():
    def run(self, argv):
        parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(prog,
                                                                                                             max_help_position=100,
                                                                                                             width=200))

        # this is a bit ugly: all of our command line arguments are added and configured as part
        # of pytest. however, we also have this wrapper script to make it easier for those who
        # aren't comfortable calling pytest directly. To avoid duplicating code (e.g. have the options
        # in two separate places) we directly use the pytest_addoption fixture from conftest.py. Unfortunately,
        # pytest wraps ArgumentParser, so, first we add the options to a pytest Parser, and then we pull
        # all of those custom options out and add them to the unwrapped ArgumentParser we want to use
        # here inside of run_dtests.py.
        #
        # So NOTE: to add a command line argument, if you're trying to do so by adding it here, you're doing it wrong!
        # add it to conftest.py:pytest_addoption
        pytest_parser = Parser()
        pytest_addoption(pytest_parser)

        # add all of the options from the pytest Parser we created, and add them into our ArgumentParser instance
        pytest_custom_opts = pytest_parser._anonymous
        for opt in pytest_custom_opts.options:
            parser.add_argument(opt._long_opts[0], action=opt._attrs['action'],
                                default=opt._attrs.get('default', None),
                                help=opt._attrs.get('help', None))

        parser.add_argument("--dtest-enable-debug-logging", action="store_true", default=False,
                            help="Enable debug logging (for this script, pytest, and during execution "
                                 "of test functions)")
        parser.add_argument("--dtest-print-tests-only", action="store_true", default=False,
                            help="Print list of all tests found eligible for execution given the provided options.")
        parser.add_argument("--dtest-print-tests-output", action="store", default=False,
                            help="Path to file where the output of --dtest-print-tests-only should be written to")
        parser.add_argument("--pytest-options", action="store", default=None,
                            help="Additional command line arguments to proxy directly thru when invoking pytest.")
        parser.add_argument("--dtest-tests", action="store", default=None,
                            help="Comma separated list of test files, test classes, or test methods to execute.")

        args = parser.parse_args()

        if args.dtest_enable_debug_logging:
            logging.root.setLevel(logging.DEBUG)
            logger.setLevel(logging.DEBUG)

            # cause logger to go to stdout
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logging.root.addHandler(handler)

        # Get dictionaries corresponding to each point in the configuration matrix
        # we want to run, then generate a config object for each of them.
        logger.debug('Generating configurations from the following matrix:\n\t{}'.format(args))

        args_to_invoke_pytest = []

        for arg in argv:
            if arg.startswith("--pytest-options") or arg.startswith("--dtest-"):
                continue
            args_to_invoke_pytest.append("'{the_arg}'".format(the_arg=arg))

        if args.dtest_print_tests_only:
            args_to_invoke_pytest.append("'--collect-only'")
            args_to_invoke_pytest.append("'-q'")

        if args.dtest_tests:
            for test in args.dtest_tests.split(","):
                args_to_invoke_pytest.append("'{test_name}'".format(test_name=test))

        args_to_invoke_pytest.append("'--ignore=meta_tests'")

        original_raw_cmd_args = ", ".join(args_to_invoke_pytest)

        logger.debug("args to call with: [%s]" % original_raw_cmd_args)

        # the original run_dtests.py script did it like this to hack around nosetest
        # limitations -- i'm not sure if they still apply or not in a pytest world
        # but for now just leaving it as is, because it does the job (although
        # certainly is still pretty complicated code and has a hacky feeling)
        to_execute = (
            "import pytest\n"
            "import sys\n"
            "sys.exit(pytest.main([{options}]))\n".format(options=original_raw_cmd_args))
        temp = NamedTemporaryFile(dir=getcwd())
        logger.debug('Writing the following to {}:'.format(temp.name))

        temp.write(to_execute.encode("utf-8"))
        temp.flush()

        # We pass nose_argv as options to the python call to maintain
        # compatibility with the nosetests command. Arguments passed in via the
        # command line are treated one way, args passed in as
        # nose.main(argv=...) are treated another. Compare with the options
        # -xsv for an example.
        cmd_list = [sys.executable, temp.name]
        logger.debug('subprocess.call-ing {cmd_list}'.format(cmd_list=cmd_list))

        sp = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())

        if args.dtest_print_tests_only:
            stdout, stderr = sp.communicate()

            if sp.returncode != 0:
                print(stderr.decode("utf-8"))
                result = sp.returncode
                exit(result)

            all_collected_test_modules = collect_test_modules(stdout)
            joined_test_modules = "\n".join(all_collected_test_modules)
            print("Collected %d Test Modules" % len(all_collected_test_modules))
            if args.dtest_print_tests_output is not None:
                collected_tests_output_file = open(args.dtest_print_tests_output, "w")
                collected_tests_output_file.write(joined_test_modules)
                collected_tests_output_file.close()

        else:
            while True:
                stdout_output = sp.stdout.readline()
                stdout_output_str = stdout_output.decode("utf-8")
                if stdout_output_str == '' and sp.poll() is not None:
                    break
                if stdout_output_str:
                    print(stdout_output_str.strip())

                stderr_output = sp.stderr.readline()
                stderr_output_str = stderr_output.decode("utf-8")
                if stderr_output_str == '' and sp.poll() is not None:
                    break
                if stderr_output_str:
                    print(stderr_output_str.strip())

        exit(sp.returncode)


def collect_test_modules(stdout):
    test_regex_pattern = re.compile(r".+::.+::.+")
    all_collected_test_modules = []
    for line in stdout.decode("utf-8").split('\n'):
        re_ret = re.search(test_regex_pattern, line)
        if re_ret:
            all_collected_test_modules.append(line)
        elif line.strip() != "":
            print(line)

    return all_collected_test_modules


if __name__ == '__main__':
    RunDTests().run(sys.argv[1:])
