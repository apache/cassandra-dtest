#!/usr/bin/env python
"""
Usage: run_dtests.py [--nose-options NOSE_OPTIONS] [TESTS...] [--vnodes VNODES_OPTIONS...]
                 [--runner-debug | --runner-quiet] [--dry-run]

nosetests options:
    --nose-options NOSE_OPTIONS  specify options to pass to `nosetests`.
    TESTS                        space-separated list of tests to pass to `nosetests`

script configuration options:
    --runner-debug -d            print debug statements in this script
    --runner-quiet -q            quiet all output from this script

cluster configuration options:
    --vnodes VNODES_OPTIONS...   specify whether to run with or without vnodes.
                                 valid values: 'true' and 'false'

example:
    The following command will execute nosetests with the '-v' (verbose) option, vnodes disabled, and run a single test:
    ./run_dtests.py --nose-options -v --vnodes false repair_tests/repair_test.py:TestRepair.token_range_repair_test_with_cf

"""
from __future__ import print_function

import subprocess
import sys
import os
from collections import namedtuple
from itertools import product
from os import getcwd, environ
from tempfile import NamedTemporaryFile

from docopt import docopt

from plugins.dtestconfig import GlobalConfigObject


# Generate values in a matrix from these lists of values for each attribute
# not defined in arguments to the runner script.
default_config_matrix = GlobalConfigObject(
    vnodes=(True, False),
)


def _noop(*args, **kwargs):
    pass


class ValidationResult(namedtuple('_ValidationResult', ['serialized', 'error_messages'])):
    """
    A value to be returned from validation functions. If serialization works,
    return one with 'serialized' set, otherwise return a list of string on the
    'error_messages' attribute.
    """
    __slots__ = ()

    def __new__(cls, serialized=None, error_messages=None):
        if error_messages is None:
            error_messages = []

        success_result = serialized is not None
        failure_result = bool(error_messages)

        if success_result + failure_result != 1:
            msg = ('attempted to instantiate a {cls_name} with serialized='
                   '{serialized} and error_messages={error_messages}. {cls_name} '
                   'objects must be instantiated with either a serialized or '
                   'error_messages argument, but not both.')
            msg = msg.format(cls_name=cls.__name__,
                             serialized=serialized,
                             error_messages=error_messages)
            raise ValueError(msg)

        return super(ValidationResult, cls).__new__(cls, serialized=serialized, error_messages=error_messages)


def _validate_and_serialize_vnodes(vnodes_value):
    """
    Validate the values received for vnodes configuration. Returns a
    ValidationResult.

    If the values validate, return a ValidationResult with 'serialized' set to
    the equivalent of:

        tuple(set({'true': True, 'false':False}[v.lower()] for v in vnodes_value))

    If the values don't validate, return a ValidationResult with 'messages' set
    to a list of strings, each of which points out an invalid value.
    """
    messages = []
    vnodes_value = set(v.lower() for v in vnodes_value)
    value_map = {'true': True, 'false': False}

    for v in vnodes_value:
        if v not in value_map:
            messages.append('{} not a valid value for --vnodes option. '
                            'valid values are {} (case-insensitive)'.format(v, ', '.join(list(value_map))))

    if messages:
        return ValidationResult(error_messages=messages)

    serialized = tuple({value_map[v] for v in vnodes_value})
    return ValidationResult(serialized=serialized)


def validate_and_serialize_options(docopt_options):
    """
    For each value that should be configured for a config object, attempt to
    serialize the passed-in strings into objects that can be used for
    configuration. If no values were passed in, use the list of options from
    the defaults above.

    Raises a ValueError and prints an error message if any values are invalid
    or didn't serialize correctly.
    """
    vnodes = _validate_and_serialize_vnodes(docopt_options['--vnodes'])
    if vnodes.error_messages:
        raise ValueError('Validation error:\n{}'.format('\t\n'.join(list(vnodes.error_messages))))
    return GlobalConfigObject(
        vnodes=vnodes.serialized or default_config_matrix.vnodes
    )


def product_of_values(d):
    """
    Transforms a dictionary of {key: list(configuration_options} into a tuple
    of dictionaries, each corresponding to a point in the product, with the
    values preserved at the keys where they were found in the argument.

    This is difficult to explain and is probably best demonstrated with an
    example:

        >>> from pprint import pprint
        >>> from runner import product_of_values
        >>> pprint(product_of_values(
        ...     {'a': [1, 2, 3],
        ...      'b': [4, 5, 6]}
        ... ))
        ({'a': 1, 'b': 4},
         {'a': 1, 'b': 5},
         {'a': 1, 'b': 6},
         {'a': 2, 'b': 4},
         {'a': 2, 'b': 5},
         {'a': 2, 'b': 6},
         {'a': 3, 'b': 4},
         {'a': 3, 'b': 5},
         {'a': 3, 'b': 6})

    So, in this case, we get something like

        for a_value in d['a']:
            for b_value in d['b']:
                yield {'a': a_value, 'b': b_value}

    This method does that, but for dictionaries with arbitrary iterables at
    arbitrary numbers of keys.
    """

    # transform, e.g., {'a': [1, 2, 3], 'b': [4, 5, 6]} into
    # [[('a', 1), ('a', 2), ('a', 3)],
    #  [('b', 4), ('b', 5), ('b', 6)]]
    tuple_list = [[(k, v) for v in v_list] for k, v_list in d.items()]

    # return the cartesian product of the flattened dict
    return tuple(dict(result) for result in product(*tuple_list))


if __name__ == '__main__':
    options = docopt(__doc__)
    validated_options = validate_and_serialize_options(options)

    nose_options = options['--nose-options'] or ''
    nose_option_list = nose_options.split()
    test_list = options['TESTS']
    nose_argv = nose_option_list + test_list

    verbosity = 1  # default verbosity level
    if options['--runner-debug']:
        verbosity = 2
    if options['--runner-quiet']:  # --debug and --quiet are mutually exclusive, enforced by docopt
        verbosity = 0

    debug = print if verbosity >= 2 else _noop
    output = print if verbosity >= 1 else _noop

    # Get dictionaries corresponding to each point in the configuration matrix
    # we want to run, then generate a config object for each of them.
    debug('Generating configurations from the following matrix:\n\t{}'.format(validated_options))
    all_configs = tuple(GlobalConfigObject(**d) for d in
                        product_of_values(validated_options._asdict()))
    output('About to run nosetests with config objects:\n'
           '\t{configs}\n'.format(configs='\n\t'.join(map(repr, all_configs))))

    results = []
    for config in all_configs:
        # These properties have to hold if we want to evaluate their reprs
        # below in the generated file.
        assert eval(repr(config), {'GlobalConfigObject': GlobalConfigObject}, {}) == config
        assert eval(repr(nose_argv), {}, {}) == nose_argv

        output('Running dtests with config object {}'.format(config))

        # Generate a file that runs nose, passing in config as the
        # configuration object.
        #
        # Yes, this is icky. The reason we do it is because we're dealing with
        # global configuration. We've decided global, nosetests-run-level
        # configuration is the way to go. This means we don't want to call
        # nose.main() multiple times in the same Python interpreter -- I have
        # not yet found a way to re-execute modules (thus getting new
        # module-level configuration) for each call. This didn't even work for
        # me with exec(script, {}, {}). So, here we are.
        #
        # How do we execute code in a new interpreter each time? Generate the
        # code as text, then shell out to a new interpreter.
        to_execute = (
            "import nose\n" +
            "from plugins.dtestconfig import DtestConfigPlugin, GlobalConfigObject\n" +
            "from plugins.dtestxunit import DTestXunit\n" +
            "from plugins.dtesttag import DTestTag\n"  +
            "from plugins.dtestcollect import DTestCollect\n" +
            "import sys\n" +
            "print sys.getrecursionlimit()\n" +
            "print sys.setrecursionlimit(8000)\n" +
            ("nose.main(addplugins=[DtestConfigPlugin({config}), DTestXunit(), DTestCollect(), DTestTag()])\n" if "TEST_TAG" in environ else "nose.main(addplugins=[DtestConfigPlugin({config}), DTestCollect(), DTestXunit()])\n")
        ).format(config=repr(config))
        temp = NamedTemporaryFile(dir=getcwd())
        debug('Writing the following to {}:'.format(temp.name))

        debug('```\n{to_execute}```\n'.format(to_execute=to_execute))
        temp.write(to_execute)
        temp.flush()

        # We pass nose_argv as options to the python call to maintain
        # compatibility with the nosetests command. Arguments passed in via the
        # command line are treated one way, args passed in as
        # nose.main(argv=...) are treated another. Compare with the options
        # -xsv for an example.
        cmd_list = [sys.executable, temp.name] + nose_argv
        debug('subprocess.call-ing {cmd_list}'.format(cmd_list=cmd_list))

        if options['--dry-run']:
            print('Would run the following command:\n\t{}'.format(cmd_list))
            with open(temp.name, 'r') as f:
                contents = f.read()
            print('{temp_name} contains:\n```\n{contents}```\n'.format(
                temp_name=temp.name,
                contents=contents
            ))
        else:
            results.append(subprocess.call(cmd_list, env=os.environ.copy()))
        # separate the end of the last subprocess.call output from the
        # beginning of the next by printing a newline.
        print()

    # If this answer:
    # http://stackoverflow.com/a/21788998/3408454
    # is to be believed, nosetests will exit with 0 on success, 1 on test or
    # other failure, and 2 on printing usage. We'll just grab the max of the
    # runs we saw -- if one printed usage, the whole run "printed usage", if
    # none printed usage, and one or more failed, we failed, else success.
    if not results:
        results = [0]
    exit(max(results))
