#!/bin/bash

# linter_check ensures your changes will pass on travis.
# Requires pep8 and flake8: pip install pep8 flake8

flake8 --ignore=E501,F811,F812,F821,F822,F823,F831,F841,N8,C9 --exclude=thrift_bindings,cassandra-thrift .
flake8_result=$?

# lint all files for everything but line length errors
git diff riptano/master...HEAD -U0 | pep8 --ignore=E501 --diff
pep8_style_check=$?

# lint all files except json_test.py for line length errors
git diff riptano/master...HEAD -U0 | pep8 --diff --exclude='json_test.py' --max-line-length=200
pep8_line_length=$?

echo -e "\nflake8 exited with ${flake8_result}."
echo "pep8 line length check exited with ${pep8_line_length} and style check exited with ${pep8_style_check}."

if [ $flake8_result -ne 0 -o $pep8_line_length -ne 0 -o $pep8_style_check -ne 0 ];
then
    echo "Your changes contain linter errors."
    echo "You can fix these manually or with autopep8, which can be installed with pip."
    exit 1
fi

echo "Done"
exit 0
