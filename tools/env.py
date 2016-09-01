"""
Module for defining the test environment.

This is located in tools/ so that both the main dtest.py module and the various
test modules can import from here without getting into circular import issues.
"""
import os

ALLOW_NOISY_LOGGING = os.environ.get('ALLOW_NOISY_LOGGING', '').lower() in ('yes', 'true')
