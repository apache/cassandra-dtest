import os
import subprocess
import time
import hashlib
import logging
import pytest

from collections import Mapping

from ccmlib.node import Node


logger = logging.getLogger(__name__)


# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None, byteman_port='0'):
    i = len(cluster.nodes) + 1
    node = Node('node%s' % i,
                cluster,
                bootstrap,
                ('127.0.0.%s' % i, 9160),
                ('127.0.0.%s' % i, 7000),
                str(7000 + i * 100),
                remote_debug_port,
                token,
                binary_interface=('127.0.0.%s' % i, 9042),
                                 byteman_port=byteman_port)
    cluster.add(node, not bootstrap, data_center=data_center)
    return node


def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)


def generate_ssl_stores(base_dir, passphrase='cassandra'):
    """
    Util for generating ssl stores using java keytool -- nondestructive method if stores already exist this method is
    a no-op.

    @param base_dir (str) directory where keystore.jks, truststore.jks and ccm_node.cer will be placed
    @param passphrase (Optional[str]) currently ccm expects a passphrase of 'cassandra' so it's the default but it can be
            overridden for failure testing
    @return None
    @throws CalledProcessError If the keytool fails during any step
    """

    if os.path.exists(os.path.join(base_dir, 'keystore.jks')):
        logger.debug("keystores already exists - skipping generation of ssl keystores")
        return

    logger.debug("generating keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-genkeypair', '-alias', 'ccm_node', '-keyalg', 'RSA', '-validity', '365',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'), '-storepass', passphrase,
                           '-dname', 'cn=Cassandra Node,ou=CCMnode,o=DataStax,c=US', '-keypass', passphrase])
    logger.debug("exporting cert from keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-export', '-rfc', '-alias', 'ccm_node',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'),
                           '-file', os.path.join(base_dir, 'ccm_node.cer'), '-storepass', passphrase])
    logger.debug("importing cert into truststore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-import', '-file', os.path.join(base_dir, 'ccm_node.cer'),
                           '-alias', 'ccm_node', '-keystore', os.path.join(base_dir, 'truststore.jks'),
                           '-storepass', passphrase, '-noprompt'])


def list_to_hashed_dict(list):
    """
    takes a list and hashes the contents and puts them into a dict so the contents can be compared
    without order. unfortunately, we need to do a little massaging of our input; the result from
    the driver can return a OrderedMapSerializedKey (e.g. [0, 9, OrderedMapSerializedKey([(10, 11)])])
    but our "expected" list is simply a list of elements (or list of list). this means if we
    hash the values as is we'll get different results. to avoid this, when we see a dict,
    convert the raw values (key, value) into a list and insert that list into a new list
    :param list the list to convert into a dict
    :return: a dict containing the contents fo the list with the hashed contents
    """
    hashed_dict = dict()
    for item_lst in list:
        normalized_list = []
        for item in item_lst:
            if hasattr(item, "items"):
                tmp_list = []
                for a, b in item.items():
                    tmp_list.append(a)
                    tmp_list.append(b)
                normalized_list.append(tmp_list)
            else:
                normalized_list.append(item)
        list_str = str(normalized_list)
        utf8 = list_str.encode('utf-8', 'ignore')
        list_digest = hashlib.sha256(utf8).hexdigest()
        hashed_dict[list_digest] = normalized_list
    return hashed_dict


def get_current_test_name():
    """
    See https://docs.pytest.org/en/latest/example/simple.html#pytest-current-test-environment-variable
    :return: returns just the name of the current running test name
    """
    pytest_current_test = os.environ.get('PYTEST_CURRENT_TEST')
    test_splits = pytest_current_test.split("::")
    current_test_name = test_splits[len(test_splits) - 1]
    current_test_name = current_test_name.replace(" (call)", "")
    current_test_name = current_test_name.replace(" (setup)", "")
    current_test_name = current_test_name.replace(" (teardown)", "")
    return current_test_name


class ImmutableMapping(Mapping):
    """
    Convenience class for when you want an immutable-ish map.

    Useful at class level to prevent mutability problems (such as a method altering the class level mutable)
    """
    def __init__(self, init_dict):
        self._data = init_dict.copy()

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return '{cls}({data})'.format(cls=self.__class__.__name__, data=self._data)


def wait_for_agreement(thrift, timeout=10):
    def check_agreement():
        schemas = thrift.describe_schema_versions()
        if len([ss for ss in list(schemas.keys()) if ss != 'UNREACHABLE']) > 1:
            raise Exception("schema agreement not reached")
    retry_till_success(check_agreement, timeout=timeout)


def add_skip(cls, reason=""):
    if hasattr(cls, "pytestmark"):
        cls.pytestmark = cls.pytestmark.copy()
        cls.pytestmark.append(pytest.mark.skip(reason))
    else:
        cls.pytestmark = [pytest.mark.skip(reason)]
    return cls
