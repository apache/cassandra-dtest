import json
import os
import subprocess
import urllib.request
import urllib.parse
import logging

import ccmlib.common as common

logger = logging.getLogger(__name__)

JOLOKIA_JAR = os.path.join('lib', 'jolokia-jvm-1.6.2-agent.jar')
CLASSPATH_SEP = ';' if common.is_win() else ':'


def jolokia_classpath():
    if 'JAVA_HOME' in os.environ:
        tools_jar = os.path.join(os.environ['JAVA_HOME'], 'lib', 'tools.jar')
        return CLASSPATH_SEP.join((tools_jar, JOLOKIA_JAR))
    else:
        logger.warning("Environment variable $JAVA_HOME not present: jmx-based " +
                       "tests may fail because of missing $JAVA_HOME/lib/tools.jar.")
        return JOLOKIA_JAR


def java_bin():
    if 'JAVA_HOME' in os.environ:
        return os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
    else:
        return 'java'


def make_mbean(package, type, **kwargs):
    '''
    Builds the name for an mbean.

    `package` is appended to the org.apache.cassandra domain.

    `type` is used as the 'type' property.

    All other keyword arguments are used as properties in the mbean's name.

    Example usage:

    >>> make_mbean('db', 'IndexSummaries')
    'org.apache.cassandra.db:type=IndexSummaries'
    >>> make_mbean('metrics', type='ColumnFamily', name='MemtableColumnsCount', keyspace='ks', scope='table')
    'org.apache.cassandra.metrics:type=ColumnFamily,keyspace=ks,name=MemtableColumnsCount,scope=table'
    '''
    rv = 'org.apache.cassandra.%s:type=%s' % (package, type)
    if kwargs:
        rv += ',' + ','.join('{k}={v}'.format(k=k, v=v)
                             for k, v in kwargs.items())
    return rv


def enable_jmx_ssl(node,
                   require_client_auth=False,
                   disable_user_auth=True,
                   keystore=None,
                   keystore_password=None,
                   truststore=None,
                   truststore_password=None):
    """
    Sets up a node (currently via the cassandra-env file) to use SSL for JMX connections
    """
    # mandatory replacements when enabling SSL
    replacement_list = [
        (r'\$env:JVM_OPTS="\$env:JVM_OPTS -Dcassandra.jmx.local.port=$JMX_PORT")',
         '#$env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.local.port=$JMX_PORT"'),
        (r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"',
         '$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"'),
        (r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"',
         '$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"')
    ] if common.is_win() else [
        ('LOCAL_JMX=yes', 'LOCAL_JMX=no'),
        (r'#JVM_OPTS="\$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"',
         'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"'),
        (r'JVM_OPTS="\$JVM_OPTS -Dcom.sun.management.jmxremote.rmi.port=\$JMX_PORT"',
         '#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"')
    ]

    if require_client_auth:
        if common.is_win():
            replacement_list.append((r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"',
                                    '$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"'))
        else:
            replacement_list.append((r'#JVM_OPTS="\$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"',
                                     'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"'))

    if keystore:
        if common.is_win():
            replacement_list.append((r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Djavax.net.ssl.keyStore=C:/keystore"',
                                    '$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.keyStore={path}"'.format(path=keystore)))
        else:
            replacement_list.append((r'#JVM_OPTS="\$JVM_OPTS -Djavax.net.ssl.keyStore=/path/to/keystore"',
                                     'JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.keyStore={path}"'.format(path=keystore)))
    if keystore_password:
        if common.is_win():
            replacement_list.append((r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Djavax.net.ssl.keyStorePassword=<keystore-password>"',
                                    '$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.keyStorePassword={password}"'.format(password=keystore_password)))
        else:
            replacement_list.append((r'#JVM_OPTS="\$JVM_OPTS -Djavax.net.ssl.keyStorePassword=<keystore-password>"',
                                     'JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.keyStorePassword={password}"'.format(password=keystore_password)))
    if truststore:
        if common.is_win():
            replacement_list.append((r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Djavax.net.ssl.trustStore=C:/truststore"',
                                    '$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.trustStore={path}"'.format(path=truststore)))
        else:
            replacement_list.append((r'#JVM_OPTS="\$JVM_OPTS -Djavax.net.ssl.trustStore=/path/to/truststore"',
                                     'JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStore={path}"'.format(path=truststore)))
    if truststore_password:
        if common.is_win():
            replacement_list.append((r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Djavax.net.ssl.trustStorePassword=<truststore-password>"',
                                     '$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.trustStorePassword={password}"'.format(password=truststore_password)))
        else:
            replacement_list.append((r'#JVM_OPTS="\$JVM_OPTS -Djavax.net.ssl.trustStorePassword=<truststore-password>"',
                                     'JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStorePassword={password}"'.format(password=truststore_password)))

    # switches off user authentication, distinct from validation of client certificates (i.e. require_client_auth)
    if disable_user_auth:
        if not common.is_win():
            replacement_list.append((r'JVM_OPTS="\$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"',
                                     'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"'))

    common.replaces_in_file(node.envfilename(), replacement_list)


def apply_jmx_authentication(node):
    replacement_list = [
        (r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"',
         '$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"'),
        (r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"',
         '$env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"'),
        (r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Djava.security.auth.login.config=C:/cassandra-jaas.config"',
         r'$env:JVM_OPTS="$env:JVM_OPTS -Djava.security.auth.login.config=$env:CASSANDRA_CONF\cassandra-jaas.config"'),
        (r'#\$env:JVM_OPTS="\$env:JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"',
         '$env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"')
    ] if common.is_win() else [
        (r'JVM_OPTS="\$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"',
         'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"'),
        (r'JVM_OPTS="\$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"',
         '#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"'),
        (r'#JVM_OPTS="\$JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"',
         'JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"'),
        (r'#JVM_OPTS="\$JVM_OPTS -Djava.security.auth.login.config=\$CASSANDRA_CONF/cassandra-jaas.config"',
         'JVM_OPTS="$JVM_OPTS -Djava.security.auth.login.config=$CASSANDRA_CONF/cassandra-jaas.config"'),
        (r'#JVM_OPTS="\$JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"',
         'JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"')
    ]

    common.replaces_in_file(node.envfilename(), replacement_list)


class JolokiaAgent(object):
    """
    This class provides a simple way to read, write, and execute
    JMX attributes and methods through a Jolokia agent.

    Example usage:

        node = cluster.nodelist()[0]
        mbean = make_mbean('db', 'IndexSummaries')
        with JolokiaAgent(node) as jmx:
            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')
            jmx.write_attribute(mbean, 'MemoryPoolCapacityInMB', 0)
            jmx.execute_method(mbean, 'redistributeSummaries')
    """

    node = None

    def __init__(self, node):
        self.node = node

    def start(self):
        """
        Starts the Jolokia agent.  The process will fork from the parent
        and continue running until stop() is called.
        """
        args = (java_bin(),
                '-cp', jolokia_classpath(),
                'org.jolokia.jvmagent.client.AgentLauncher',
                '--host', self.node.network_interfaces['binary'][0],
                'start', str(self.node.pid))

        try:
            subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exc:
            print("Failed to start jolokia agent (command was: %s): %s" % (' '.join(args), exc))
            print("Exit status was: %d" % (exc.returncode,))
            print("Output was: %s" % (exc.output,))
            raise

    def stop(self):
        """
        Stops the Jolokia agent.
        """
        args = (java_bin(),
                '-cp', jolokia_classpath(),
                'org.jolokia.jvmagent.client.AgentLauncher',
                'stop', str(self.node.pid))
        try:
            subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exc:
            print("Failed to stop jolokia agent (command was: %s): %s" % (' '.join(args), exc))
            print("Exit status was: %d" % (exc.returncode,))
            print("Output was: %s" % (exc.output,))
            raise

    def _query(self, body, verbose=True):
        request_data = json.dumps(body).encode("utf-8")
        url = 'http://%s:8778/jolokia/' % (self.node.network_interfaces['binary'][0],)
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req, data=request_data, timeout=10.0)
        if response.code != 200:
            raise Exception("Failed to query Jolokia agent; HTTP response code: %d; response: %s" % (response.code, response.readlines()))

        raw_response = response.readline()
        response = json.loads(raw_response.decode(encoding='utf-8'))
        if response['status'] != 200:
            stacktrace = response.get('stacktrace')
            if stacktrace and verbose:
                print("Stacktrace from Jolokia error follows:")
                for line in stacktrace.splitlines():
                    print(line)
            raise Exception("Jolokia agent returned non-200 status: %s" % (response,))
        return response

    def has_mbean(self, mbean, verbose=True):
        """
        Check for the existence of an MBean

        `mbean` should be the full name of an mbean.  See the mbean() utility
        function for an easy way to create this.
        """
        body = {'type': 'search',
                'mbean': mbean}
        response = self._query(body, verbose=verbose)
        return len(response['value']) > 0

    def read_attribute(self, mbean, attribute, path=None, verbose=True):
        """
        Reads a single JMX attribute.

        `mbean` should be the full name of an mbean.  See the mbean() utility
        function for an easy way to create this.

        `attribute` should be the name of an attribute on that mbean.

        `path` is an optional string that can be used to specify sub-attributes
        for complex JMX attributes.
        """
        body = {'type': 'read',
                'mbean': mbean,
                'attribute': attribute}
        if path:
            body['path'] = path
        response = self._query(body, verbose=verbose)
        return response['value']

    def write_attribute(self, mbean, attribute, value, path=None, verbose=True):
        """
        Writes a values to a single JMX attribute.

        `mbean` should be the full name of an mbean.  See the mbean() utility
        function for an easy way to create this.

        `attribute` should be the name of an attribute on that mbean.

        `value` should be the new value for the attribute.

        `path` is an optional string that can be used to specify sub-attributes
        for complex JMX attributes.
        """

        body = {'type': 'write',
                'mbean': mbean,
                'attribute': attribute,
                'value': value}
        if path:
            body['path'] = path
        self._query(body, verbose=verbose)

    def execute_method(self, mbean, operation, arguments=None):
        """
        Executes a method on a JMX mbean.

        `mbean` should be the full name of an mbean.  See the mbean() utility
        function for an easy way to create this.

        `operation` should be the name of the method on the mbean.

        `arguments` is an optional list of arguments to pass to the method.
        """

        if arguments is None:
            arguments = []

        body = {'type': 'exec',
                'mbean': mbean,
                'operation': operation,
                'arguments': arguments}

        response = self._query(body)
        return response['value']

    def __enter__(self):
        """ For contextmanager-style usage. """
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        """ For contextmanager-style usage. """
        self.stop()
        return exc_type is None
