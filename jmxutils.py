from urllib2 import urlopen
import json
import os
import subprocess

JOLOKIA_JAR = os.path.join('lib', 'jolokia-jvm-1.2.3-agent.jar')


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
                             for k, v in kwargs.iteritems())
    return rv


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
        args = ('java',
                '-jar', JOLOKIA_JAR,
                '--host', self.node.network_interfaces['binary'][0],
                'start', str(self.node.pid))
        try:
            subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, exc:
            print "Failed to start jolokia agent (command was: %s): %s" % (' '.join(args), exc)
            print "Exit status was: %d" % (exc.returncode,)
            print "Output was: %s" % (exc.output,)
            raise

    def stop(self):
        """
        Stops the Jolokia agent.
        """
        args = ('java',
                '-jar', JOLOKIA_JAR,
                'stop', str(self.node.pid))
        try:
            subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, exc:
            print "Failed to stop jolokia agent (command was: %s): %s" % (' '.join(args), exc)
            print "Exit status was: %d" % (exc.returncode,)
            print "Output was: %s" % (exc.output,)
            raise

    def _query(self, body):
        request_data = json.dumps(body)
        url = 'http://%s:8778/jolokia/' % (self.node.network_interfaces['binary'][0],)
        response = urlopen(url, data=request_data, timeout=10.0)
        if response.code != 200:
            raise Exception("Failed to query Jolokia agent; HTTP response code: %d; response: %s" % (response.code, response.readlines()))

        raw_response = response.readline()
        response = json.loads(raw_response)
        if response['status'] != 200:
            stacktrace = response.get('stacktrace')
            if stacktrace:
                print "Stacktrace from Jolokia error follows:"
                for line in stacktrace.splitlines():
                    print line
            raise Exception("Jolokia agent returned non-200 status: %s" % (response,))
        return response

    def read_attribute(self, mbean, attribute, path=None):
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
        response = self._query(body)
        return response['value']

    def write_attribute(self, mbean, attribute, value, path=None):
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
        self._query(body)

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
