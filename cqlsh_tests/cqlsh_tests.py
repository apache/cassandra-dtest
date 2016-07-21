# -*- coding: utf-8 -*-
import binascii
import csv
import datetime
import os
import re
import subprocess
import sys
from decimal import Decimal
from distutils.version import LooseVersion
from tempfile import NamedTemporaryFile
from uuid import UUID, uuid4

from cassandra import InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement, BatchType
from ccmlib import common

from assertions import assert_all, assert_none
from cqlsh_tools import monkeypatch_driver, unmonkeypatch_driver
from dtest import Tester, debug
from tools import (create_c1c2_table, insert_c1c2, rows_to_list, since, known_failure)


class TestCqlsh(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)
        self.maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls._cached_driver_methods = monkeypatch_driver()

    @classmethod
    def tearDownClass(cls):
        unmonkeypatch_driver(cls._cached_driver_methods)

    def tearDown(self):
        if hasattr(self, 'tempfile') and not common.is_win():
            os.unlink(self.tempfile.name)
        super(TestCqlsh, self).tearDown()

    @since('2.1.9')
    def test_pep8_compliance(self):
        """
        @jira_ticket CASSANDRA-10066
        Checks that cqlsh is compliant with pep8 with the following command:
        pep8 --ignore E501,E402,E731 pylib/cqlshlib/*.py bin/cqlsh.py
        """
        cluster = self.cluster

        if cluster.version() < '2.2':
            cqlsh_path = os.path.join(cluster.get_install_dir(), 'bin', 'cqlsh')
        else:
            cqlsh_path = os.path.join(cluster.get_install_dir(), 'bin', 'cqlsh.py')

        cqlshlib_path = os.path.join(cluster.get_install_dir(), 'pylib', 'cqlshlib')
        cqlshlib_paths = os.listdir(cqlshlib_path)
        cqlshlib_paths = [os.path.join(cqlshlib_path, x) for x in cqlshlib_paths if '.py' in x and '.pyc' not in x]

        cmds = ['pep8', '--ignore', 'E501,E402,E731', cqlsh_path] + cqlshlib_paths

        debug(cmds)

        p = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()

        self.assertEqual(len(stdout), 0, stdout)
        self.assertEqual(len(stderr), 0, stderr)

    def test_simple_insert(self):

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds="""
            CREATE KEYSPACE simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            use simple;
            create TABLE simple (id int PRIMARY KEY , value text ) ;
            insert into simple (id, value) VALUES (1, 'one');
            insert into simple (id, value) VALUES (2, 'two');
            insert into simple (id, value) VALUES (3, 'three');
            insert into simple (id, value) VALUES (4, 'four');
            insert into simple (id, value) VALUES (5, 'five')""")

        session = self.patient_cql_connection(node1)
        rows = list(session.execute("select id, value from simple.simple"))

        self.assertEqual({1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five'},
                         {k: v for k, v in rows})

    def test_lwt(self):
        """
        Test LWT inserts and updates.

        @jira_ticket CASSANDRA-11003
        """

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds="""
            CREATE KEYSPACE lwt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            CREATE TABLE lwt.lwt (id int PRIMARY KEY , value text)""")

        def assert_applied(stmt, node=node1):
            expected_substring = '[applied]'
            output, _ = self.run_cqlsh(node, stmt)
            msg = '{exp} not found in output from {stmt}: {routput}'.format(
                exp=repr(expected_substring),
                stmt=repr(stmt),
                routput=repr(output)
            )
            self.assertIn(expected_substring, output, msg=msg)

        assert_applied("INSERT INTO lwt.lwt (id, value) VALUES (1, 'one') IF NOT EXISTS")
        assert_applied("INSERT INTO lwt.lwt (id, value) VALUES (1, 'one') IF NOT EXISTS")
        assert_applied("UPDATE lwt.lwt SET value = 'one' WHERE id = 1 IF value = 'one'")
        assert_applied("UPDATE lwt.lwt SET value = 'one' WHERE id = 1 IF value = 'zzz'")

    @since('2.2')
    def test_past_and_future_dates(self):
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds="""
            CREATE KEYSPACE simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            use simple;
            create TABLE simpledate (id int PRIMARY KEY , value timestamp ) ;
            insert into simpledate (id, value) VALUES (1, '2143-04-19 11:21:01+0000');
            insert into simpledate (id, value) VALUES (2, '1943-04-19 11:21:01+0000')""")

        session = self.patient_cql_connection(node1)
        list(session.execute("select id, value from simple.simpledate"))

        output, err = self.run_cqlsh(node1, 'use simple; SELECT * FROM simpledate')

        if LooseVersion(self.cluster.version()) >= LooseVersion('3.4'):
            self.assertIn("2143-04-19 11:21:01.000000+0000", output)
            self.assertIn("1943-04-19 11:21:01.000000+0000", output)
        else:
            self.assertIn("2143-04-19 11:21:01+0000", output)
            self.assertIn("1943-04-19 11:21:01+0000", output)

    @since('3.4')
    def test_sub_second_precision(self):
        """
        Test that we can query at millisecond precision.
        @jira_ticket 10428
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds="""
            CREATE KEYSPACE simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            use simple;
            create TABLE testsubsecond (id int, subid timestamp, value text, primary key (id, subid));
            insert into testsubsecond (id, subid, value) VALUES (1, '1943-06-19 11:21:01.123+0000', 'abc');
            insert into testsubsecond (id, subid, value) VALUES (2, '1943-06-19 11:21:01+0000', 'def')""")

        output, err = node1.run_cqlsh(cmds="use simple; SELECT * FROM testsubsecond "
                                           "WHERE id = 1 AND subid = '1943-06-19 11:21:01.123+0000'",
                                      return_output=True)

        debug(output)
        self.assertIn("1943-06-19 11:21:01.123000+0000", output)
        self.assertNotIn("1943-06-19 11:21:01.000000+0000", output)

        output, err = node1.run_cqlsh(cmds="use simple; SELECT * FROM testsubsecond "
                                           "WHERE id = 2 AND subid = '1943-06-19 11:21:01+0000'",
                                      return_output=True)

        debug(output)
        self.assertIn("1943-06-19 11:21:01.000000+0000", output)
        self.assertNotIn("1943-06-19 11:21:01.123000+0000", output)

    def verify_glass(self, node):
        session = self.patient_cql_connection(node)

        def verify_varcharmap(map_name, expected, encode_value=False):
            rows = list(session.execute((u"SELECT %s FROM testks.varcharmaptable WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';" % map_name).encode("utf-8")))
            if encode_value:
                got = {k.encode("utf-8"): v.encode("utf-8") for k, v in rows[0][0].iteritems()}
            else:
                got = {k.encode("utf-8"): v for k, v in rows[0][0].iteritems()}
            self.assertEqual(got, expected)

        verify_varcharmap('varcharasciimap', {
            'Vitrum edere possum, mihi non nocet.': 'Hello',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 'My',
            'Можам да јадам стакло, а не ме штета.': 'Name',
            'I can eat glass and it does not hurt me': 'Is'
        })

        verify_varcharmap('varcharbigintmap', {
            'Vitrum edere possum, mihi non nocet.': 5100003,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -45,
            'Можам да јадам стакло, а не ме штета.': 12300,
            'I can eat glass and it does not hurt me': 0
        })

        verify_varcharmap('varcharblobmap', {
            'Vitrum edere possum, mihi non nocet.': binascii.a2b_hex("FEED103A"),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': binascii.a2b_hex("DEADBEEF"),
            'Можам да јадам стакло, а не ме штета.': binascii.a2b_hex("BEEFBEEF"),
            'I can eat glass and it does not hurt me': binascii.a2b_hex("FEEB")
        })

        verify_varcharmap('varcharbooleanmap', {
            'Vitrum edere possum, mihi non nocet.': True,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': False,
            'Можам да јадам стакло, а не ме штета.': False,
            'I can eat glass and it does not hurt me': False
        })

        verify_varcharmap('varchardecimalmap', {
            'Vitrum edere possum, mihi non nocet.': Decimal('50'),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': Decimal('-20.4'),
            'Можам да јадам стакло, а не ме штета.': Decimal('11234234.3'),
            'I can eat glass and it does not hurt me': Decimal('10.0')
        })

        verify_varcharmap('varchardoublemap', {
            'Vitrum edere possum, mihi non nocet.': 4234243,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -432.311,
            'Можам да јадам стакло, а не ме штета.': 3.1415,
            'I can eat glass and it does not hurt me': 20000.0
        })

        verify_varcharmap('varcharfloatmap', {
            'Vitrum edere possum, mihi non nocet.': 10.0,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -234.3000030517578,
            'Можам да јадам стакло, а не ме штета.': -234234,
            'I can eat glass and it does not hurt me': 1000.5
        })

        verify_varcharmap('varcharintmap', {
            'Vitrum edere possum, mihi non nocet.': 1,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 2,
            'Можам да јадам стакло, а не ме штета.': -3,
            'I can eat glass and it does not hurt me': -500
        })

        verify_varcharmap('varcharinetmap', {
            'Vitrum edere possum, mihi non nocet.': '192.168.0.1',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': '127.0.0.1',
            'Можам да јадам стакло, а не ме штета.': '8.8.8.8',
            'I can eat glass and it does not hurt me': '8.8.4.4'
        })

        verify_varcharmap('varchartextmap', {
            'Vitrum edere possum, mihi non nocet.': 'Once I went',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 'On a trip',
            'Можам да јадам стакло, а не ме штета.': 'Across',
            'I can eat glass and it does not hurt me': 'The '
        })

        verify_varcharmap('varchartimestampmap', {
            'Vitrum edere possum, mihi non nocet.': datetime.datetime(2013, 6, 19, 3, 21, 1),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': datetime.datetime(1985, 8, 3, 4, 21, 1),
            'Можам да јадам стакло, а не ме штета.': datetime.datetime(2000, 1, 1, 0, 20, 1),
            'I can eat glass and it does not hurt me': datetime.datetime(1942, 3, 11, 5, 21, 1)
        })

        verify_varcharmap('varcharuuidmap', {
            'Vitrum edere possum, mihi non nocet.': UUID('7787064c-ce54-4324-abdd-05775b89ead7'),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': UUID('1df0b6ac-f3d3-456c-8b78-2bc70e585107'),
            'Можам да јадам стакло, а не ме штета.': UUID('e2ed2164-31dc-42cb-8ee9-47376e071210'),
            'I can eat glass and it does not hurt me': UUID('a487fe45-8af5-4454-ac66-2614286d7e89')
        })

        verify_varcharmap('varchartimeuuidmap', {
            'Vitrum edere possum, mihi non nocet.': UUID('4a36c100-d8ec-11e2-a28f-0800200c9a66'),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': UUID('670c7f90-d8ec-11e2-a28f-0800200c9a66'),
            'Можам да јадам стакло, а не ме штета.': UUID('750c2d70-d8ec-11e2-a28f-0800200c9a66'),
            'I can eat glass and it does not hurt me': UUID('80d74810-d8ec-11e2-a28f-0800200c9a66')
        })

        verify_varcharmap('varcharvarcharmap', {
            'Vitrum edere possum, mihi non nocet.': '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑',
            'Можам да јадам стакло, а не ме штета.': 'Можам да јадам стакло, а не ме штета.',
            'I can eat glass and it does not hurt me': 'I can eat glass and it does not hurt me'
        }, encode_value=True)

        verify_varcharmap('varcharvarintmap', {
            'Vitrum edere possum, mihi non nocet.': 1010010101020400204143243,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -40,
            'Можам да јадам стакло, а не ме штета.': 110230,
            'I can eat glass and it does not hurt me': 1400
        })

        output, err = self.run_cqlsh(node, 'use testks; SELECT * FROM varcharmaptable', ['--encoding=utf-8'])

        self.assertEquals(output.count('Можам да јадам стакло, а не ме штета.'), 16)
        self.assertEquals(output.count(' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑'), 16)
        self.assertEquals(output.count('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜'), 2)

    def test_eat_glass(self):

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds=u"""create KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use testks;

CREATE TABLE varcharmaptable (
        varcharkey varchar ,
        varcharasciimap map<varchar, ascii>,
        varcharbigintmap map<varchar, bigint>,
        varcharblobmap map<varchar, blob>,
        varcharbooleanmap map<varchar, boolean>,
        varchardecimalmap map<varchar, decimal>,
        varchardoublemap map<varchar, double>,
        varcharfloatmap map<varchar, float>,
        varcharintmap map<varchar, int>,
        varcharinetmap map<varchar, inet>,
        varchartextmap map<varchar, text>,
        varchartimestampmap map<varchar, timestamp>,
        varcharuuidmap map<varchar, uuid>,
        varchartimeuuidmap map<varchar, timeuuid>,
        varcharvarcharmap map<varchar, varchar>,
        varcharvarintmap map<varchar, varint>,
        PRIMARY KEY (varcharkey));

INSERT INTO varcharmaptable (varcharkey, varcharasciimap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 'My','Можам да јадам стакло, а не ме штета.': 'Name','I can eat glass and it does not hurt me': 'Is'} );

UPDATE varcharmaptable SET varcharasciimap = varcharasciimap + {'Vitrum edere possum, mihi non nocet.':'Cassandra'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharasciimap['Vitrum edere possum, mihi non nocet.'] = 'Hello' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharbigintmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -45,'Можам да јадам стакло, а не ме штета.': 12300,'I can eat glass and it does not hurt me': 0} );

UPDATE varcharmaptable SET varcharbigintmap = varcharbigintmap + {'Vitrum edere possum, mihi non nocet.':23} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharbigintmap['Vitrum edere possum, mihi non nocet.'] = 5100003 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharblobmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 0xDEADBEEF,'Можам да јадам стакло, а не ме штета.': 0xBEEFBEEF,'I can eat glass and it does not hurt me': 0xFEEB} );

UPDATE varcharmaptable SET varcharblobmap = varcharblobmap + {'Vitrum edere possum, mihi non nocet.':0x10} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharblobmap['Vitrum edere possum, mihi non nocet.'] = 0xFEED103A WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharbooleanmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': FALSE,'Можам да јадам стакло, а не ме штета.': FALSE,'I can eat glass and it does not hurt me': FALSE} );

UPDATE varcharmaptable SET varcharbooleanmap = varcharbooleanmap + {'Vitrum edere possum, mihi non nocet.':TRUE} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharbooleanmap['Vitrum edere possum, mihi non nocet.'] = TRUE WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchardecimalmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -20.4,'Можам да јадам стакло, а не ме штета.': 11234234.3,'I can eat glass and it does not hurt me': 10.0} );

UPDATE varcharmaptable SET varchardecimalmap = varchardecimalmap + {'Vitrum edere possum, mihi non nocet.':0.0} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchardecimalmap['Vitrum edere possum, mihi non nocet.'] = 50.0 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchardoublemap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -432.311,'Можам да јадам стакло, а не ме штета.': 3.1415,'I can eat glass and it does not hurt me': 20000.0} );

UPDATE varcharmaptable SET varchardoublemap = varchardoublemap + {'Vitrum edere possum, mihi non nocet.':11} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchardoublemap['Vitrum edere possum, mihi non nocet.'] = 4234243 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharfloatmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -234.3,'Можам да јадам стакло, а не ме штета.': -234234,'I can eat glass and it does not hurt me': 1000.5} );

UPDATE varcharmaptable SET varcharfloatmap = varcharfloatmap + {'Vitrum edere possum, mihi non nocet.':-3.14} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharfloatmap['Vitrum edere possum, mihi non nocet.'] = 10.0 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharintmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 2,'Можам да јадам стакло, а не ме штета.': -3,'I can eat glass and it does not hurt me': -500} );

UPDATE varcharmaptable SET varcharintmap = varcharintmap + {'Vitrum edere possum, mihi non nocet.':20000} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharintmap['Vitrum edere possum, mihi non nocet.'] = 1 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharinetmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': '127.0.0.1','Можам да јадам стакло, а не ме штета.': '8.8.8.8','I can eat glass and it does not hurt me': '8.8.4.4'} );

UPDATE varcharmaptable SET varcharinetmap = varcharinetmap + {'Vitrum edere possum, mihi non nocet.':'241.30.12.24'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharinetmap['Vitrum edere possum, mihi non nocet.'] = '192.168.0.1' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchartextmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 'On a trip','Можам да јадам стакло, а не ме штета.': 'Across','I can eat glass and it does not hurt me': 'The '} );

UPDATE varcharmaptable SET varchartextmap = varchartextmap + {'Vitrum edere possum, mihi non nocet.':'Sea'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchartextmap['Vitrum edere possum, mihi non nocet.'] = 'Once I went' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchartimestampmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': '1985-08-03T04:21:01+0000','Можам да јадам стакло, а не ме штета.': '2000-01-01T00:20:01+0000','I can eat glass and it does not hurt me': '1942-03-11T5:21:01+0000'} );

UPDATE varcharmaptable SET varchartimestampmap = varchartimestampmap + {'Vitrum edere possum, mihi non nocet.':'2043-11-04T11:21:01+0000'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchartimestampmap['Vitrum edere possum, mihi non nocet.'] = '2013-06-19T03:21:01+0000' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharuuidmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 1df0b6ac-f3d3-456c-8b78-2bc70e585107,'Можам да јадам стакло, а не ме штета.': e2ed2164-31dc-42cb-8ee9-47376e071210,'I can eat glass and it does not hurt me': a487fe45-8af5-4454-ac66-2614286d7e89} );

UPDATE varcharmaptable SET varcharuuidmap = varcharuuidmap + {'Vitrum edere possum, mihi non nocet.':d25bdfc7-eb81-472c-bf5b-b4e6afdf66c2} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharuuidmap['Vitrum edere possum, mihi non nocet.'] = 7787064c-ce54-4324-abdd-05775b89ead7 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchartimeuuidmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 670c7f90-d8ec-11e2-a28f-0800200c9a66,'Можам да јадам стакло, а не ме штета.': 750c2d70-d8ec-11e2-a28f-0800200c9a66,'I can eat glass and it does not hurt me': 80d74810-d8ec-11e2-a28f-0800200c9a66} );

UPDATE varcharmaptable SET varchartimeuuidmap = varchartimeuuidmap + {'Vitrum edere possum, mihi non nocet.':93e276f0-d8ec-11e2-a28f-0800200c9a66} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchartimeuuidmap['Vitrum edere possum, mihi non nocet.'] = 4a36c100-d8ec-11e2-a28f-0800200c9a66 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharvarcharmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑','Можам да јадам стакло, а не ме штета.': 'Можам да јадам стакло, а не ме штета.','I can eat glass and it does not hurt me': 'I can eat glass and it does not hurt me'} );

UPDATE varcharmaptable SET varcharvarcharmap = varcharvarcharmap + {'Vitrum edere possum, mihi non nocet.':'Vitrum edere possum, mihi non nocet.'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharvarcharmap['Vitrum edere possum, mihi non nocet.'] = '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharvarintmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -40,'Можам да јадам стакло, а не ме штета.': 110230,'I can eat glass and it does not hurt me': 1400} );

UPDATE varcharmaptable SET varcharvarintmap = varcharvarintmap + {'Vitrum edere possum, mihi non nocet.':20000} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharvarintmap['Vitrum edere possum, mihi non nocet.'] = 1010010101020400204143243 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜'
        """.encode("utf-8"))

        self.verify_glass(node1)

    def test_source_glass(self):

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds="SOURCE 'cqlsh_tests/glass.cql'")

        self.verify_glass(node1)

    @known_failure(failure_source='test',
                   jira_url='https://datastax.jira.com/browse/CSTAR-574',
                   flaky=False,
                   notes='Offheap and Windows jobs ONLY')
    def test_unicode_syntax_error(self):
        """
        Ensure that syntax errors involving unicode are handled correctly.
        @jira_ticket CASSANDRA-11626
        """

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        output, err = node1.run_cqlsh(cmds=u"ä;".encode('utf8'), return_output=True)
        err = err.decode('utf8')
        self.assertIn(u'Invalid syntax', err)
        self.assertIn(u'ä', err)

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11895',
                   flaky=False)
    @known_failure(failure_source='test',
                   jira_url='https://datastax.jira.com/browse/CSTAR-574',
                   flaky=False,
                   notes='Offheap and Windows jobs ONLY')
    def test_unicode_invalid_request_error(self):
        """
        Ensure that invalid request errors involving unicode are handled correctly.
        @jira_ticket CASSANDRA-11626
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        cmd = u'''create keyspace "ä" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};'''
        cmd = cmd.encode('utf8')
        output, err = node1.run_cqlsh(cmds=cmd, return_output=True, cqlsh_options=['--debug'])

        err = err.decode('utf8')
        self.assertIn(u'"ä" is not a valid keyspace name', err)

    def test_with_empty_values(self):
        """
        CASSANDRA-7196. Make sure the server returns empty values and CQLSH prints them properly
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds=u"""create keyspace  CASSANDRA_7196 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} ;

use CASSANDRA_7196;

CREATE TABLE has_all_types (
    num int PRIMARY KEY,
    intcol int,
    asciicol ascii,
    bigintcol bigint,
    blobcol blob,
    booleancol boolean,
    decimalcol decimal,
    doublecol double,
    floatcol float,
    textcol text,
    timestampcol timestamp,
    uuidcol uuid,
    varcharcol varchar,
    varintcol varint
) WITH compression = {'sstable_compression':'LZ4Compressor'};

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (0, -12, 'abcdefg', 1234567890123456789, 0x000102030405fffefd, true,
        19952.11882, 1.0, -2.1, 'Voilá!', '2012-05-14 12:53:20+0000',
        bd1924e1-6af8-44ae-b5e1-f24131dbd460, '"', 10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (1, 2147483647, '__!''$#@!~"', 9223372036854775807, 0xffffffffffffffffff, true,
        0.00000000000001, 9999999.999, 99999.99, '∭Ƕ⑮ฑ➳❏''', '1900-01-01+0000',
        ffffffff-ffff-ffff-ffff-ffffffffffff, 'newline->
<-', 9);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (2, 0, '', 0, 0x, false,
        0.0, 0.0, 0.0, '', 0,
        00000000-0000-0000-0000-000000000000, '', 0);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (3, -2147483648, '''''''', -9223372036854775808, 0x80, false,
        10.0000000000000, -1004.10, 100000000.9, '龍馭鬱', '2038-01-19T03:14-1200',
        ffffffff-ffff-1fff-8fff-ffffffffffff, '''', -10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (4, blobAsInt(0x), '', blobAsBigint(0x), 0x, blobAsBoolean(0x), blobAsDecimal(0x),
        blobAsDouble(0x), blobAsFloat(0x), '', blobAsTimestamp(0x), blobAsUuid(0x), '',
        blobAsVarint(0x))""".encode("utf-8"))

        output, err = self.run_cqlsh(node1, "select intcol, bigintcol, varintcol from CASSANDRA_7196.has_all_types where num in (0, 1, 2, 3, 4)")
        if common.is_win():
            output = output.replace('\r', '')

        expected = """
 intcol      | bigintcol            | varintcol
-------------+----------------------+-----------------------------
         -12 |  1234567890123456789 |  10000000000000000000000000
  2147483647 |  9223372036854775807 |                           9
           0 |                    0 |                           0
 -2147483648 | -9223372036854775808 | -10000000000000000000000000
             |                      |                            \n\n(5 rows)"""

        self.assertTrue(expected in output, "Output \n {%s} \n doesn't contain expected\n {%s}" % (output, expected))

    def tracing_from_system_traces_test(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)

        insert_c1c2(session, n=100)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * FROM ks.cf')
        self.assertIn('Tracing session: ', out)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * FROM system_traces.events')
        self.assertNotIn('Tracing session: ', out)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * FROM system_traces.sessions')
        self.assertNotIn('Tracing session: ', out)

    def select_element_inside_udt_test(self):
        self.cluster.populate(1).start()

        node1, = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TYPE address (
            street text,
            city text,
            zip_code int,
            phones set<text>
             );""")

        session.execute("""CREATE TYPE fullname (
            firstname text,
            lastname text
            );""")

        session.execute("""CREATE TABLE users (
            id uuid PRIMARY KEY,
            name FROZEN <fullname>,
            addresses map<text, FROZEN <address>>
            );""")

        session.execute("""INSERT INTO users (id, name)
            VALUES (62c36092-82a1-3a00-93d1-46196ee77204, {firstname: 'Marie-Claude', lastname: 'Josset'});
            """)

        out, err = self.run_cqlsh(node1, "SELECT name.lastname FROM ks.users WHERE id=62c36092-82a1-3a00-93d1-46196ee77204")
        self.assertNotIn('list index out of range', err)
        # If this assertion fails check CASSANDRA-7891

    def verify_output(self, query, node, expected):
        output, err = self.run_cqlsh(node, query, ['-u', 'cassandra', '-p', 'cassandra'])
        if common.is_win():
            output = output.replace('\r', '')

        self.assertEqual(len(err), 0, "Failed to execute cqlsh: {}".format(err))

        debug(output)
        self.assertTrue(expected in output, "Output \n {%s} \n doesn't contain expected\n {%s}" % (output, expected))

    def test_list_queries(self):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': '0'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(1)
        self.cluster.start()
        node1, = self.cluster.nodelist()
        node1.watch_log_for('Created default superuser')

        conn = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        conn.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        conn.execute("CREATE TABLE ks.t1 (k int PRIMARY KEY, v int)")
        conn.execute("CREATE USER user1 WITH PASSWORD 'user1'")
        conn.execute("GRANT ALL ON ks.t1 TO user1")

        if self.cluster.version() >= '2.2':
            self.verify_output("LIST USERS", node1, """
 name      | super
-----------+-------
 cassandra |  True
     user1 | False

(2 rows)
""")
        else:
            self.verify_output("LIST USERS", node1, """
 name      | super
-----------+-------
     user1 | False
 cassandra |  True

(2 rows)
""")

        if self.cluster.version() >= '2.2':
            self.verify_output("LIST ALL PERMISSIONS OF user1", node1, """
 role  | username | resource      | permission
-------+----------+---------------+------------
 user1 |    user1 | <table ks.t1> |      ALTER
 user1 |    user1 | <table ks.t1> |       DROP
 user1 |    user1 | <table ks.t1> |     SELECT
 user1 |    user1 | <table ks.t1> |     MODIFY
 user1 |    user1 | <table ks.t1> |  AUTHORIZE

(5 rows)
""")
        else:
            self.verify_output("LIST ALL PERMISSIONS OF user1", node1, """
 username | resource      | permission
----------+---------------+------------
    user1 | <table ks.t1> |     CREATE
    user1 | <table ks.t1> |      ALTER
    user1 | <table ks.t1> |       DROP
    user1 | <table ks.t1> |     SELECT
    user1 | <table ks.t1> |     MODIFY
    user1 | <table ks.t1> |  AUTHORIZE

(6 rows)
""")

    def test_describe(self):
        """
        @jira_ticket CASSANDRA-7814
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()

        self.execute(
            cql="""
                CREATE KEYSPACE test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};
                CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int);
                CREATE INDEX myindex ON test.users (age);
                CREATE TABLE test.test (id int, col int, val text, PRIMARY KEY(id, col));
                CREATE INDEX ON test.test (col);
                CREATE INDEX ON test.test (val)
                """)

        # Describe keyspaces
        output = self.execute(cql="DESCRIBE KEYSPACES")
        self.assertIn("test", output)
        self.assertIn("system", output)

        # Describe keyspace
        self.execute(cql="DESCRIBE KEYSPACE test", expected_output=self.get_keyspace_output())
        self.execute(cql="DESCRIBE test", expected_output=self.get_keyspace_output())
        self.execute(cql="DESCRIBE test2", expected_err="'test2' not found in keyspaces")
        self.execute(cql="USE test; DESCRIBE KEYSPACE", expected_output=self.get_keyspace_output())

        # Describe table
        self.execute(cql="DESCRIBE TABLE test.test", expected_output=self.get_test_table_output())
        self.execute(cql="DESCRIBE TABLE test.users", expected_output=self.get_users_table_output())
        self.execute(cql="DESCRIBE test.test", expected_output=self.get_test_table_output())
        self.execute(cql="DESCRIBE test.users", expected_output=self.get_users_table_output())
        self.execute(cql="DESCRIBE test.users2", expected_err="'users2' not found in keyspace 'test'")
        self.execute(cql="USE test; DESCRIBE TABLE test", expected_output=self.get_test_table_output())
        self.execute(cql="USE test; DESCRIBE TABLE users", expected_output=self.get_users_table_output())
        self.execute(cql="USE test; DESCRIBE test", expected_output=self.get_keyspace_output())
        self.execute(cql="USE test; DESCRIBE users", expected_output=self.get_users_table_output())
        self.execute(cql="USE test; DESCRIBE users2", expected_err="'users2' not found in keyspace 'test'")

        # Describe index
        self.execute(cql='DESCRIBE INDEX test.myindex', expected_output=self.get_index_output('myindex', 'test', 'users', 'age'))
        self.execute(cql='DESCRIBE INDEX test.test_col_idx', expected_output=self.get_index_output('test_col_idx', 'test', 'test', 'col'))
        self.execute(cql='DESCRIBE INDEX test.test_val_idx', expected_output=self.get_index_output('test_val_idx', 'test', 'test', 'val'))
        self.execute(cql='DESCRIBE test.myindex', expected_output=self.get_index_output('myindex', 'test', 'users', 'age'))
        self.execute(cql='DESCRIBE test.test_col_idx', expected_output=self.get_index_output('test_col_idx', 'test', 'test', 'col'))
        self.execute(cql='DESCRIBE test.test_val_idx', expected_output=self.get_index_output('test_val_idx', 'test', 'test', 'val'))
        self.execute(cql='DESCRIBE test.myindex2', expected_err="'myindex2' not found in keyspace 'test'")
        self.execute(cql='USE test; DESCRIBE INDEX myindex', expected_output=self.get_index_output('myindex', 'test', 'users', 'age'))
        self.execute(cql='USE test; DESCRIBE INDEX test_col_idx', expected_output=self.get_index_output('test_col_idx', 'test', 'test', 'col'))
        self.execute(cql='USE test; DESCRIBE INDEX test_val_idx', expected_output=self.get_index_output('test_val_idx', 'test', 'test', 'val'))
        self.execute(cql='USE test; DESCRIBE myindex', expected_output=self.get_index_output('myindex', 'test', 'users', 'age'))
        self.execute(cql='USE test; DESCRIBE test_col_idx', expected_output=self.get_index_output('test_col_idx', 'test', 'test', 'col'))
        self.execute(cql='USE test; DESCRIBE test_val_idx', expected_output=self.get_index_output('test_val_idx', 'test', 'test', 'val'))
        self.execute(cql='USE test; DESCRIBE myindex2', expected_err="'myindex2' not found in keyspace 'test'")

        # Drop table and recreate
        self.execute(cql='DROP TABLE test.users')
        self.execute(cql='DESCRIBE test.users', expected_err="'users' not found in keyspace 'test'")
        self.execute(cql='DESCRIBE test.myindex', expected_err="'myindex' not found in keyspace 'test'")
        self.execute(cql="""
                CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int);
                CREATE INDEX myindex ON test.users (age)
                """)
        self.execute(cql="DESCRIBE test.users", expected_output=self.get_users_table_output())
        self.execute(cql='DESCRIBE test.myindex', expected_output=self.get_index_output('myindex', 'test', 'users', 'age'))

        # Drop index and recreate
        self.execute(cql='DROP INDEX test.myindex')
        self.execute(cql='DESCRIBE test.myindex', expected_err="'myindex' not found in keyspace 'test'")
        self.execute(cql='CREATE INDEX myindex ON test.users (age)')
        self.execute(cql='DESCRIBE INDEX test.myindex', expected_output=self.get_index_output('myindex', 'test', 'users', 'age'))

        # Alter table. Renaming indexed columns is not allowed, and since 3.0 neither is dropping them
        # Prior to 3.0 the index would have been automatically dropped, but now we need to explicitly do that.
        self.execute(cql='DROP INDEX test.test_val_idx')
        self.execute(cql='ALTER TABLE test.test DROP val')
        self.execute(cql="DESCRIBE test.test", expected_output=self.get_test_table_output(has_val=False, has_val_idx=False))
        self.execute(cql='DESCRIBE test.test_val_idx', expected_err="'test_val_idx' not found in keyspace 'test'")
        self.execute(cql='ALTER TABLE test.test ADD val text')
        self.execute(cql="DESCRIBE test.test", expected_output=self.get_test_table_output(has_val=True, has_val_idx=False))
        self.execute(cql='DESCRIBE test.test_val_idx', expected_err="'test_val_idx' not found in keyspace 'test'")

    def test_describe_describes_non_default_compaction_parameters(self):
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node, = self.cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("CREATE TABLE tab (key int PRIMARY KEY ) "
                        "WITH compaction = {'class': 'SizeTieredCompactionStrategy',"
                        "'min_threshold': 10, 'max_threshold': 100 }")
        describe_cmd = 'DESCRIBE ks.tab'
        stdout, _ = self.run_cqlsh(node, describe_cmd)
        self.assertIn("'min_threshold': '10'", stdout)
        self.assertIn("'max_threshold': '100'", stdout)

    def test_describe_on_non_reserved_keywords(self):
        """
        @jira_ticket CASSANDRA-9232
        Test that we can describe tables whose name is a non-reserved CQL keyword
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node, = self.cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("CREATE TABLE map (key int PRIMARY KEY, val text)")
        describe_cmd = 'USE ks; DESCRIBE map'
        out, err = self.run_cqlsh(node, describe_cmd)
        self.assertEqual("", err)
        self.assertIn("CREATE TABLE ks.map (", out)

    @since('3.0')
    def test_describe_mv(self):
        """
        @jira_ticket CASSANDRA-9961
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()

        self.execute(
            cql="""
                CREATE KEYSPACE test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};
                CREATE TABLE test.users (username varchar, password varchar, gender varchar,
                session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username));
                CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)
                """)

        output = self.execute(cql="DESCRIBE KEYSPACE test")
        self.assertIn("users_by_state", output)

        self.execute(cql='DESCRIBE MATERIALIZED VIEW test.users_by_state', expected_output=self.get_users_by_state_mv_output())
        self.execute(cql='DESCRIBE test.users_by_state', expected_output=self.get_users_by_state_mv_output())
        self.execute(cql='USE test; DESCRIBE MATERIALIZED VIEW test.users_by_state', expected_output=self.get_users_by_state_mv_output())
        self.execute(cql='USE test; DESCRIBE MATERIALIZED VIEW users_by_state', expected_output=self.get_users_by_state_mv_output())
        self.execute(cql='USE test; DESCRIBE users_by_state', expected_output=self.get_users_by_state_mv_output())

        # test quotes
        self.execute(cql='USE test; DESCRIBE MATERIALIZED VIEW "users_by_state"', expected_output=self.get_users_by_state_mv_output())
        self.execute(cql='USE test; DESCRIBE "users_by_state"', expected_output=self.get_users_by_state_mv_output())

    def get_keyspace_output(self):
        return ("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;" +
                self.get_test_table_output() +
                self.get_users_table_output())

    def get_test_table_output(self, has_val=True, has_val_idx=True):
        if has_val:
            ret = """
                CREATE TABLE test.test (
                    id int,
                    col int,
                    val text,
                PRIMARY KEY (id, col)
                """
        else:
            ret = """
                CREATE TABLE test.test (
                    id int,
                    col int,
                PRIMARY KEY (id, col)
                """

        if LooseVersion(self.cluster.version()) >= LooseVersion('3.9'):
            ret += """
        ) WITH CLUSTERING ORDER BY (col ASC)
            AND bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND cdc = false
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99PERCENTILE';
        """
        elif LooseVersion(self.cluster.version()) >= LooseVersion('3.0'):
            ret += """
        ) WITH CLUSTERING ORDER BY (col ASC)
            AND bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99PERCENTILE';
        """
        else:
            ret += """
        ) WITH CLUSTERING ORDER BY (col ASC)
            AND bloom_filter_fp_chance = 0.01
            AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
            AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99.0PERCENTILE';
        """

        col_idx_def = self.get_index_output('test_col_idx', 'test', 'test', 'col')

        if has_val_idx:
            val_idx_def = self.get_index_output('test_val_idx', 'test', 'test', 'val')
            if LooseVersion(self.cluster.version()) >= LooseVersion('2.2'):
                return ret + "\n" + val_idx_def + "\n" + col_idx_def
            else:
                return ret + "\n" + col_idx_def + "\n" + val_idx_def
        else:
            return ret + "\n" + col_idx_def

    def get_users_table_output(self):
        if LooseVersion(self.cluster.version()) >= LooseVersion('3.9'):
            return """
        CREATE TABLE test.users (
            userid text PRIMARY KEY,
            age int,
            firstname text,
            lastname text
        ) WITH bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND cdc = false
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99PERCENTILE';
        """ + self.get_index_output('myindex', 'test', 'users', 'age')
        elif LooseVersion(self.cluster.version()) >= LooseVersion('3.0'):
            return """
        CREATE TABLE test.users (
            userid text PRIMARY KEY,
            age int,
            firstname text,
            lastname text
        ) WITH bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99PERCENTILE';
        """ + self.get_index_output('myindex', 'test', 'users', 'age')
        else:
            return """
        CREATE TABLE test.users (
            userid text PRIMARY KEY,
            age int,
            firstname text,
            lastname text
        ) WITH bloom_filter_fp_chance = 0.01
            AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
            AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99.0PERCENTILE';
        """ + self.get_index_output('myindex', 'test', 'users', 'age')

    def get_index_output(self, index, ks, table, col):
        return "CREATE INDEX {} ON {}.{} ({});".format(index, ks, table, col)

    def get_users_by_state_mv_output(self):
        if LooseVersion(self.cluster.version()) >= LooseVersion('3.9'):
            return """
                CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT *
                FROM test.users
                WHERE state IS NOT NULL AND username IS NOT NULL
                PRIMARY KEY (state, username)
                WITH CLUSTERING ORDER BY (username ASC)
                AND bloom_filter_fp_chance = 0.01
                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                AND cdc = false
                AND comment = ''
                AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99PERCENTILE';
               """
        else:
            return """
                CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT *
                FROM test.users
                WHERE state IS NOT NULL AND username IS NOT NULL
                PRIMARY KEY (state, username)
                WITH CLUSTERING ORDER BY (username ASC)
                AND bloom_filter_fp_chance = 0.01
                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                AND comment = ''
                AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99PERCENTILE';
               """

    def execute(self, cql, expected_output=None, expected_err=None, env_vars=None):
        debug(cql)
        node1, = self.cluster.nodelist()
        output, err = self.run_cqlsh(node1, cql, env_vars=env_vars)

        if err:
            if expected_err:
                err = err[10:]  # strip <stdin>:2:
                self.check_response(err, expected_err)
                return
            else:
                self.assertTrue(False, err)

        if expected_output:
            self.check_response(output, expected_output)

        return output

    def check_response(self, response, expected_response):
        lines = [s.strip() for s in response.split("\n") if s.strip()]
        expected_lines = [s.strip() for s in expected_response.split("\n") if s.strip()]
        self.assertEqual(expected_lines, lines)

    def test_copy_to(self):
        self.cluster.populate(1).start()
        node1, = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(10000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("SELECT * FROM testcopyto"))

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: %s' % (self.tempfile.name,))
        node1.run_cqlsh(cmds="COPY ks.testcopyto TO '%s'" % (self.tempfile.name,))

        # session
        with open(self.tempfile.name, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            result_list = [map(str, cql_row) for cql_row in results]
            self.assertItemsEqual(result_list, csvreader)

        # import the CSV file with COPY FROM
        session.execute("TRUNCATE ks.testcopyto")
        node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '%s'" % (self.tempfile.name,))
        new_results = list(session.execute("SELECT * FROM testcopyto"))
        self.assertItemsEqual(results, new_results)

    def test_float_formatting(self):
        """ Tests for CASSANDRA-9224, check format of float and double values"""
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            CREATE KEYSPACE formatting WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            use formatting;
            create TABLE values ( part text, id int, val1 double, val2 float, PRIMARY KEY (part, id) );
            insert into values (part, id, val1, val2) VALUES ('+', 1, 0.00000006, 0.00000006);
            insert into values (part, id, val1, val2) VALUES ('+', 2, 0.0000006, 0.0000006);
            insert into values (part, id, val1, val2) VALUES ('+', 3, 0.000006, 0.000006);
            insert into values (part, id, val1, val2) VALUES ('+', 4, 0.00006, 0.00006);
            insert into values (part, id, val1, val2) VALUES ('+', 5, 0.0006, 0.0006);
            insert into values (part, id, val1, val2) VALUES ('+', 6, 0.006, 0.006);
            insert into values (part, id, val1, val2) VALUES ('+', 7, 0.06, 0.06);
            insert into values (part, id, val1, val2) VALUES ('+', 8, 0.6, 0.6);
            insert into values (part, id, val1, val2) VALUES ('+', 9, 6, 6);
            insert into values (part, id, val1, val2) VALUES ('+', 10, 6.0, 6.0);
            insert into values (part, id, val1, val2) VALUES ('+', 11, 6.00, 6.00);
            insert into values (part, id, val1, val2) VALUES ('+', 12, 6.000, 6.000);
            insert into values (part, id, val1, val2) VALUES ('+', 13, 6.00000, 6.00000);
            insert into values (part, id, val1, val2) VALUES ('+', 14, 6.000000, 6.000000);
            insert into values (part, id, val1, val2) VALUES ('+', 15, 6.1, 6.1);
            insert into values (part, id, val1, val2) VALUES ('+', 16, 6.12, 6.12);
            insert into values (part, id, val1, val2) VALUES ('+', 17, 6.123, 6.123);
            insert into values (part, id, val1, val2) VALUES ('+', 18, 6.1234, 6.1234);
            insert into values (part, id, val1, val2) VALUES ('+', 19, 6.12345, 6.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 20, 6.123454, 6.123454);
            insert into values (part, id, val1, val2) VALUES ('+', 21, 6.123455, 6.123455);
            insert into values (part, id, val1, val2) VALUES ('+', 22, 6.123456, 6.123456);
            insert into values (part, id, val1, val2) VALUES ('+', 23, 6.1234565, 6.1234565);
            insert into values (part, id, val1, val2) VALUES ('+', 24, 6.1234555, 6.1234555);
            insert into values (part, id, val1, val2) VALUES ('+', 25, 6.12345555, 6.12345555);
            insert into values (part, id, val1, val2) VALUES ('+', 26, 6.12345555555555, 6.12345555555555);
            insert into values (part, id, val1, val2) VALUES ('+', 27, 16.12345, 16.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 28, 116.12345, 116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 29, 1116.12345, 1116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 30, 11116.12345, 11116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 31, 111116.12345, 111116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 32, 1111116.12345, 1111116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 33, 11111116.12345, 11111116.12345)""")

        self.verify_output("select * from formatting.values where part = '+'", node1, """
 part | id | val1        | val2
------+----+-------------+-------------
    + |  1 |       6e-08 |       6e-08
    + |  2 |       6e-07 |       6e-07
    + |  3 |       6e-06 |       6e-06
    + |  4 |       6e-05 |       6e-05
    + |  5 |      0.0006 |      0.0006
    + |  6 |       0.006 |       0.006
    + |  7 |        0.06 |        0.06
    + |  8 |         0.6 |         0.6
    + |  9 |           6 |           6
    + | 10 |           6 |           6
    + | 11 |           6 |           6
    + | 12 |           6 |           6
    + | 13 |           6 |           6
    + | 14 |           6 |           6
    + | 15 |         6.1 |         6.1
    + | 16 |        6.12 |        6.12
    + | 17 |       6.123 |       6.123
    + | 18 |      6.1234 |      6.1234
    + | 19 |     6.12345 |     6.12345
    + | 20 |     6.12345 |     6.12345
    + | 21 |     6.12345 |     6.12346
    + | 22 |     6.12346 |     6.12346
    + | 23 |     6.12346 |     6.12346
    + | 24 |     6.12346 |     6.12346
    + | 25 |     6.12346 |     6.12346
    + | 26 |     6.12346 |     6.12346
    + | 27 |    16.12345 |    16.12345
    + | 28 |   116.12345 |   116.12345
    + | 29 |  1116.12345 |  1116.12341
    + | 30 | 11116.12345 | 11116.12305
    + | 31 |  1.1112e+05 |  1.1112e+05
    + | 32 |  1.1111e+06 |  1.1111e+06
    + | 33 |  1.1111e+07 |  1.1111e+07
""")

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            use formatting;
            insert into values (part, id, val1, val2) VALUES ('-', 1, -0.00000006, -0.00000006);
            insert into values (part, id, val1, val2) VALUES ('-', 2, -0.0000006, -0.0000006);
            insert into values (part, id, val1, val2) VALUES ('-', 3, -0.000006, -0.000006);
            insert into values (part, id, val1, val2) VALUES ('-', 4, -0.00006, -0.00006);
            insert into values (part, id, val1, val2) VALUES ('-', 5, -0.0006, -0.0006);
            insert into values (part, id, val1, val2) VALUES ('-', 6, -0.006, -0.006);
            insert into values (part, id, val1, val2) VALUES ('-', 7, -0.06, -0.06);
            insert into values (part, id, val1, val2) VALUES ('-', 8, -0.6, -0.6);
            insert into values (part, id, val1, val2) VALUES ('-', 9, -6, -6);
            insert into values (part, id, val1, val2) VALUES ('-', 10, -6.0, -6.0);
            insert into values (part, id, val1, val2) VALUES ('-', 11, -6.00, -6.00);
            insert into values (part, id, val1, val2) VALUES ('-', 12, -6.000, -6.000);
            insert into values (part, id, val1, val2) VALUES ('-', 13, -6.00000, -6.00000);
            insert into values (part, id, val1, val2) VALUES ('-', 14, -6.000000, -6.000000);
            insert into values (part, id, val1, val2) VALUES ('-', 15, -6.1, -6.1);
            insert into values (part, id, val1, val2) VALUES ('-', 16, -6.12, -6.12);
            insert into values (part, id, val1, val2) VALUES ('-', 17, -6.123, -6.123);
            insert into values (part, id, val1, val2) VALUES ('-', 18, -6.1234, -6.1234);
            insert into values (part, id, val1, val2) VALUES ('-', 19, -6.12345, -6.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 20, -6.123454, -6.123454);
            insert into values (part, id, val1, val2) VALUES ('-', 21, -6.123455, -6.123455);
            insert into values (part, id, val1, val2) VALUES ('-', 22, -6.123456, -6.123456);
            insert into values (part, id, val1, val2) VALUES ('-', 23, -6.1234565, -6.1234565);
            insert into values (part, id, val1, val2) VALUES ('-', 24, -6.1234555, -6.1234555);
            insert into values (part, id, val1, val2) VALUES ('-', 25, -6.12345555, -6.12345555);
            insert into values (part, id, val1, val2) VALUES ('-', 26, -6.12345555555555, -6.12345555555555);
            insert into values (part, id, val1, val2) VALUES ('-', 27, -16.12345, -16.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 28, -116.12345, -116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 29, -1116.12345, -1116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 30, -11116.12345, -11116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 31, -111116.12345, -111116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 32, -1111116.12345, -1111116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 33, -11111116.12345, -11111116.12345)""")

        self.verify_output("select * from formatting.values where part = '-'", node1, """
 part | id | val1         | val2
------+----+--------------+--------------
    - |  1 |       -6e-08 |       -6e-08
    - |  2 |       -6e-07 |       -6e-07
    - |  3 |       -6e-06 |       -6e-06
    - |  4 |       -6e-05 |       -6e-05
    - |  5 |      -0.0006 |      -0.0006
    - |  6 |       -0.006 |       -0.006
    - |  7 |        -0.06 |        -0.06
    - |  8 |         -0.6 |         -0.6
    - |  9 |           -6 |           -6
    - | 10 |           -6 |           -6
    - | 11 |           -6 |           -6
    - | 12 |           -6 |           -6
    - | 13 |           -6 |           -6
    - | 14 |           -6 |           -6
    - | 15 |         -6.1 |         -6.1
    - | 16 |        -6.12 |        -6.12
    - | 17 |       -6.123 |       -6.123
    - | 18 |      -6.1234 |      -6.1234
    - | 19 |     -6.12345 |     -6.12345
    - | 20 |     -6.12345 |     -6.12345
    - | 21 |     -6.12345 |     -6.12346
    - | 22 |     -6.12346 |     -6.12346
    - | 23 |     -6.12346 |     -6.12346
    - | 24 |     -6.12346 |     -6.12346
    - | 25 |     -6.12346 |     -6.12346
    - | 26 |     -6.12346 |     -6.12346
    - | 27 |    -16.12345 |    -16.12345
    - | 28 |   -116.12345 |   -116.12345
    - | 29 |  -1116.12345 |  -1116.12341
    - | 30 | -11116.12345 | -11116.12305
    - | 31 |  -1.1112e+05 |  -1.1112e+05
    - | 32 |  -1.1111e+06 |  -1.1111e+06
    - | 33 |  -1.1111e+07 |  -1.1111e+07
""")

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            use formatting;
            insert into values (part, id, val1, val2) VALUES ('0', 1, 0, 0);
            insert into values (part, id, val1, val2) VALUES ('0', 2, 0.000000000001, 0.000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 3, 0.0000000000001, 0.0000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 4, 0.00000000000001, 0.00000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 5, 0.000000000000001, 0.000000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 6, 0.0000000000000001, 0.0000000000000001)""")

        self.verify_output("select * from formatting.values where part = '0'", node1, """
 part | id | val1  | val2
------+----+-------+-------
    0 |  1 |     0 |     0
    0 |  2 | 1e-12 | 1e-12
    0 |  3 | 1e-13 | 1e-13
    0 |  4 | 1e-14 | 1e-14
    0 |  5 | 1e-15 | 1e-15
    0 |  6 | 1e-16 | 1e-16
""")

    @since('2.2')
    def test_int_values(self):
        """ Tests for CASSANDRA-9399, check tables with int, bigint, smallint and tinyint values"""
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            CREATE KEYSPACE int_checks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            USE int_checks;
            CREATE TABLE values (part text, val1 int, val2 bigint, val3 smallint, val4 tinyint, PRIMARY KEY (part));
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('1', 1, 1, 1, 1);
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('0', 0, 0, 0, 0);
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('min', %d, %d, -32768, -128);
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('max', %d, %d, 32767, 127)""" % (-1 << 31, -1 << 63, (1 << 31) - 1, (1 << 63) - 1))

        self.assertEqual(len(stderr), 0, "Failed to execute cqlsh: {}".format(stderr))

        self.verify_output("select * from int_checks.values", node1, """
 part | val1        | val2                 | val3   | val4
------+-------------+----------------------+--------+------
  min | -2147483648 | -9223372036854775808 | -32768 | -128
  max |  2147483647 |  9223372036854775807 |  32767 |  127
    0 |           0 |                    0 |      0 |    0
    1 |           1 |                    1 |      1 |    1
""")

        self.verify_output("DESCRIBE TABLE int_checks.values", node1, """
CREATE TABLE int_checks.values (
    part text PRIMARY KEY,
    val1 int,
    val2 bigint,
    val3 smallint,
    val4 tinyint
""")

    @since('2.2')
    def test_datetime_values(self):
        """ Tests for CASSANDRA-9399, check tables with date and time values"""
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            CREATE KEYSPACE datetime_checks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            USE datetime_checks;
            CREATE TABLE values (d date, t time, PRIMARY KEY (d, t));
            INSERT INTO values (d, t) VALUES ('9800-12-31', '23:59:59.999999999');
            INSERT INTO values (d, t) VALUES ('2015-05-14', '16:30:00.555555555');
            INSERT INTO values (d, t) VALUES ('1582-1-1', '00:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '00:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '01:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '02:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '03:00:00.000000000')"""
                                        % (datetime.MINYEAR - 1, datetime.MINYEAR, datetime.MAXYEAR, datetime.MAXYEAR + 1,))
        # outside the MIN and MAX range it should print the number of days from the epoch

        self.assertEqual(len(stderr), 0, "Failed to execute cqlsh: {}".format(stderr))

        self.verify_output("select * from datetime_checks.values", node1, """
 d          | t
------------+--------------------
    -719528 | 00:00:00.000000000
 9800-12-31 | 23:59:59.999999999
 0001-01-01 | 01:00:00.000000000
 1582-01-01 | 00:00:00.000000000
    2932897 | 03:00:00.000000000
 9999-01-01 | 02:00:00.000000000
 2015-05-14 | 16:30:00.555555555
""")

        self.verify_output("DESCRIBE TABLE datetime_checks.values", node1, """
CREATE TABLE datetime_checks.values (
    d date,
    t time,
    PRIMARY KEY (d, t)
""")

    @since('2.2')
    def test_tracing(self):
        """
        Tests for CASSANDRA-9399, check tracing works.
        We care mostly that we do not crash, not so much on the tracing content, which may change and would
        therefore make this test too brittle.
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            CREATE KEYSPACE tracing_checks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            USE tracing_checks;
            CREATE TABLE test (id int, val text, PRIMARY KEY (id));
            INSERT INTO test (id, val) VALUES (1, 'adfad');
            INSERT INTO test (id, val) VALUES (2, 'lkjlk');
            INSERT INTO test (id, val) VALUES (3, 'iuiou')""")

        self.assertEqual(len(stderr), 0, "Failed to execute cqlsh: {}".format(stderr))

        self.verify_output("use tracing_checks; tracing on; select * from test", node1, """Now Tracing is enabled

 id | val
----+-------
  1 | adfad
  2 | lkjlk
  3 | iuiou

(3 rows)

Tracing session:""")

    @since('2.2')
    def test_client_warnings(self):
        """
        Tests for CASSANDRA-9399, check client warnings:
        - an unlogged batch across multiple partitions should generate a WARNING if there are more than
        unlogged_batch_across_partitions_warn_threshold partitions.

        Execute two unlogged batches: one only with fewer partitions and the other one with more than
        unlogged_batch_across_partitions_warn_threshold partitions.

        Check that only the second one generates a client warning.

        @jira_ticket CASSNADRA-9399
        @jira_ticket CASSANDRA-9303
        @jira_ticket CASSANDRA-11529
        """
        max_partitions_per_batch = 5
        self.cluster.populate(3)
        self.cluster.set_configuration_options({
            'unlogged_batch_across_partitions_warn_threshold': str(max_partitions_per_batch)})

        self.cluster.start(wait_for_binary_proto=True)

        node1 = self.cluster.nodelist()[0]

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            CREATE KEYSPACE client_warnings WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            USE client_warnings;
            CREATE TABLE test (id int, val text, PRIMARY KEY (id))""")

        self.assertEqual(len(stderr), 0, "Failed to execute cqlsh: {}".format(stderr))

        session = self.patient_cql_connection(node1)
        prepared = session.prepare("INSERT INTO client_warnings.test (id, val) VALUES (?, 'abc')")

        batch_without_warning = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch_with_warning = BatchStatement(batch_type=BatchType.UNLOGGED)

        for i in xrange(max_partitions_per_batch + 1):
            batch_with_warning.add(prepared, (i,))
            if i < max_partitions_per_batch:
                batch_without_warning.add(prepared, (i,))

        fut = session.execute_async(batch_without_warning)
        fut.result()  # wait for batch to complete before checking warnings
        self.assertIsNone(fut.warnings)

        fut = session.execute_async(batch_with_warning)
        fut.result()  # wait for batch to complete before checking warnings
        debug(fut.warnings)
        self.assertIsNotNone(fut.warnings)
        self.assertEquals(1, len(fut.warnings))
        self.assertEquals("Unlogged batch covering {} partitions detected against table [client_warnings.test]. "
                          .format(max_partitions_per_batch + 1) +
                          "You should use a logged batch for atomicity, or asynchronous writes for performance.",
                          fut.warnings[0])

    def test_connect_timeout(self):
        """
        @jira_ticket CASSANDRA-9601
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1, cmds='USE system', cqlsh_options=['--debug', '--connect-timeout=10'])
        self.assertTrue("Using connect timeout: 10 seconds" in stderr)

    def test_update_schema_with_down_node(self):
        """
        Test that issuing a DML statement after a DDL statement will work with a down node
        @jira_ticket CASSANDRA-9689
        """
        self.cluster.populate(3)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        node1, node2, node3 = self.cluster.nodelist()
        node2.stop(wait_other_notice=True)

        # --request-timeout option needed on 2.1 due to CASSANDRA-10686
        cqlsh_opts = [] if LooseVersion(self.cluster.version()) >= LooseVersion('2.2') else ['--request-timeout=6']

        stdout, stderr = self.run_cqlsh(node1, cmds="""
              CREATE KEYSPACE training WITH replication={'class':'SimpleStrategy','replication_factor':1};
              DESCRIBE KEYSPACES""", cqlsh_options=cqlsh_opts)
        self.assertIn("training", stdout)

        stdout, stderr = self.run_cqlsh(node1, """USE training;
                                                  CREATE TABLE mytable (id int, val text, PRIMARY KEY (id));
                                                  describe tables""", cqlsh_options=cqlsh_opts)
        self.assertIn("mytable", stdout)

    def test_describe_round_trip(self):
        """
        @jira_ticket CASSANDRA-9064

        Tests for the error reported in 9064 by:

        - creating the table described in the bug report, using LCS,
        - DESCRIBE-ing that table via cqlsh, then DROPping it,
        - running the output of the DESCRIBE statement as a CREATE TABLE statement, and
        - inserting a value into the table.

        The final two steps of the test should not fall down. If one does, that
        indicates the output of DESCRIBE is not a correct CREATE TABLE statement.
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'test_ks', 1)
        session.execute("CREATE TABLE lcs_describe (key int PRIMARY KEY) WITH compaction = "
                        "{'class': 'LeveledCompactionStrategy'}")
        describe_out, describe_err = self.run_cqlsh(node1, 'DESCRIBE TABLE test_ks.lcs_describe')

        session.execute('DROP TABLE test_ks.lcs_describe')

        create_statement = 'USE test_ks; ' + ' '.join(describe_out.splitlines())
        create_out, create_err = self.run_cqlsh(node1, create_statement)

        # these statements shouldn't fall down
        reloaded_describe_out, reloaded_describe_err = self.run_cqlsh(node1, 'DESCRIBE TABLE test_ks.lcs_describe')
        session.execute('INSERT INTO lcs_describe (key) VALUES (1)')

        # the table created before and after should be the same
        self.assertEqual(reloaded_describe_out, describe_out)

    @since('3.0')
    def test_materialized_view(self):
        """
        Test operations on a materialized view: create, describe, select from, drop, create using describe output.
        @jira_ticket CASSANDRA-9961 and CASSANDRA-10348
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'test', 1)

        session.execute("""CREATE TABLE test.users (username varchar, password varchar, gender varchar,
                session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username))""")

        session.execute("""CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)""")

        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        describe_out, err = self.run_cqlsh(node1, 'DESCRIBE MATERIALIZED VIEW test.users_by_state')
        self.assertEqual(0, len(err), err)

        select_out, err = self.run_cqlsh(node1, "SELECT * FROM test.users_by_state")
        self.assertEqual(0, len(err), err)
        debug(select_out)

        out, err = self.run_cqlsh(node1, "DROP MATERIALIZED VIEW test.users_by_state; DESCRIBE KEYSPACE test; DESCRIBE table test.users")
        self.assertEqual(0, len(err), err)
        self.assertNotIn("CREATE MATERIALIZED VIEW users_by_state", out)

        out, err = self.run_cqlsh(node1, 'DESCRIBE MATERIALIZED VIEW test.users_by_state')
        self.assertEqual(0, len(out.strip()), out)
        self.assertIn("Materialized view 'users_by_state' not found", err)

        create_statement = 'USE test; ' + ' '.join(describe_out.splitlines()).strip()[:-1]
        out, err = self.run_cqlsh(node1, create_statement)
        self.assertEqual(0, len(err), err)

        reloaded_describe_out, err = self.run_cqlsh(node1, 'DESCRIBE MATERIALIZED VIEW test.users_by_state')
        self.assertEqual(0, len(err), err)
        self.assertEqual(describe_out, reloaded_describe_out)

        reloaded_select_out, err = self.run_cqlsh(node1, "SELECT * FROM test.users_by_state")
        self.assertEqual(0, len(err), err)
        self.assertEqual(select_out, reloaded_select_out)

    @since('3.0')
    def test_clear(self):
        """
        Test the CLEAR command
        @jira_ticket CASSANDRA-10086
        """
        self._test_clear_screen('CLEAR')

    @since('3.0')
    def test_cls(self):
        """
        Test the CLS command
        @jira_ticket CASSANDRA-10086
        """
        self._test_clear_screen('CLS')

    def _test_clear_screen(self, cmd):
        """
            We use ANSI escape sequences to check the output:
            http://ascii-table.com/ansi-escape-sequences-vt-100.php

            Possible clear screen sequences:
            Esc[J or Esc[0J or Esc[1J or Esc[2J

            In addition we can move the cursor upper left:
            Esc[H or Esc[;H

            The escape character code is 27.

            We don't check for moving the cursor though, we only check that
            there is no error and that at least one of the possible clear
            screen sequences is contained in the output, via a regular
            expression.
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()

        out, err = self.run_cqlsh(node1, cmd, env_vars={'TERM': 'xterm'})
        self.assertEqual("", err)

        # Can't check escape sequence on cmd prompt. Assume no errors is good enough metric.
        if not common.is_win():
            self.assertTrue(re.search(chr(27) + "\[[0,1,2]?J", out))

    def test_batch(self):
        """
        Test the BATCH command
        @jira_ticket CASSANDRA-10272
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1, cmds="""
            CREATE KEYSPACE Excelsior  WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1};
            CREATE TABLE excelsior.data (id int primary key);
            BEGIN BATCH INSERT INTO excelsior.data (id) VALUES (0); APPLY BATCH""")

        self.assertEqual(0, len(stderr), stderr)
        self.assertEqual(0, len(stdout), stdout)

    def run_cqlsh(self, node, cmds, cqlsh_options=None, env_vars=None):
        if env_vars is None:
            env_vars = {}
        if cqlsh_options is None:
            cqlsh_options = []
        cdir = node.get_install_dir()
        cli = os.path.join(cdir, 'bin', common.platform_binary('cqlsh'))
        env = common.make_cassandra_env(cdir, node.get_path())
        env['LANG'] = 'en_US.UTF-8'
        env.update(env_vars)
        if LooseVersion(self.cluster.version()) >= LooseVersion('2.1'):
            host = node.network_interfaces['binary'][0]
            port = node.network_interfaces['binary'][1]
        else:
            host = node.network_interfaces['thrift'][0]
            port = node.network_interfaces['thrift'][1]
        args = cqlsh_options + [host, str(port)]
        sys.stdout.flush()
        p = subprocess.Popen([cli] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        for cmd in cmds.split(';'):
            p.stdin.write(cmd + ';\n')
        p.stdin.write("quit;\n")
        return p.communicate()


class CqlshSmokeTest(Tester):
    """
    Tests simple use cases for clqsh.
    """

    def setUp(self):
        super(CqlshSmokeTest, self).setUp()
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node1] = self.cluster.nodelist()
        self.session = self.patient_cql_connection(self.node1)

    def test_uuid(self):
        """
        the `uuid()` function can generate UUIDs from cqlsh.
        """
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', key_type='uuid', columns={'i': 'int'})

        out, err = self.node1.run_cqlsh("INSERT INTO ks.test (key) VALUES (uuid())",
                                        return_output=True)
        self.assertEqual(err, "")

        result = list(self.session.execute("SELECT key FROM ks.test"))
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 1)
        self.assertIsInstance(result[0][0], UUID)

        out, err = self.node1.run_cqlsh("INSERT INTO ks.test (key) VALUES (uuid())",
                                        return_output=True)
        self.assertEqual(err, "")

        result = list(self.session.execute("SELECT key FROM ks.test"))
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)
        self.assertIsInstance(result[0][0], UUID)
        self.assertIsInstance(result[1][0], UUID)
        self.assertNotEqual(result[0][0], result[1][0])

    def test_commented_lines(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test')

        self.node1.run_cqlsh(
            """
            -- commented line
            // Another comment
            /* multiline
             *
             * comment */
            """)
        out, err = self.node1.run_cqlsh("DESCRIBE KEYSPACE ks; // post-line comment",
                                        return_output=True)
        self.assertEqual(err, "")
        self.assertTrue(out.strip().startswith("CREATE KEYSPACE ks"))

    def test_colons_in_string_literals(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', columns={'i': 'int'})

        self.node1.run_cqlsh(
            """
            INSERT INTO ks.test (key) VALUES ('Cassandra:TheMovie');
            """)
        assert_all(self.session, "SELECT key FROM test",
                   [[u'Cassandra:TheMovie']])

    def test_select(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test')

        self.session.execute("INSERT INTO ks.test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.session, "SELECT key, c, v FROM test",
                   [[u'a', u'a', u'a']])

        out, err = self.node1.run_cqlsh("SELECT key, c, v FROM ks.test",
                                        return_output=True)
        out_lines = [x.strip() for x in out.split("\n")]

        # there should be only 1 row returned & it should contain the inserted values
        self.assertIn("(1 rows)", out_lines)
        self.assertIn("a | a | a", out_lines)
        self.assertEqual(err, '')

    def test_insert(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test')

        self.node1.run_cqlsh("INSERT INTO ks.test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.session, "SELECT key, c, v FROM test", [[u"a", u"a", u"a"]])

    def test_update(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test')

        self.session.execute("INSERT INTO test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.session, "SELECT key, c, v FROM test", [[u"a", u"a", u"a"]])
        self.node1.run_cqlsh("UPDATE ks.test SET v = 'b' WHERE key = 'a' AND c = 'a'")
        assert_all(self.session, "SELECT key, c, v FROM test", [[u"a", u"a", u"b"]])

    def test_delete(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', columns={'i': 'int'})

        self.session.execute("INSERT INTO test (key) VALUES ('a')")
        self.session.execute("INSERT INTO test (key) VALUES ('b')")
        self.session.execute("INSERT INTO test (key) VALUES ('c')")
        self.session.execute("INSERT INTO test (key) VALUES ('d')")
        self.session.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.session, 'SELECT key from test',
                   [[u'a'], [u'c'], [u'e'], [u'd'], [u'b']])

        self.node1.run_cqlsh("DELETE FROM ks.test WHERE key = 'c'")

        assert_all(self.session, 'SELECT key from test',
                   [[u'a'], [u'e'], [u'd'], [u'b']])

    def test_batch(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', columns={'i': 'int'})
        # run batch statement (inserts are fine)
        self.node1.run_cqlsh(
            '''
            BEGIN BATCH
                INSERT INTO ks.test (key) VALUES ('eggs')
                INSERT INTO ks.test (key) VALUES ('sausage')
                INSERT INTO ks.test (key) VALUES ('spam')
            APPLY BATCH;
            ''')
        # make sure everything inserted is actually there
        assert_all(self.session, 'SELECT key FROM ks.test',
                   [[u'eggs'], [u'spam'], [u'sausage']])

    def test_create_keyspace(self):
        self.assertNotIn(u'created', self.get_keyspace_names())

        self.node1.run_cqlsh("CREATE KEYSPACE created WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        self.assertIn(u'created', self.get_keyspace_names())

    def test_drop_keyspace(self):
        self.create_ks(self.session, 'ks', 1)
        self.assertIn(u'ks', self.get_keyspace_names())

        self.node1.run_cqlsh('DROP KEYSPACE ks')

        self.assertNotIn(u'ks', self.get_keyspace_names())

    def test_create_table(self):
        self.create_ks(self.session, 'ks', 1)

        self.node1.run_cqlsh('CREATE TABLE ks.test (i int PRIMARY KEY);')
        self.assertEquals(self.get_tables_in_keyspace('ks'), [u'test'])

    def test_drop_table(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test')

        assert_none(self.session, 'SELECT key FROM test')

        self.node1.run_cqlsh('DROP TABLE ks.test;')
        self.session.cluster.refresh_schema_metadata()

        self.assertEqual(0, len(self.session.cluster.metadata.keyspaces['ks'].tables))

    def test_truncate(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', columns={'i': 'int'})

        self.session.execute("INSERT INTO test (key) VALUES ('a')")
        self.session.execute("INSERT INTO test (key) VALUES ('b')")
        self.session.execute("INSERT INTO test (key) VALUES ('c')")
        self.session.execute("INSERT INTO test (key) VALUES ('d')")
        self.session.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.session, 'SELECT key from test',
                   [[u'a'], [u'c'], [u'e'], [u'd'], [u'b']])

        self.node1.run_cqlsh('TRUNCATE ks.test;')
        self.assertEqual([], rows_to_list(self.session.execute('SELECT * from test')))

    def test_alter_table(self):
        self.create_ks(self.session, 'ks', 1, )
        self.create_cf(self.session, 'test', columns={'i': 'ascii'})

        def get_ks_columns():
            table = self.session.cluster.metadata.keyspaces['ks'].tables['test']

            return [[table.name, column.name, column.cql_type] for column in table.columns.values()]

        old_column_spec = [u'test', u'i',
                           u'ascii']
        self.assertIn(old_column_spec, get_ks_columns())

        self.node1.run_cqlsh('ALTER TABLE ks.test ALTER i TYPE text;')
        self.session.cluster.refresh_table_metadata("ks", "test")

        new_columns = get_ks_columns()
        self.assertNotIn(old_column_spec, new_columns)
        self.assertIn([u'test', u'i',
                       u'text'],
                      new_columns)

    def test_use_keyspace(self):
        # ks1 contains ks1table, ks2 contains ks2table
        self.create_ks(self.session, 'ks1', 1)
        self.create_cf(self.session, 'ks1table')
        self.create_ks(self.session, 'ks2', 1)
        self.create_cf(self.session, 'ks2table')

        ks1_stdout, ks1_stderr = self.node1.run_cqlsh(
            '''
            USE ks1;
            DESCRIBE TABLES;
            ''',
            return_output=True)
        self.assertEqual([x for x in ks1_stdout.split() if x], ['ks1table'])
        self.assertEqual(ks1_stderr, '')

        ks2_stdout, ks2_stderr = self.node1.run_cqlsh(
            '''
            USE ks2;
            DESCRIBE TABLES;
            ''',
            return_output=True)
        self.assertEqual([x for x in ks2_stdout.split() if x], ['ks2table'])
        self.assertEqual(ks2_stderr, '')

    # DROP INDEX statement fails in 2.0 (see CASSANDRA-9247)
    def test_drop_index(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', columns={'i': 'int'})

        # create a statement that will only work if there's an index on i
        requires_index = 'SELECT * from test WHERE i = 5'

        def execute_requires_index():
            return self.session.execute(requires_index)

        # make sure it fails as expected
        self.assertRaises(InvalidRequest, execute_requires_index)

        # make sure it doesn't fail when an index exists
        self.session.execute('CREATE INDEX index_to_drop ON test (i);')
        assert_none(self.session, requires_index)

        # drop the index via cqlsh, then make sure it fails
        self.node1.run_cqlsh('DROP INDEX ks.index_to_drop;')
        self.assertRaises(InvalidRequest, execute_requires_index)

    # DROP INDEX statement fails in 2.0 (see CASSANDRA-9247)
    def test_create_index(self):
        self.create_ks(self.session, 'ks', 1)
        self.create_cf(self.session, 'test', columns={'i': 'int'})

        # create a statement that will only work if there's an index on i
        requires_index = 'SELECT * from test WHERE i = 5;'

        def execute_requires_index():
            return self.session.execute(requires_index)

        # make sure it fails as expected
        self.assertRaises(InvalidRequest, execute_requires_index)

        # make sure index exists after creating via cqlsh
        self.node1.run_cqlsh('CREATE INDEX index_to_drop ON ks.test (i);')
        assert_none(self.session, requires_index)

        # drop the index, then make sure it fails again
        self.session.execute('DROP INDEX ks.index_to_drop;')
        self.assertRaises(InvalidRequest, execute_requires_index)

    def get_keyspace_names(self):
        self.session.cluster.refresh_schema_metadata()
        return [ks.name for ks in self.session.cluster.metadata.keyspaces.values()]

    def get_tables_in_keyspace(self, keyspace):
        self.session.cluster.refresh_schema_metadata()
        return [table.name for table in self.session.cluster.metadata.keyspaces[keyspace].tables.values()]


class CqlLoginTest(Tester):
    """
    Tests login which requires password authenticator
    """

    def setUp(self):
        super(CqlLoginTest, self).setUp()
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node1] = self.cluster.nodelist()
        self.node1.watch_log_for('Created default superuser')
        self.session = self.patient_cql_connection(self.node1, user='cassandra', password='cassandra')

    def assert_login_not_allowed(self, user, input):
        message = ("Provided username {user} and/or password are incorrect".format(user=user)
                   if LooseVersion(self.cluster.version()) >= LooseVersion('3.10')
                   else "Username and/or password are incorrect")

        self.assertEqual([message in x for x in input.split("\n") if x], [True])

    def test_login_keeps_keyspace(self):
        self.create_ks(self.session, 'ks1', 1)
        self.create_cf(self.session, 'ks1table')
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            '''
            USE ks1;
            DESCRIBE TABLES;
            LOGIN user1 'changeme';
            DESCRIBE TABLES;
            ''',
            return_output=True,
            cqlsh_options=['-u', 'cassandra', '-p', 'cassandra'])
        self.assertEqual([x for x in cqlsh_stdout.split() if x], ['ks1table', 'ks1table'])
        self.assertEqual(cqlsh_stderr, '')

    def test_login_rejects_bad_pass(self):
        self.create_ks(self.session, 'ks1', 1)
        self.create_cf(self.session, 'ks1table')
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            '''
            LOGIN user1 'badpass';
            ''',
            return_output=True,
            cqlsh_options=['-u', 'cassandra', '-p', 'cassandra'])

        self.assert_login_not_allowed('user1', cqlsh_stderr)

    def test_login_authenticates_correct_user(self):
        self.create_ks(self.session, 'ks1', 1)
        self.create_cf(self.session, 'ks1table')
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        if self.cluster.version() >= '2.2':
            query = '''
                    LOGIN user1 'changeme';
                    CREATE USER user2 WITH PASSWORD 'fail' SUPERUSER;
                    '''
            expected_error = "Only superusers can create a role with superuser status"
        else:
            query = '''
                    LOGIN user1 'changeme';
                    CREATE USER user2 WITH PASSWORD 'fail';
                    '''
            expected_error = 'Only superusers are allowed to perform CREATE USER queries'

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            query,
            return_output=True,
            cqlsh_options=['-u', 'cassandra', '-p', 'cassandra'])

        err_lines = cqlsh_stderr.splitlines()
        for err_line in err_lines:
            if expected_error in err_line:
                break
        else:
            self.fail("Did not find expected error '{}' in "
                      "cqlsh stderr output: {}".format(expected_error,
                                                       '\n'.join(err_lines)))

    def test_login_allows_bad_pass_and_continued_use(self):
        self.create_ks(self.session, 'ks1', 1)
        self.create_cf(self.session, 'ks1table')
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            '''
            LOGIN user1 'badpass';
            USE ks1;
            DESCRIBE TABLES;
            ''',
            return_output=True,
            cqlsh_options=['-u', 'cassandra', '-p', 'cassandra'])
        self.assertEqual([x for x in cqlsh_stdout.split() if x], ['ks1table'])
        self.assert_login_not_allowed('user1', cqlsh_stderr)
