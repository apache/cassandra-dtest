# -*- coding: utf-8 -*-
from dtest import Tester, debug
from cassandra import InvalidRequest
from ccmlib import common
import binascii
from cassandra.concurrent import execute_concurrent_with_args
import csv
import datetime
from decimal import Decimal
import os
import subprocess
import sys
from tempfile import NamedTemporaryFile
from uuid import UUID, uuid4
from distutils.version import LooseVersion
from tools import create_c1c2_table, insert_c1c2, since, rows_to_list
from assertions import assert_all, assert_none


class TestCqlsh(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def test_simple_insert(self):

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds = """
            CREATE KEYSPACE simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            use simple;
            create TABLE simple (id int PRIMARY KEY , value text ) ;
            insert into simple (id, value) VALUES (1, 'one');
            insert into simple (id, value) VALUES (2, 'two');
            insert into simple (id, value) VALUES (3, 'three');
            insert into simple (id, value) VALUES (4, 'four');
            insert into simple (id, value) VALUES (5, 'five')""")

        cursor = self.patient_cql_connection(node1)
        rows = cursor.execute("select id, value from simple.simple");

        self.assertEqual({1:'one', 2:'two', 3:'three', 4:'four', 5:'five'},
                         {k : v for k,v in rows})

    def test_eat_glass(self):

        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds = u"""create KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
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

        cursor = self.patient_cql_connection(node1)
        def verify_varcharmap(map_name, expected, encode_value=False):
            rows = cursor.execute((u"SELECT %s FROM testks.varcharmaptable WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';" % map_name).encode("utf-8"))
            if encode_value:
                got = {k.encode("utf-8"):v.encode("utf-8") for k,v in rows[0][0].iteritems()}
            else:
                got = {k.encode("utf-8"):v for k,v in rows[0][0].iteritems()}
            self.assertEqual(got, expected)

        verify_varcharmap('varcharasciimap', {
            'Vitrum edere possum, mihi non nocet.' : 'Hello',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : 'My',
            'Можам да јадам стакло, а не ме штета.' : 'Name',
            'I can eat glass and it does not hurt me' : 'Is'
        })

        verify_varcharmap('varcharbigintmap', {
            'Vitrum edere possum, mihi non nocet.' : 5100003,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : -45,
            'Можам да јадам стакло, а не ме штета.' : 12300,
            'I can eat glass and it does not hurt me' : 0
        })

        verify_varcharmap('varcharblobmap', {
            'Vitrum edere possum, mihi non nocet.' : binascii.a2b_hex("FEED103A"),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : binascii.a2b_hex("DEADBEEF"),
            'Можам да јадам стакло, а не ме штета.' : binascii.a2b_hex("BEEFBEEF"),
            'I can eat glass and it does not hurt me' : binascii.a2b_hex("FEEB")
        })


        verify_varcharmap('varcharbooleanmap', {
            'Vitrum edere possum, mihi non nocet.' : True,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : False,
            'Можам да јадам стакло, а не ме штета.' : False,
            'I can eat glass and it does not hurt me' : False
        })

        verify_varcharmap('varchardecimalmap', {
            'Vitrum edere possum, mihi non nocet.' : Decimal('50'),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : Decimal('-20.4'),
            'Можам да јадам стакло, а не ме штета.' : Decimal('11234234.3'),
            'I can eat glass and it does not hurt me' : Decimal('10.0')
        })

        verify_varcharmap('varchardoublemap', {
            'Vitrum edere possum, mihi non nocet.' : 4234243,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : -432.311,
            'Можам да јадам стакло, а не ме штета.' : 3.1415,
            'I can eat glass and it does not hurt me' : 20000.0
        })

        verify_varcharmap('varcharfloatmap', {
            'Vitrum edere possum, mihi non nocet.' : 10.0,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : -234.3000030517578,
            'Можам да јадам стакло, а не ме штета.' : -234234,
            'I can eat glass and it does not hurt me' : 1000.5
        })

        verify_varcharmap('varcharintmap', {
            'Vitrum edere possum, mihi non nocet.' : 1,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : 2,
            'Можам да јадам стакло, а не ме штета.' : -3,
            'I can eat glass and it does not hurt me' : -500
        })

        verify_varcharmap('varcharinetmap', {
            'Vitrum edere possum, mihi non nocet.' : '192.168.0.1',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : '127.0.0.1',
            'Можам да јадам стакло, а не ме штета.' : '8.8.8.8',
            'I can eat glass and it does not hurt me' : '8.8.4.4'
        })

        verify_varcharmap('varchartextmap', {
            'Vitrum edere possum, mihi non nocet.' : 'Once I went',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : 'On a trip',
            'Можам да јадам стакло, а не ме штета.' : 'Across',
            'I can eat glass and it does not hurt me' : 'The '
        })

        verify_varcharmap('varchartimestampmap', {
            'Vitrum edere possum, mihi non nocet.' : datetime.datetime(2013, 6, 19, 3, 21, 1),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : datetime.datetime(1985, 8, 3, 4, 21, 1),
            'Можам да јадам стакло, а не ме штета.' : datetime.datetime(2000, 1, 1, 0, 20, 1),
            'I can eat glass and it does not hurt me' : datetime.datetime(1942, 3, 11, 5, 21, 1)
        })

        verify_varcharmap('varcharuuidmap', {
            'Vitrum edere possum, mihi non nocet.' : UUID('7787064c-ce54-4324-abdd-05775b89ead7'),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : UUID('1df0b6ac-f3d3-456c-8b78-2bc70e585107'),
            'Можам да јадам стакло, а не ме штета.' : UUID('e2ed2164-31dc-42cb-8ee9-47376e071210'),
            'I can eat glass and it does not hurt me' : UUID('a487fe45-8af5-4454-ac66-2614286d7e89')
        })

        verify_varcharmap('varchartimeuuidmap', {
            'Vitrum edere possum, mihi non nocet.' : UUID('4a36c100-d8ec-11e2-a28f-0800200c9a66'),
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : UUID('670c7f90-d8ec-11e2-a28f-0800200c9a66'),
            'Можам да јадам стакло, а не ме штета.' : UUID('750c2d70-d8ec-11e2-a28f-0800200c9a66'),
            'I can eat glass and it does not hurt me' : UUID('80d74810-d8ec-11e2-a28f-0800200c9a66')
        })

        verify_varcharmap('varcharvarcharmap', {
            'Vitrum edere possum, mihi non nocet.' : '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑',
            'Можам да јадам стакло, а не ме штета.' : 'Можам да јадам стакло, а не ме штета.',
            'I can eat glass and it does not hurt me' : 'I can eat glass and it does not hurt me'
        }, encode_value=True)

        verify_varcharmap('varcharvarintmap', {
            'Vitrum edere possum, mihi non nocet.' : 1010010101020400204143243,
            ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑' : -40,
            'Можам да јадам стакло, а не ме штета.' : 110230,
            'I can eat glass and it does not hurt me' : 1400
        })

        output, err = self.run_cqlsh(node1, 'use testks; SELECT * FROM varcharmaptable')

        self.assertEquals(output.count('Можам да јадам стакло, а не ме штета.'), 16)
        self.assertEquals(output.count(' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑'), 16)
        self.assertEquals(output.count('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜'), 2)

    def test_with_empty_values(self):
        """
        CASSANDRA-7196. Make sure the server returns empty values and CQLSH prints them properly
        """
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        node1.run_cqlsh(cmds = u"""create keyspace  CASSANDRA_7196 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} ;

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

    @since('2.0')
    def tracing_from_system_traces_test(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)

        for n in xrange(100):
            insert_c1c2(session, n)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * FROM ks.cf')
        self.assertIn('Tracing session: ', out)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * FROM system_traces.events')
        self.assertNotIn('Tracing session: ', out)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * FROM system_traces.sessions')
        self.assertNotIn('Tracing session: ', out)

    @since('2.1')
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
        ##If this assertion fails check CASSANDRA-7891

    def verify_output(self, query, node, expected):
            output, err = self.run_cqlsh(node, query, ['-u', 'cassandra', '-p', 'cassandra'])
            if common.is_win():
                output = output.replace('\r', '')
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
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("SELECT * FROM testcopyto"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: %s' % (tempfile.name,))
        node1.run_cqlsh(cmds="COPY ks.testcopyto TO '%s'" % (tempfile.name,))

        # session
        with open(tempfile.name, 'r') as csvfile:
            row_count = 0
            csvreader = csv.reader(csvfile)
            for cql_row, csv_row in zip(results, csvreader):
                self.assertEquals(map(str, cql_row), csv_row)
                row_count += 1

            self.assertEquals(len(results), row_count)

        # import the CSV file with COPY FROM
        session.execute("TRUNCATE ks.testcopyto")
        node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '%s'" % (tempfile.name,))
        new_results = list(session.execute("SELECT * FROM testcopyto"))
        self.assertEquals(results, new_results)

    @since('2.1')
    def test_float_formatting(self):
        """ Tests for CASSANDRA-9224, check format of float and double values"""
        self.cluster.populate(1)
        self.cluster.start(wait_for_binary_proto=True)

        node1, = self.cluster.nodelist()

        stdout, stderr = self.run_cqlsh(node1,cmds = """
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

        stdout, stderr = self.run_cqlsh(node1,cmds = """
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

        stdout, stderr = self.run_cqlsh(node1,cmds = """
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

    def run_cqlsh(self, node, cmds, cqlsh_options=[]):
        cdir = node.get_install_dir()
        cli = os.path.join(cdir, 'bin', common.platform_binary('cqlsh'))
        env = common.make_cassandra_env(cdir, node.get_path())
        env['LANG'] = 'en_US.UTF-8'
        if LooseVersion(self.cluster.version()) >= LooseVersion('2.1'):
            host = node.network_interfaces['binary'][0]
            port = node.network_interfaces['binary'][1]
        else:
            host = node.network_interfaces['thrift'][0]
            port = node.network_interfaces['thrift'][1]
        args = cqlsh_options + [ host, str(port) ]
        sys.stdout.flush()
        p = subprocess.Popen([ cli ] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        for cmd in cmds.split(';'):
            p.stdin.write(cmd + ';\n')
        p.stdin.write("quit;\n")
        return p.communicate()


class CqlshSmokeTest(Tester):
    '''
    Tests simple use cases for clqsh.
    '''
    def setUp(self):
        super(CqlshSmokeTest, self).setUp()
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node1] = self.cluster.nodelist()
        self.cursor = self.patient_cql_connection(self.node1)

    def test_uuid(self):
        """
        the `uuid()` function can generate UUIDs from cqlsh.
        """
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', key_type='uuid', columns={'i': 'int'})

        out, err = self.node1.run_cqlsh("INSERT INTO ks.test (key) VALUES (uuid())",
                                        return_output=True)
        self.assertEqual(err, "")

        result = self.cursor.execute("SELECT key FROM ks.test")
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 1)
        self.assertIsInstance(result[0][0], UUID)

        out, err = self.node1.run_cqlsh("INSERT INTO ks.test (key) VALUES (uuid())",
                                        return_output=True)
        self.assertEqual(err, "")

        result = self.cursor.execute("SELECT key FROM ks.test")
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)
        self.assertIsInstance(result[0][0], UUID)
        self.assertIsInstance(result[1][0], UUID)
        self.assertNotEqual(result[0][0], result[1][0])

    def test_commented_lines(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test')

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
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})

        self.node1.run_cqlsh(
            """
            INSERT INTO ks.test (key) VALUES ('Cassandra:TheMovie');
            """)
        assert_all(self.cursor, "SELECT key FROM test",
                   [[u'Cassandra:TheMovie']])

    def test_select(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test')

        self.cursor.execute("INSERT INTO ks.test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.cursor, "SELECT key, c, v FROM test",
                   [[u'a', u'a', u'a']])

        out, err = self.node1.run_cqlsh("SELECT key, c, v FROM ks.test",
                                        return_output=True)
        out_lines = [x.strip() for x in out.split("\n")]

        # there should be only 1 row returned & it should contain the inserted values
        self.assertIn("(1 rows)", out_lines)
        self.assertIn("a | a | a", out_lines)
        self.assertEqual(err, '')

    def test_insert(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test')

        self.node1.run_cqlsh("INSERT INTO ks.test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.cursor, "SELECT key, c, v FROM test", [[u"a", u"a", u"a"]])

    def test_update(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test')

        self.cursor.execute("INSERT INTO test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.cursor, "SELECT key, c, v FROM test", [[u"a", u"a", u"a"]])
        self.node1.run_cqlsh("UPDATE ks.test SET v = 'b' WHERE key = 'a' AND c = 'a'")
        assert_all(self.cursor, "SELECT key, c, v FROM test", [[u"a", u"a", u"b"]])

    def test_delete(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})

        self.cursor.execute("INSERT INTO test (key) VALUES ('a')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('b')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('c')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('d')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.cursor, 'SELECT key from test',
                   [[u'a'], [u'c'], [u'e'], [u'd'], [u'b']])

        self.node1.run_cqlsh("DELETE FROM ks.test WHERE key = 'c'")

        assert_all(self.cursor, 'SELECT key from test',
                   [[u'a'], [u'e'], [u'd'], [u'b']])

    def test_batch(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})
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
        assert_all(self.cursor, 'SELECT key FROM ks.test',
                   [[u'eggs'], [u'spam'], [u'sausage']])

    def test_create_keyspace(self):
        self.assertNotIn(u'created', self.get_keyspace_names())

        self.node1.run_cqlsh("CREATE KEYSPACE created WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        self.assertIn(u'created', self.get_keyspace_names())

    def test_drop_keyspace(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.assertIn(u'ks', self.get_keyspace_names())

        self.node1.run_cqlsh('DROP KEYSPACE ks')

        self.assertNotIn(u'ks', self.get_keyspace_names())

    def test_create_table(self):
        self.create_ks(self.cursor, 'ks', 1)

        self.node1.run_cqlsh('CREATE TABLE ks.test (i int PRIMARY KEY);')
        self.assertEquals(self.get_tables_in_keyspace('ks'), [u'test'])

    def test_drop_table(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test')

        assert_none(self.cursor, 'SELECT key FROM test')

        self.node1.run_cqlsh('DROP TABLE ks.test;')
        result = rows_to_list(self.cursor.execute("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='ks';"))
        self.assertEqual([], result)

    def test_truncate(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})

        self.cursor.execute("INSERT INTO test (key) VALUES ('a')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('b')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('c')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('d')")
        self.cursor.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.cursor, 'SELECT key from test',
                   [[u'a'], [u'c'], [u'e'], [u'd'], [u'b']])

        self.node1.run_cqlsh('TRUNCATE ks.test;')
        self.assertEqual([], rows_to_list(self.cursor.execute('SELECT * from test')))

    def test_alter_table(self):
        self.create_ks(self.cursor, 'ks', 1, )
        self.create_cf(self.cursor, 'test', columns={'i': 'ascii'})

        def get_ks_columns():
            return rows_to_list(self.cursor.execute(
                "SELECT columnfamily_name, column_name, validator FROM system.schema_columns WHERE keyspace_name='ks';"
            ))

        old_column_spec = [u'test', u'i',
                           u'org.apache.cassandra.db.marshal.AsciiType']
        self.assertIn(old_column_spec, get_ks_columns())

        self.node1.run_cqlsh('ALTER TABLE ks.test ALTER i TYPE text;')

        new_columns = get_ks_columns()
        self.assertNotIn(old_column_spec, new_columns)
        self.assertIn([u'test', u'i',
                       u'org.apache.cassandra.db.marshal.UTF8Type'],
                      new_columns)

    def test_use_keyspace(self):
        # ks1 contains ks1table, ks2 contains ks2table
        self.create_ks(self.cursor, 'ks1', 1)
        self.create_cf(self.cursor, 'ks1table')
        self.create_ks(self.cursor, 'ks2', 1)
        self.create_cf(self.cursor, 'ks2table')

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
    @since('2.1')
    def test_drop_index(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})

        # create a statement that will only work if there's an index on i
        requires_index = 'SELECT * from test WHERE i = 5'

        def execute_requires_index():
            return self.cursor.execute(requires_index)

        # make sure it fails as expected
        self.assertRaises(InvalidRequest, execute_requires_index)

        # make sure it doesn't fail when an index exists
        self.cursor.execute('CREATE INDEX index_to_drop ON test (i);')
        assert_none(self.cursor, requires_index)

        # drop the index via cqlsh, then make sure it fails
        self.node1.run_cqlsh('DROP INDEX ks.index_to_drop;')
        self.assertRaises(InvalidRequest, execute_requires_index)

    # DROP INDEX statement fails in 2.0 (see CASSANDRA-9247)
    @since('2.1')
    def test_create_index(self):
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})

        # create a statement that will only work if there's an index on i
        requires_index = 'SELECT * from test WHERE i = 5;'

        def execute_requires_index():
            return self.cursor.execute(requires_index)

        # make sure it fails as expected
        self.assertRaises(InvalidRequest, execute_requires_index)

        # make sure index exists after creating via cqlsh
        self.node1.run_cqlsh('CREATE INDEX index_to_drop ON ks.test (i);')
        assert_none(self.cursor, requires_index)

        # drop the index, then make sure it fails again
        self.cursor.execute('DROP INDEX ks.index_to_drop;')
        self.assertRaises(InvalidRequest, execute_requires_index)

    def get_keyspace_names(self):
        return [x[0] for x in
                rows_to_list(self.cursor.execute(
                    'SELECT keyspace_name from system.schema_keyspaces'))]

    def get_tables_in_keyspace(self, keyspace=None):
        cmd = "SELECT columnfamily_name FROM system.schema_columnfamilies"
        cmd += " WHERE keyspace_name='{keyspace}'".format(keyspace=keyspace) if keyspace else ""
        cmd += ";"

        return [x[0] for x in rows_to_list(self.cursor.execute(cmd))]

class CqlLoginTest(Tester):
    '''
    Tests login which requires password authenticator
    '''
    def setUp(self):
        super(CqlLoginTest, self).setUp()
        config = {'authenticator' : 'org.apache.cassandra.auth.PasswordAuthenticator'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node1] = self.cluster.nodelist()
        self.node1.watch_log_for('Created default superuser')
        self.cursor = self.patient_cql_connection(self.node1, user='cassandra', password='cassandra')

    def test_login_keeps_keyspace(self):
        self.create_ks(self.cursor, 'ks1', 1)
        self.create_cf(self.cursor, 'ks1table')
        self.cursor.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

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
        self.create_ks(self.cursor, 'ks1', 1)
        self.create_cf(self.cursor, 'ks1table')
        self.cursor.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            '''
            LOGIN user1 'badpass';
            ''',
            return_output=True,
            cqlsh_options=['-u', 'cassandra', '-p', 'cassandra'])
        self.assertEqual(['''Username and/or password are incorrect''' in x for x in cqlsh_stderr.split("\n") if x], [True])

    def test_login_authenticates_correct_user(self):
        self.create_ks(self.cursor, 'ks1', 1)
        self.create_cf(self.cursor, 'ks1table')
        self.cursor.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

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
            self.fail("Did not find expected error '%s' in cqlsh stderr output: %s" %
                      expected_error, '\n'.join(err_lines))

    def test_login_allows_bad_pass_and_continued_use(self):
        self.create_ks(self.cursor, 'ks1', 1)
        self.create_cf(self.cursor, 'ks1table')
        self.cursor.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            '''
            LOGIN user1 'badpass';
            USE ks1;
            DESCRIBE TABLES;
            ''',
            return_output=True,
            cqlsh_options=['-u', 'cassandra', '-p', 'cassandra'])
        self.assertEqual([x for x in cqlsh_stdout.split() if x], ['ks1table'])
        self.assertEqual(['''Username and/or password are incorrect''' in x for x in cqlsh_stderr.split("\n") if x], [True])
