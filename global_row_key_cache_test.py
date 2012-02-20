import time
import logging
import types
import pprint
import hashlib


from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib import common as ccmcommon

from loadmaker import LoadMaker

import pycassa
import pycassa.system_manager as system_manager

# NOTE: with nosetests, use the --nologcapture flag to let logging get through.
# then set the logging level here.
# A logging-level of DEBUG will show the load being created.
logging.basicConfig(level=logging.INFO)
logging.info("Starting...")


class TestGlobalRowKeyCache(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestGlobalRowKeyCache, self).__init__(*argv, **kwargs)
        # When a node goes down under load it prints an error in it's log. 
        # If we don't allow log errors, then the test will fail.
#        self.allow_log_errors = True

    @since("1.1")
    def functional_test(self):
        """
        Test global caches.

        Test that save and load work in the situation when you write to
        different CFs. Read 2 or 3 times to make sure the page cache doesn't
        skew the results.
        """

        # create some rows to insert
        NUM_INSERTS = 1000
        NUM_UPDATES = 100
        NUM_DELETES = 10

        cluster = self.cluster
        cluster.populate(3)
        node1 = cluster.nodelist()[0]

        for kcsim in (0, 10):
            for rcsim in (0, 10):
                setup_name = "%d_%d" % (kcsim, rcsim)
                ks_name = 'ks_' + setup_name
                cf_name = 'cf_' + setup_name

                print "setup", setup_name
                cluster.set_configuration_options(values={
                        'key_cache_size_in_mb': kcsim,
                        'row_cache_size_in_mb': rcsim,
                        'row_cache_save_period': 5,
                        'key_cache_save_period': 5,
                        })
                cluster.start()
                time.sleep(.5)
                cursor = self.cql_connection(node1).cursor()
                self.create_ks(cursor, ks_name, 3)
                time.sleep(1) # wait for propagation

                # create some load makers
                lm_standard = LoadMaker(keyspace_name=ks_name, column_family_type='standard')
                lm_super = LoadMaker(keyspace_name=ks_name, column_family_type='super')
                lm_counter = LoadMaker(keyspace_name=ks_name, column_family_type='standard', is_counter=True)
                lm_counter_super = LoadMaker(keyspace_name=ks_name, column_family_type='super', is_counter=True)

                # insert some rows
                lm_standard.generate(NUM_INSERTS)
                lm_super.generate(NUM_INSERTS)
                lm_counter.generate(NUM_INSERTS)
                lm_counter_super.generate(NUM_INSERTS)

                # flush everything to get it into sstables
                for node in cluster.nodelist():
                    node.flush()

                print "Validating"
                for i in range(3):
                    # read and modify multiple times to get data into and invalidated out of the cache.
                    lm_standard.update(NUM_UPDATES).delete(NUM_DELETES).validate()
                    lm_super.update(NUM_UPDATES).delete(NUM_DELETES).validate()
                    lm_counter.generate().validate()
#                    lm_counter_super.generate().validate()



                lm_counter_super.validate()

                # flush everything to get it into sstables
#                for node in cluster.nodelist():
#                    node.flush()

                # let the data be written to the row/key caches.
                print "Letting caches be written"
                time.sleep(10)
                print "Stopping cluster"
                cluster.stop()
                time.sleep(1)
                print "Starting cluster"
                cluster.start()
                time.sleep(5) # read the data back from row and key caches

                lm_standard.set_server_list()
                lm_super.set_server_list()
                lm_counter.set_server_list()
                lm_counter_super.set_server_list()

                print "Validating again..."
                for i in range(2):
                    # read and modify multiple times to get data into and invalidated out of the cache.
                    lm_standard.validate()
                    lm_super.validate()
                    lm_counter.validate()
#                    lm_counter_super.validate()


                cluster.stop()


