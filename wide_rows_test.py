from dtest import Tester, debug
import os
import datetime
import random

status_messages = (
    "I''m going to the Cassandra Summit in June!",
    "C* is awesome!",
    "All your sstables are belong to us.",
    "Just turned on another 50 C* nodes at <insert tech startup here>, scales beautifully.",
    "Oh, look! Cats, on reddit!",
    "Netflix recommendations are really good, wonder why?",
    "Spotify playlists are always giving me good tunes, wonder why?"
)

clients = (
    "Android",
    "iThing",
    "Chromium",
    "Mozilla",
    "Emacs"
    )

class TestWideRows(Tester):

    def __init__(self, *args, **kwargs):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = 'git:cassandra-1.2'
        Tester.__init__(self, *args, **kwargs)
    
    # def test_cassandra_1_2_1(self):
    #     self.write_wide_rows('git:cassandra-1.2.1')

    def test_cassandra_1_2_head(self):
        self.write_wide_rows('git:cassandra-1.2')

    def write_wide_rows(self, version):
        cluster = self.cluster
        self.cluster.set_cassandra_dir(cassandra_version=version)
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        start_time = datetime.datetime.now()
        self.create_ks(cursor, 'wide_rows', 1)
        # Simple timeline:  user -> {date: value, ...}
        debug('Create Table....')
        cursor.execute('CREATE TABLE user_events (userid text, event timestamp, value text, PRIMARY KEY (userid, event));', 1)
        date = datetime.datetime.now()
        # Create a large timeline for each of a group of users:
        for user in ('ryan', 'cathy', 'mallen', 'joaquin', 'erin', 'ham'): 
            debug("Writing values for: %s" % user)
            for day in xrange(5000):
                date_str = (date + datetime.timedelta(day)).strftime("%Y-%m-%d")
                client = random.choice(clients)
                msg = random.choice(status_messages)
                query = "UPDATE user_events SET value = '{msg:%s, client:%s}' WHERE userid='%s' and event='%s';" \
                               % (msg, client, user, date_str)
                #debug(query)
                cursor.execute(query, 1)

        #debug('Duration of test: %s' % (datetime.datetime.now() - start_time))

        # Pick out an update for a specific date:
        query = "SELECT value FROM user_events WHERE userid='ryan' and event='%s'" % \
                (date + datetime.timedelta(10)).strftime("%Y-%m-%d")
        cursor.execute(query, 1)
        for value in cursor:
            debug(value)
            assert len(value[0]) > 0
            
