import random
import time
from threading import Thread

from dtest import debug


class InterruptBootstrap(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("Prepare completed")
        self.node.stop(gently=False)


class InterruptCompaction(Thread):
    """
    Interrupt compaction by killing a node as soon as
    the "Compacting" string is found in the log file
    for the table specified. This requires debug level
    logging in 2.1+ and expects debug information to be
    available in a file called "debug.log" unless a
    different name is passed in as a paramter.
    """

    def __init__(self, node, tablename, filename='debug.log', delay=0):
        Thread.__init__(self)
        self.node = node
        self.tablename = tablename
        self.filename = filename
        self.delay = delay
        self.mark = node.mark_log(filename=self.filename)

    def run(self):
        self.node.watch_log_for("Compacting(.*)%s" % (self.tablename,), from_mark=self.mark, filename=self.filename)
        if self.delay > 0:
            random_delay = random.uniform(0, self.delay)
            debug("Sleeping for {} seconds".format(random_delay))
            time.sleep(random_delay)
        debug("Killing node {}".format(self.node.address()))
        self.node.stop(gently=False)


class KillOnBootstrap(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("JOINING: Starting to bootstrap")
        self.node.stop(gently=False)
