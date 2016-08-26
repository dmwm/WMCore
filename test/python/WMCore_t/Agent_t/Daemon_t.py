#!/usr/bin/env python
"""
_Daemon_t_

Unit tests for  daemon creation.
"""

import os
import shutil
import tempfile
import time
import unittest

from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.Daemon.Details import Details
from WMQuality.TestInit import TestInit


class DaemonTest(unittest.TestCase):
    """
    _Daemon_t_

    Unit tests for message services: subscription, priority subscription, buffers,
    etc..

    """

    # minimum number of messages that need to be in queue
    _minMsg = 20
    # number of publish and gets from queue
    _publishAndGet = 10

    def setUp(self):
        "make a logger instance and create tables"
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.tempDir = tempfile.mkdtemp()

    def tearDown(self):
        """
        Deletion of the databases
        """
        self.testInit.clearDatabase()
        shutil.rmtree(self.tempDir, True)

    def testA(self):
        """
        __testSubscribe__

        Test daemon creation
        """
        # keep the parent alive
        self.pid = createDaemon(self.tempDir, True)
        try:
            try:
                if self.pid != 0:
                    time.sleep(2)
                    details = Details(os.path.join(self.tempDir, "Daemon.xml"))
                    time.sleep(10)
                    details.killWithPrejudice()
                else:
                    while True:
                        time.sleep(1)
            except:
                pass
        finally:
            if self.pid == 0:
                os._exit(-1)
            else:
                os.system('kill -9 %s' % self.pid)


if __name__ == "__main__":
    unittest.main()
