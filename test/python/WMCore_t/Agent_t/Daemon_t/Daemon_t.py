#!/usr/bin/env python
"""
_Daemon_t_

Unit tests for  daemon creation

"""

__revision__ = "$Id: Daemon_t.py,v 1.5 2009/10/01 01:13:24 meloam Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "fvlingen@caltech.edu"

import commands
import unittest
import logging
import os
import threading
import time

from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.Daemon.Details import Details
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMFactory import WMFactory
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
        self.testInit.setSchema(['WMCore.Agent.Daemon'])


    def tearDown(self):
        """
        Deletion of the databases 
        """
        self.testInit.clearDatabase()

               
    def testA(self):
        """
        __testSubscribe__

        Test deamon creation
        """
        # keep the parent alive
        pid = createDaemon(os.getenv("TESTDIR"), True)
        if pid != 0 :
            print('Deamon created I am the parent')
            time.sleep(2)
            print('Going to destroy my daemon')
            details = Details(os.path.join(os.getenv("TESTDIR"),"Daemon.xml"))
            print('Found Deamon details (sleeping for 10 secs.)')
            print(str(details.isAlive()))
            time.sleep(10)
            details.killWithPrejudice()
            print('Daemon killed')
        else:
            while True:
                logging.debug('I am a deamon (wait 10 seconds)')
                time.sleep(1)
        DaemonTest._teardown = True

    def runTest(self):
        """
        Runs the tests.
        """
        self.testA()

if __name__ == "__main__":
    unittest.main()


