#!/usr/bin/env python
"""
_Daemon_t_

Unit tests for  daemon creation

"""

__revision__ = "$Id: Daemon_t.py,v 1.4 2009/02/09 21:00:15 fvlingen Exp $"
__version__ = "$Revision: 1.4 $"
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

    _setup = False
    _teardown = False

    # minimum number of messages that need to be in queue
    _minMsg = 20
    # number of publish and gets from queue
    _publishAndGet = 10

    def setUp(self):
        "make a logger instance and create tables"
       
        if not DaemonTest._setup: 
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema(['WMCore.Agent.Daemon'])
            DaemonTest._setup = True

    def tearDown(self):
        """
        Deletion of the databases 
        """
        myThread = threading.currentThread()
        if DaemonTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))

        DaemonTest._teardown = False

               
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


