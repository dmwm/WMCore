#!/usr/bin/env python
"""
_Daemon_t_

Unit tests for  daemon creation

"""

__revision__ = "$Id: Daemon_t.py,v 1.10 2010/02/05 14:16:13 meloam Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "fvlingen@caltech.edu"

import commands
import unittest
import logging
import os
import threading
import time
import shutil
import tempfile

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
        self.tempDir = tempfile.mkdtemp()


    def tearDown(self):
        """
        Deletion of the databases 
        """
        self.testInit.clearDatabase()
        shutil.rmtree( self.tempDir, True )

               
    def testA(self):
        """
        __testSubscribe__

        Test daemon creation
        """
        # keep the parent alive
        self.pid = createDaemon(self.tempDir, True)
        try:
            try:
                if self.pid != 0 :
                    print('Deamon created I am the parent')
                    time.sleep(2)
                    print('Going to destroy my daemon')
                    details = Details(os.path.join(self.tempDir,"Daemon.xml"))
                    print('Found Deamon details (sleeping for 10 secs.)')
                    print(str(details.isAlive()))
                    time.sleep(10)
                    details.killWithPrejudice()
                    print('Daemon killed')
                else:
                    while True:
                        logging.debug('I am a daemon (wait 10 seconds)')
                        time.sleep(1)
            except:
                pass
        finally:
            if self.pid == 0:
                os._exit(-1)
            else:
                os.system(['kill', '-9', self.pid])

if __name__ == "__main__":
    unittest.main()


