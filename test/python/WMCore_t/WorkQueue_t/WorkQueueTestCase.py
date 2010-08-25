"""
_File_t_

Unit tests for the WMBS File class.
"""

__revision__ = "$Id: WorkQueueTestCase.py,v 1.2 2009/07/02 18:30:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import logging
import os
import commands
import threading
import random
from sets import Set

from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class WorkQueueTestCase(unittest.TestCase):
    _setup = False
    _teardown = False
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """
        if self._setup:
            return

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"], useDefault = False)
        self.testInit.setSchema(customModules = ["WMCore.WorkQueue.Database"], useDefault = False)

        myThread = threading.currentThread()
                
        self._setup = True
        return
          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
        
        myThread.transaction.begin()
        
        self.testInit.clearDatabase(modules = ["WMCore.WorkQueue.Database"])
        self.testInit.clearDatabase(modules = ["WMCore.WMBS"])
        self._teardown = True
