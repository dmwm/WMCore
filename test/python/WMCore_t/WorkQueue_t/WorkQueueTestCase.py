"""
_File_t_

Unit tests for the WMBS File class.
"""

__revision__ = "$Id: WorkQueueTestCase.py,v 1.8 2010/04/07 15:53:10 sryu Exp $"
__version__ = "$Revision: 1.8 $"

import unittest
import logging
import os
import threading
import tempfile

from WMCore.Configuration import Configuration, saveConfigurationFile
from WMQuality.TestInit import TestInit
# pylint: disable-msg = W0611
import WMCore.WMLogging # needed to bring in logging.SQLDEBUG
# pylint: enable-msg = W0611
from WMQuality.Emulators.EmulatorUnitTestBase import EmulatorUnitTestBase

class WorkQueueTestCase(EmulatorUnitTestBase):

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setSchema(customModules = ["WMCore.WorkQueue.Database"],
                                useDefault = False)
        self.workDir = self.testInit.generateWorkDir()
        EmulatorUnitTestBase.setUp(self)

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        
        EmulatorUnitTestBase.tearDown(self)
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        
    