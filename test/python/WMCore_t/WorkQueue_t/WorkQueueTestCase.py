"""
_File_t_

Unit tests for the WMBS File class.
"""

__revision__ = "$Id: WorkQueueTestCase.py,v 1.5 2009/10/13 23:06:08 meloam Exp $"
__version__ = "$Revision: 1.5 $"

import unittest
import logging
import os
import threading

from WMQuality.TestInit import TestInit
# pylint: disable-msg = W0611
import WMCore.WMLogging # needed to bring in logging.SQLDEBUG
# pylint: enable-msg = W0611

class WorkQueueTestCase(unittest.TestCase):

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


    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        selt.testInit.clearDatabase()
