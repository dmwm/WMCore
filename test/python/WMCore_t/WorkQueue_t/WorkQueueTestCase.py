"""
_File_t_

Unit tests for the WMBS File class.
"""

__revision__ = "$Id: WorkQueueTestCase.py,v 1.9 2010/04/13 18:49:25 sryu Exp $"
__version__ = "$Revision: 1.9 $"

import unittest
from WMQuality.TestInit import TestInit
# pylint: disable-msg = W0611

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
        self.workDir = self.testInit.generateWorkDir()

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        
    