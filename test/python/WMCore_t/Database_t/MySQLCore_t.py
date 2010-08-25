#!/usr/bin/env python
"""
_MySQLCore_t

Unit tests for the MySQL version of DBCore.
"""

__revision__ = "$Id: MySQLCore_t.py,v 1.1 2010/02/12 21:01:08 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import logging

from WMCore.Database.MySQLCore import MySQLInterface
from WMQuality.TestInit import TestInit

class DBCoreTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup logging.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        return
            
    def tearDown(self):
        """
        _tearDown_

        Nothing to do.
        """
        return

    def testBindSubstitution(self):
        """
        _testBindSubstitution_

        Verify that bind substition works correctly with similarly named bind
        variables.
        """
        sql = "INSERT INTO RELEASE_VERSIONS (RELEASE_VERSION_ID, RELEASE_VERSION) VALUES (:release_version_id, :release_version)"
        binds = [{"release_version_id": 1, "release_version": "CMSSW_12_1_8"}]

        myInterface = MySQLInterface(logger = logging, engine = None)
        (updatedSQL, bindList) = myInterface.substitute(sql, binds)

        goodSQL = "INSERT INTO RELEASE_VERSIONS (RELEASE_VERSION_ID, RELEASE_VERSION) VALUES (%s, %s)"
        assert goodSQL == updatedSQL, \
               "Error: SQL is different."

        assert len(bindList) == 1, \
               "Error: bindList is malformed."
        assert len(bindList[0]) == 2, \
               "Error: bind list is the wrong length."
        assert bindList[0][0] == 1, \
               "Error: First bind parameter is wrong."
        assert bindList[0][1] == "CMSSW_12_1_8", \
               "Error: Second bind parameter is wrong."
               
        return

if __name__ == "__main__":
    unittest.main()     
