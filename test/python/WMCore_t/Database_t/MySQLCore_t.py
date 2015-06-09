#!/usr/bin/env python
"""
_MySQLCore_t

Unit tests for the MySQL version of DBCore.
"""




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

    def testBindSubstitutionA(self):
        """
        _testBindSubstitutionA_

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

    def testBindSubstitutionB(self):
        """
        _testBindSubstitutionB_

        Test another query that has been causing problems.
        """
        sql = "INSERT INTO FILE_LUMIS (FILE_LUMI_ID, RUN_NUM, LUMI_SECTION_NUM, FILE_ID) VALUES (:file_lumi_id, :run_num, :lumi_section_num, :file_id)"
        binds = [{"lumi_section_num": "27414", "run_num": "1", "file_lumi_id": 1, "file_id": 1},
                 {"lumi_section_num": "26422", "run_num": "1", "file_lumi_id": 2, "file_id": 1},
                 {"lumi_section_num": "29838", "run_num": "1", "file_lumi_id": 3, "file_id": 1}]

        myInterface = MySQLInterface(logger = logging, engine = None)
        (updatedSQL, bindList) = myInterface.substitute(sql, binds)

        goodSQL = "INSERT INTO FILE_LUMIS (FILE_LUMI_ID, RUN_NUM, LUMI_SECTION_NUM, FILE_ID) VALUES (%s, %s, %s, %s)"

        assert updatedSQL == goodSQL, \
               "Error: SQL updated failed."

        assert len(bindList) == 3, \
               "Error: Wrong number of binds."
        assert bindList[0] == (1, '1', '27414', 1), \
               "Error: Bind 0 has wrong values."
        assert bindList[1] == (2, '1', '26422', 1), \
               "Error: Bind 1 has wrong values."
        assert bindList[2] == (3, '1', '29838', 1), \
               "Error: Bind 2 has wrong values."

        return

    def testBindSubstitutionCase(self):
        """
        _testBindSubstitutionCase_

        Verify that bind substitution works correctly when the bind variables in
        the query and the bind variables list have different case.
        """
        sql = "INSERT INTO FILE_LUMIS (FILE_LUMI_ID, RUN_NUM, LUMI_SECTION_NUM, FILE_ID) VALUES (:FILE_LUMI_ID, :RUN_NUM, :LUMI_SECTION_NUM, :FILE_ID)"
        binds = [{"lumi_section_num": "27414", "run_num": "1", "file_lumi_id": 1, "file_id": 1},
                 {"lumi_section_num": "26422", "run_num": "1", "file_lumi_id": 2, "file_id": 1},
                 {"lumi_section_num": "29838", "run_num": "1", "file_lumi_id": 3, "file_id": 1}]

        myInterface = MySQLInterface(logger = logging, engine = None)
        (updatedSQL, bindList) = myInterface.substitute(sql, binds)

        goodSQL = "INSERT INTO FILE_LUMIS (FILE_LUMI_ID, RUN_NUM, LUMI_SECTION_NUM, FILE_ID) VALUES (%s, %s, %s, %s)"

        assert updatedSQL == goodSQL, \
               "Error: SQL updated failed."

        assert len(bindList) == 3, \
               "Error: Wrong number of binds."
        assert bindList[0] == (1, '1', '27414', 1), \
               "Error: Bind 0 has wrong values."
        assert bindList[1] == (2, '1', '26422', 1), \
               "Error: Bind 1 has wrong values."
        assert bindList[2] == (3, '1', '29838', 1), \
               "Error: Bind 2 has wrong values."

        return

if __name__ == "__main__":
    unittest.main()
