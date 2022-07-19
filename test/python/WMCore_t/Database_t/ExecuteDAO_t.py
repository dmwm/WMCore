#!/usr/bin/env python
"""
Unit tests for ExecuteDAO function(s)
"""

import unittest

from WMCore.Database.ExecuteDAO import ExecuteDAO, loggerSetup


class ExecuteDAOTest(unittest.TestCase):
    """
    unittest for ExecuteDAO functions
    """

    def setUp(self):
        self.maxDiff = None
        self.logger = loggerSetup()
        self.package = "WMCore.WMBS"
        self.module = "Workflow.GetDeletableWorkflows"
        self.daoObject = ExecuteDAO(logger=self.logger,
                                    package=self.package, daoModule=self.module)
        self.sqlQueriesResult = {}
        self.sqlQueriesExpected = {'compltetedWFs': """ SELECT name FROM wmbs_workflow
                            WHERE name NOT IN (
                                SELECT DISTINCT ww.name FROM wmbs_workflow ww
                                      INNER JOIN wmbs_subscription ws
                                         ON ws.workflow = ww.id
                                WHERE ws.finished =0)
                      """,
        'wfsWithIncompletedChildWFs': """ SELECT DISTINCT ww.name FROM wmbs_workflow ww
                                  INNER JOIN wmbs_subscription ws
                                      ON ws.workflow = ww.id
                                  INNER JOIN wmbs_fileset wfs ON
                                     wfs.id = ws.fileset
                                  INNER JOIN wmbs_fileset_files wfsf ON
                                     wfsf.fileset = wfs.id
                                  INNER JOIN wmbs_file_parent wfp ON
                                     wfp.parent = wfsf.fileid
                                  INNER JOIN wmbs_fileset_files child_fileset ON
                                     child_fileset.fileid = wfp.child
                                  INNER JOIN wmbs_subscription child_subscription ON
                                     child_subscription.fileset = child_fileset.fileset
                                  WHERE child_subscription.finished = 0
                            """,
        'sql': """SELECT DISTINCT wmbs_workflow.name, wmbs_workflow.spec,
                        wmbs_workflow.id AS workflow_id, wmbs_subscription.id AS sub_id
                 FROM wmbs_subscription
                     INNER JOIN wmbs_workflow ON
                         wmbs_workflow.id = wmbs_subscription.workflow
                     INNER JOIN ( SELECT name FROM wmbs_workflow
                            WHERE name NOT IN (
                                SELECT DISTINCT ww.name FROM wmbs_workflow ww
                                      INNER JOIN wmbs_subscription ws
                                         ON ws.workflow = ww.id
                                WHERE ws.finished =0)
                      ) complete_workflow ON
                         complete_workflow.name = wmbs_workflow.name
                 WHERE wmbs_workflow.name NOT IN ( SELECT DISTINCT ww.name FROM wmbs_workflow ww
                                  INNER JOIN wmbs_subscription ws
                                      ON ws.workflow = ww.id
                                  INNER JOIN wmbs_fileset wfs ON
                                     wfs.id = ws.fileset
                                  INNER JOIN wmbs_fileset_files wfsf ON
                                     wfsf.fileset = wfs.id
                                  INNER JOIN wmbs_file_parent wfp ON
                                     wfp.parent = wfsf.fileid
                                  INNER JOIN wmbs_fileset_files child_fileset ON
                                     child_fileset.fileid = wfp.child
                                  INNER JOIN wmbs_subscription child_subscription ON
                                     child_subscription.fileset = child_fileset.fileset
                                  WHERE child_subscription.finished = 0
                            )
              """
        }

        super(ExecuteDAOTest, self).setUp()

    def testGetSqlQuery(self):
        """
        Test the getsqlQuery method from the ExeccuteDAO class
        Tests the dictionary with all sql queries found in the DAO
        """
        self.sqlQueriesResult = self.daoObject.getSqlQuery()
        self.assertDictEqual(self.sqlQueriesResult, self.sqlQueriesExpected)

    def testDryRun(self):
        """
        Test execute in dryRun Mode
        """
        self.assertListEqual(self.daoObject(dryRun=True), [])


if __name__ == '__main__':
    unittest.main()
