#!/usr/bin/env python
"""
_DBSBufferUtil_t_

Unit tests for DBSBufferUtil class
"""

from builtins import range
import unittest
import threading

from WMCore.DAOFactory import DAOFactory
from WMQuality.TestInit import TestInit
from WMCore.WMBS.Workflow import Workflow
from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile

class DBSBufferUtilTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Create some DBSBuffer tables and fake data for testing
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMComponent.DBS3Buffer", "WMCore.WMBS"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.dbsbufferFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.wmbsFactory = DAOFactory(package="WMCore.WMBS",
                                           logger=myThread.logger,
                                           dbinterface=myThread.dbi)
        self.dbsUtil = DBSBufferUtil()

        # Create two test dbsbuffer workflows
        insertWorkflow = self.dbsbufferFactory(classname="InsertWorkflow")
        insertWorkflow.execute("Test1", "Task1", 0, 0, 0, 0)
        insertWorkflow.execute("Test2", "Task2", 0, 0, 0, 0)

        # Update one workflow to "completed" state
        updateWorkflow = self.dbsbufferFactory(classname="UpdateWorkflowsToCompleted")
        updateWorkflow.execute(["Test1"])

        # Create a test wmbs workflow
        testWorkflow = Workflow(spec="somespec.xml", owner="Erik", name="Test1", task="Task1")
        testWorkflow.create()

        # Create a test dbsbuffer file
        self.createTestFiles()

    def createTestFiles(self):
        """
        _createTestFiles_

        Create some dbsbuffer test files with different statuses
        :return:
        """
        phedexStatus = self.dbsbufferFactory(classname="DBSBufferFiles.SetPhEDExStatus")

        for i in range(0, 4):

            lfn = "/path/to/some/lfn" + str(i)

            # Two files should be InDBS, two files should be NOTUPLOADED
            if i in [0,2]:
                status = 'InDBS'
            else:
                status = 'NOTUPLOADED'

            testDBSFile = DBSBufferFile(lfn=lfn, size=600000, events=60000, status=status, workflowId=1)

            testDBSFile.setAlgorithm(appName="cmsRun", appVer="UNKNOWN",
                                     appFam="RECO", psetHash="SOMEHASH" + str(i),
                                     configContent="SOMECONTENT")
            testDBSFile.setDatasetPath("/path/to/some/dataset")
            testDBSFile.create()

            # Create all four combinations of status(InDBS,NOTUPLOADED) and in_phedex(0,1)
            if i in [0,1]:
                phedexStatus.execute(lfn, 1)

    def tearDown(self):
        """
        _tearDown_

        Drop all the DBSBuffer tables.
        """
        self.testInit.clearDatabase()

    # List of methods to potentially test
    # def loadDBSBufferFilesBulk(self, fileObjs):
    # def findUploadableDAS(self):
    # def testFindOpenBlocks(self):
    # def loadBlocksByDAS(self, das):
    #
    # def loadBlocks(self, blocknames):
    #
    # def findUploadableFilesByDAS(self, datasetpath):
    #
    # def loadFilesByBlock(self, blockname):

    def testGetPhEDExDBSStatusForCompletedWorkflows(self):
        """
        _testGetPhEDExDBSStatusForCompletedWorkflows_

        :return:
        """

        results = self.dbsUtil.getPhEDExDBSStatusForCompletedWorkflows()
        self.assertEqual(results["Test1"]["InDBS"], 2, "ERROR: Files with InDBS status is incorrect.")
        self.assertEqual(results["Test1"]["InPhEDEx"], 2, "ERROR: Files with InPhEDEx status is incorrect.")
        self.assertEqual(results["Test1"]["NotInDBS"], 2, "ERROR: Files with NotInDBS status is incorrect.")
        self.assertEqual(results["Test1"]["NotInPhEDEx"], 2, "ERROR: Files with NotInPhEDEx status is incorrect.")

        return

    def testGetCompletedWorkflows(self):
        """
        _testGetCompletedWorkflows_

        :return:
        """

        results = self.dbsUtil.getCompletedWorkflows()
        self.assertEqual(len(results), 1, "ERROR: GetCompletedWorkflows returned incorrect number of completed workflows.")
        self.assertIn("Test1", results, "ERROR: GetCompletedWorkflows returned incorrect workflow.")

        return


if __name__ == "__main__":
    unittest.main()
