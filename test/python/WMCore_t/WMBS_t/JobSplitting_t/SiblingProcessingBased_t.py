#!/usr/bin/env python
"""
_SiblingProcessingBased_t_

Test SiblingProcessing job splitting.
"""

__revision__ = "$Id: SiblingProcessingBased_t.py,v 1.1 2010/04/22 15:42:39 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMQuality.TestInit import TestInit

class SiblingProcessingBasedTest(unittest.TestCase):
    """
    _SiblingProcessingBasedTest_

    Test SiblingProcessing job splitting.
    """
    def setUp(self):
        """
        _setUp_

        Setup the database connections and schema.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        self.testFilesetA = Fileset(name = "FilesetA")
        self.testFilesetA.create()
        self.testFilesetB = Fileset(name = "FilesetB")
        self.testFilesetB.create()

        self.testFileA = File("testFileA", size = 1000, events = 100,
                              locations = set(["somese.cern.ch"]))
        self.testFileA.create()
        self.testFileB = File("testFileB", size = 1000, events = 100,
                              locations = set(["somese.cern.ch"]))
        self.testFileB.create()
        self.testFileC = File("testFileC", size = 1000, events = 100,
                              locations = set(["somese.cern.ch"]))
        self.testFileC.create()        

        self.testFilesetA.addFile(self.testFileA)
        self.testFilesetA.addFile(self.testFileB)
        self.testFilesetA.addFile(self.testFileC)        
        self.testFilesetA.commit()

        self.testFileD = File("testFileD", size = 1000, events = 100,
                              locations = set(["somese.cern.ch"]))
        self.testFileD.create()
        self.testFileE = File("testFileE", size = 1000, events = 100,
                              locations = set(["somese.cern.ch"]))
        self.testFileE.create()
        self.testFileF = File("testFileF", size = 1000, events = 100,
                              locations = set(["somese.cern.ch"]))
        self.testFileF.create()        

        self.testFilesetB.addFile(self.testFileD)
        self.testFilesetB.addFile(self.testFileE)
        self.testFilesetB.addFile(self.testFileF)
        self.testFilesetB.commit()

        testWorkflowA = Workflow(spec = "specA.xml", owner = "Steve",
                                 name = "wfA", task = "Test")
        testWorkflowA.create()
        testWorkflowB = Workflow(spec = "specB.xml", owner = "Steve",
                                 name = "wfB", task = "Test")
        testWorkflowB.create()
        testWorkflowC = Workflow(spec = "specC.xml", owner = "Steve",
                                 name = "wfC", task = "Test")
        testWorkflowC.create()
        testWorkflowD = Workflow(spec = "specD.xml", owner = "Steve",
                                 name = "wfD", task = "Test")
        testWorkflowD.create()

        self.testSubscriptionA = Subscription(fileset = self.testFilesetA,
                                              workflow = testWorkflowA,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionA.create()
        self.testSubscriptionB = Subscription(fileset = self.testFilesetB,
                                              workflow = testWorkflowB,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionB.create()
        self.testSubscriptionC = Subscription(fileset = self.testFilesetB,
                                              workflow = testWorkflowC,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionC.create()
        self.testSubscriptionD = Subscription(fileset = self.testFilesetB,
                                              workflow = testWorkflowD,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionD.create()        

        deleteWorkflow = Workflow(spec = "specE.xml", owner = "Steve",
                                  name = "wfE", task = "Test")
        deleteWorkflow.create()
        
        self.deleteSubscriptionA = Subscription(fileset = self.testFilesetA,
                                                workflow = deleteWorkflow,
                                                split_algo = "SiblingProcessingBased",
                                                type = "Cleanup")
        self.deleteSubscriptionA.create()
        self.deleteSubscriptionB = Subscription(fileset = self.testFilesetB,
                                                workflow = deleteWorkflow,
                                                split_algo = "SiblingProcessingBased",
                                                type = "Cleanup")
        self.deleteSubscriptionB.create()
        return
    
    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def testSiblingProcessing(self):
        """
        _testSiblingProcessing_

        Verify that the sibling processing split works correctly dealing with
        failed files and acquiring files correctly.
        """
        splitter = SplitterFactory()
        deleteFactoryA = splitter(package = "WMCore.WMBS",
                                  subscription = self.deleteSubscriptionA)
        deleteFactoryB = splitter(package = "WMCore.WMBS",
                                  subscription = self.deleteSubscriptionB)

        result = deleteFactoryA()

        assert len(result) == 0, \
               "Error: No jobs should be returned."

        result = deleteFactoryB()

        assert len(result) == 0, \
               "Error: No jobs should be returned."

        self.testSubscriptionA.completeFiles(self.testFileA)

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 1, \
               "Error: Only one jobgroup should be returned."
        assert len(result[0].jobs) == 1, \
               "Error: There should only be one job in the jobgroup."
        assert len(result[0].jobs[0]["input_files"]) == 1, \
               "Error: Job should only have one input file."
        assert result[0].jobs[0]["input_files"][0]["lfn"] == "testFileA", \
               "Error: Input file for job is wrong."
        
        result = deleteFactoryB(files_per_job = 1)

        assert len(result) == 0, \
               "Error: Second subscription should have no jobs."

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        self.testSubscriptionB.completeFiles(self.testFileD)
        self.testSubscriptionC.failFiles(self.testFileD)

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        result = deleteFactoryB(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        self.testSubscriptionD.failFiles(self.testFileD)

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        result = deleteFactoryB(files_per_job = 1)

        assert len(result) == 1, \
               "Error: One job group should have been created."
        assert len(result[0].jobs) == 1, \
               "Error: There should only be one job in the jobgroup."
        assert len(result[0].jobs[0]["input_files"]) == 1, \
               "Error: Job should only have one input file."
        assert result[0].jobs[0]["input_files"][0]["lfn"] == "testFileD", \
               "Error: Input file for job is wrong."

        self.testSubscriptionB.completeFiles([self.testFileE, self.testFileF])
        self.testSubscriptionC.completeFiles([self.testFileE, self.testFileF])
        self.testSubscriptionD.completeFiles([self.testFileE, self.testFileF])        

        result = deleteFactoryB(files_per_job = 10)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        self.testFilesetB.markOpen(False)

        result = deleteFactoryB(files_per_job = 10)

        assert len(result) == 1, \
               "Error: One jobgroup should have been returned."
        assert len(result[0].jobs) == 1, \
               "Error: There should only be one job in the jobgroup."
        assert len(result[0].jobs[0]["input_files"]) == 2, \
               "Error: Job should only have one input file."

        lfns = [result[0].jobs[0]["input_files"][0]["lfn"], result[0].jobs[0]["input_files"][1]["lfn"]]

        assert "testFileE" in lfns, \
               "Error: TestFileE missing from job input."
        assert "testFileF" in lfns, \
               "Error: TestFileF missing from job input."

        return

if __name__ == '__main__':
    unittest.main()
