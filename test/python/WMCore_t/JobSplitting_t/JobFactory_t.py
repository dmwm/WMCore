#!/usr/bin/env python
"""
_JobFactory_t_

Test the job splitting Job Factory
"""




import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow

from WMCore.JobSplitting.JobFactory import JobFactory

class JobFactoryTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def testMetaData(self):
        """
        _testMetaData_

        Make sure that the workflow name, task, owner and white and black lists
        make it into each job object.
        """
        testWorkflow = Workflow(spec = "spec.pkl", owner = "Steve",
                                name = "TestWorkflow", task = "TestTask")

        testFileset = Fileset(name = "TestFileset")
        testFile = File(lfn = "someLFN")
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")

        myJobFactory = JobFactory(subscription = testSubscription)
        testJobGroups =  myJobFactory(siteWhitelist = ["site1"], siteBlacklist = ["site2"])

        for testJobGroup in testJobGroups:
            for job in testJobGroup.jobs:
                assert job["task"] == "TestTask", \
                       "Error: Task is wrong."
                assert job["workflow"] == "TestWorkflow", \
                       "Error: Workflow is wrong."
                assert job["owner"] == "Steve", \
                       "Error: Owner is wrong."
                assert job["siteWhitelist"] == ["site1"], \
                       "Error: Site white list is wrong."
                assert job["siteBlacklist"] == ["site2"], \
                       "Error: Site black list is wrong."
        return

    def testProductionRunNumber(self):
        """
        _testProductionRunNumber_
        
        Verify that jobs created by production subscritpions have the correct
        run number is their job mask.  Also verify that non-production
        subscriptions don't have modified run numbers.
        """
        testWorkflow = Workflow(spec = "spec.pkl", owner = "Steve",
                                name = "TestWorkflow", task = "TestTask")
    
        testFileset = Fileset(name = "TestFileset")
        testFile = File(lfn = "someLFN")
        testFileset.addFile(testFile)
        testFileset.commit()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased",
                                        type = "Production")
    
        myJobFactory = JobFactory(subscription = testSubscription)
        testJobGroups =  myJobFactory()
    
        for testJobGroup in testJobGroups:
            for job in testJobGroup.jobs:
                assert job["mask"]["FirstRun"] == 1, \
                       "Error: First run is wrong."
                assert job["mask"]["LastRun"] == 1, \
                       "Error: Last run is wrong."

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased",
                                        type = "Processing")
    
        myJobFactory = JobFactory(subscription = testSubscription)
        testJobGroups =  myJobFactory()
        
        for testJobGroup in testJobGroups:
            for job in testJobGroup.jobs:
                assert job["mask"]["FirstRun"] == None, \
                       "Error: First run is wrong."
                assert job["mask"]["LastRun"] == None, \
                       "Error: Last run is wrong."

        return
            
if __name__ == '__main__':
    unittest.main()
