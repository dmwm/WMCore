#!/usr/bin/env python
"""
_ResourceControl_t_

Unit tests for ResourceControl.
"""

__revision__ = "$Id: ResourceControl_t.py,v 1.5 2010/03/01 21:19:48 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

import unittest
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription

from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMQuality.TestInit import TestInit
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class ResourceControlTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Install schema and create a DAO factory for WMBS.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMCore.ResourceControl"],
                                useDefault = False)

        myThread = threading.currentThread()        
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)                
        return

    def tearDown(self):
        """
        _tearDown_

        Clear the schema.
        """
        self.testInit.clearDatabase()
        return

    def testInsert(self):
        """
        _testInsert_

        Verify that inserting sites and thresholds works correctly, even is the
        site or threshold already exists.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite1", 10, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite2", 100, "testSE2", "testCE2")

        myResourceControl.insertThreshold("testSite1", "Processing", 10, 20)
        myResourceControl.insertThreshold("testSite1", "Merge", 100, 200) 
        myResourceControl.insertThreshold("testSite1", "Merge", 150, 250)
        myResourceControl.insertThreshold("testSite2", "Processing", 25, 50)
        myResourceControl.insertThreshold("testSite2", "Merge", 75, 135)

        createThresholds =  myResourceControl.listThresholdsForCreate()

        assert len(createThresholds.keys()) == 2, \
               "Error: Wrong number of site in Resource Control DB"
        assert "testSite1" in createThresholds.keys(), \
               "Error: Test Site 1 missing from thresholds."
        assert "testSite2" in createThresholds.keys(), \
               "Error: Test Site 2 missing from thresholds."        
        assert createThresholds["testSite1"]["total_slots"] == 10, \
               "Error: Wrong number of total slots."
        assert createThresholds["testSite1"]["running_jobs"] == 0, \
               "Error: Wrong number of running jobs."
        assert createThresholds["testSite2"]["total_slots"] == 100, \
               "Error: Wrong number of total slots."
        assert createThresholds["testSite2"]["running_jobs"] == 0, \
               "Error: Wrong number of running jobs."        
        
        thresholds = myResourceControl.listThresholdsForSubmit()

        assert len(thresholds.keys()) == 2, \
               "Error: Wrong number of sites in Resource Control DB"
        assert "testSite1" in thresholds.keys(), \
               "Error: testSite1 missing from thresholds."
        assert "testSite2" in thresholds.keys(), \
               "Error: testSite2 missing from thresholds."

        site1Thresholds = thresholds["testSite1"]
        site2Thresholds = thresholds["testSite2"]

        assert len(site1Thresholds) == 2, \
               "Error: Wrong number of task types."
        assert len(site2Thresholds) == 2, \
               "Error: Wrong number of task types."
        assert "Processing" in site1Thresholds.keys(), \
               "Error: Processing task type missing from site1"
        assert "Processing" in site2Thresholds.keys(), \
               "Error: Processing task type missing from site2"
        assert "Merge" in site1Thresholds.keys(), \
               "Error: Merge task type missing from site1"
        assert "Merge" in site2Thresholds.keys(), \
               "Error: Merge task type missing from site2"

        assert site1Thresholds["Processing"]["total_slots"] == 10, \
               "Error: Site thresholds wrong"
        assert site1Thresholds["Processing"]["total_running_jobs"] == 0, \
               "Error: Site thresholds wrong"
        assert site1Thresholds["Processing"]["task_running_jobs"] == 0, \
               "Error: Site thresholds wrong"        
        assert site1Thresholds["Processing"]["max_slots"] == 20, \
               "Error: Site thresholds wrong"
        assert site1Thresholds["Processing"]["min_slots"] == 10, \
               "Error: Site thresholds wrong"

        assert site1Thresholds["Merge"]["total_slots"] == 10, \
               "Error: Site thresholds wrong"
        assert site1Thresholds["Merge"]["total_running_jobs"] == 0, \
               "Error: Site thresholds wrong"
        assert site1Thresholds["Merge"]["task_running_jobs"] == 0, \
               "Error: Site thresholds wrong"        
        assert site1Thresholds["Merge"]["max_slots"] == 250, \
               "Error: Site thresholds wrong"
        assert site1Thresholds["Merge"]["min_slots"] == 150, \
               "Error: Site thresholds wrong"

        assert site2Thresholds["Processing"]["total_slots"] == 100, \
               "Error: Site thresholds wrong"
        assert site2Thresholds["Processing"]["total_running_jobs"] == 0, \
               "Error: Site thresholds wrong"
        assert site2Thresholds["Processing"]["task_running_jobs"] == 0, \
               "Error: Site thresholds wrong"        
        assert site2Thresholds["Processing"]["max_slots"] == 50, \
               "Error: Site thresholds wrong"
        assert site2Thresholds["Processing"]["min_slots"] == 25, \
               "Error: Site thresholds wrong"

        assert site2Thresholds["Merge"]["total_slots"] == 100, \
               "Error: Site thresholds wrong"
        assert site2Thresholds["Merge"]["total_running_jobs"] == 0, \
               "Error: Site thresholds wrong"
        assert site2Thresholds["Merge"]["task_running_jobs"] == 0, \
               "Error: Site thresholds wrong"        
        assert site2Thresholds["Merge"]["max_slots"] == 135, \
               "Error: Site thresholds wrong"
        assert site2Thresholds["Merge"]["min_slots"] == 75, \
               "Error: Site thresholds wrong"                        

        return

    def testList(self):
        """
        _testList_

        Test the functions that list thresholds for creating jobs and submitting
        jobs.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite2", 20, "testSE2", "testCE2")        

        myResourceControl.insertThreshold("testSite1", "Processing", 10, 20)
        myResourceControl.insertThreshold("testSite1", "Merge", 100, 200) 
        myResourceControl.insertThreshold("testSite2", "Processing", 25, 50)
        myResourceControl.insertThreshold("testSite2", "Merge", 75, 135)        

        testWorkflow = Workflow(spec = makeUUID(), owner = "Steve",
                                name = makeUUID(), task = "Test")
        testWorkflow.create()

        testFilesetA = Fileset(name = "TestFilesetA")
        testFilesetA.create()
        testFilesetB = Fileset(name = "TestFilesetB")
        testFilesetB.create()        

        testSubscriptionA = Subscription(fileset = testFilesetA,
                                        workflow = testWorkflow,
                                        type = "Processing")
        testSubscriptionA.create()
        testSubscriptionB = Subscription(fileset = testFilesetB,
                                        workflow = testWorkflow,
                                        type = "Merge")
        testSubscriptionB.create()        

        subLocationAction = self.daoFactory(classname = "Subscriptions.MarkLocation")
        subLocationAction.execute(testSubscriptionA["id"], "testSite1")
        subLocationAction.execute(testSubscriptionB["id"], "testSite1")        

        testJobGroupA = JobGroup(subscription = testSubscriptionA)
        testJobGroupA.create()
        testJobGroupB = JobGroup(subscription = testSubscriptionB)
        testJobGroupB.create()        

        # Has been assigned a location and is complete.
        testJobA = Job(name = makeUUID(), files = [])
        testJobA["couch_record"] = makeUUID()
        testJobA.create(group = testJobGroupA)
        testJobA["state"] = "success"

        # Has been assigned a location and is incomplete.
        testJobB = Job(name = makeUUID(), files = [])
        testJobB["couch_record"] = makeUUID()
        testJobB.create(group = testJobGroupA)
        testJobB["state"] = "executing"

        # Does not have a location.
        testJobC = Job(name = makeUUID(), files = [])
        testJobC["couch_record"] = makeUUID()
        testJobC.create(group = testJobGroupA)        
        testJobC["state"] = "new"

        # Has been assigned a location and is complete.
        testJobD = Job(name = makeUUID(), files = [])
        testJobD["couch_record"] = makeUUID()
        testJobD.create(group = testJobGroupB)        
        testJobD["state"] = "cleanout"

        # Has been assigned a location and is incomplete.
        testJobE = Job(name = makeUUID(), files = [])
        testJobE["couch_record"] = makeUUID()
        testJobE.create(group = testJobGroupB)        
        testJobE["state"] = "new"

        # Does not have a location.
        testJobF = Job(name = makeUUID(), files = [])
        testJobF["couch_record"] = makeUUID()
        testJobF.create(group = testJobGroupB)        
        testJobF["state"] = "new"

        changeStateAction = self.daoFactory(classname = "Jobs.ChangeState")
        changeStateAction.execute(jobs = [testJobA, testJobB, testJobC, testJobD,
                                          testJobE, testJobF])

        setLocationAction = self.daoFactory(classname = "Jobs.SetLocation")
        setLocationAction.execute(testJobA["id"], "testSite1")
        setLocationAction.execute(testJobB["id"], "testSite1")
        setLocationAction.execute(testJobD["id"], "testSite1")
        setLocationAction.execute(testJobE["id"], "testSite1")

        testFilesetC = Fileset(name = "TestFilesetC")
        testFilesetC.create()

        testSubscriptionC = Subscription(fileset = testFilesetC,
                                        workflow = testWorkflow,
                                        type = "Processing")
        testSubscriptionC.create()
        subLocationAction.execute(testSubscriptionC["id"], "testSite1")        
        subLocationAction.execute(testSubscriptionC["id"], "testSite2")

        testJobGroupC = JobGroup(subscription = testSubscriptionC)
        testJobGroupC.create()

        # Has been assigned a location and is complete.
        testJobG = Job(name = makeUUID(), files = [])
        testJobG["couch_record"] = makeUUID()
        testJobG.create(group = testJobGroupC)
        testJobG["state"] = "success"

        # Has been assigned a location and is incomplete.
        testJobH = Job(name = makeUUID(), files = [])
        testJobH["couch_record"] = makeUUID()
        testJobH.create(group = testJobGroupC)
        testJobH["state"] = "executing"

        # No location
        testJobI = Job(name = makeUUID(), files = [])
        testJobI["couch_record"] = makeUUID()
        testJobI.create(group = testJobGroupC)        
        testJobI["state"] = "new"

        changeStateAction.execute(jobs = [testJobG, testJobH, testJobI])
        setLocationAction.execute(testJobG["id"], "testSite2")
        setLocationAction.execute(testJobH["id"], "testSite2")

        createThresholds = myResourceControl.listThresholdsForCreate()
        submitThresholds = myResourceControl.listThresholdsForSubmit()

        assert len(createThresholds.keys()) == 2, \
               "Error: Wrong number of sites in create thresholds"
        assert createThresholds["testSite1"]["total_slots"] == 10, \
               "Error: Wrong number of slots for site 1"
        assert createThresholds["testSite2"]["total_slots"] == 20, \
               "Error: Wrong number of slots for site 2"
        # We should have two running jobs with locations at site one,
        # two running jobs without locations at site one, and one running
        # job without a location at site one and two.
        assert createThresholds["testSite1"]["running_jobs"] == 5, \
               "Error: Wrong number of running jobs for site 1"
        # We should have one running job with a location at site 2 and
        # another running job without a location.
        assert createThresholds["testSite2"]["running_jobs"] == 2, \
               "Error: Wrong number of running jobs for site 2"

        assert submitThresholds["testSite1"]["Merge"]["total_running_jobs"] == 2, \
               "Error: Wrong number of running jobs for submit thresholds."
        assert submitThresholds["testSite1"]["Processing"]["total_running_jobs"] == 2, \
               "Error: Wrong number of running jobs for submit thresholds."
        assert submitThresholds["testSite2"]["Merge"]["total_running_jobs"] == 1, \
               "Error: Wrong number of running jobs for submit thresholds."
        assert submitThresholds["testSite2"]["Processing"]["total_running_jobs"] == 1, \
               "Error: Wrong number of running jobs for submit thresholds."        

        assert submitThresholds["testSite1"]["Merge"]["task_running_jobs"] == 1, \
               "Error: Wrong number of task running jobs for submit thresholds."
        assert submitThresholds["testSite1"]["Processing"]["task_running_jobs"] == 1, \
               "Error: Wrong number of task running jobs for submit thresholds."        
        assert submitThresholds["testSite2"]["Merge"]["task_running_jobs"] == 0, \
               "Error: Wrong number of task running jobs for submit thresholds."
        assert submitThresholds["testSite2"]["Processing"]["task_running_jobs"] == 1, \
               "Error: Wrong number of task running jobs for submit thresholds."

        return

    def testListSiteInfo(self):
        """
        _testListSiteInfo_

        Verify that the listSiteInfo() methods works properly.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite2", 100, "testSE2", "testCE2")                

        siteInfo = myResourceControl.listSiteInfo("testSite1")

        assert siteInfo["site_name"] == "testSite1", \
               "Error: Site name is wrong."
        assert siteInfo["se_name"] == "testSE1", \
               "Error: SE name is wrong."
        assert siteInfo["ce_name"] == "testCE1", \
               "Error: CE name is wrong."
        assert siteInfo["job_slots"] == 10, \
               "Error: Job slots is wrong."

        return

if __name__ == '__main__':
    unittest.main()
