#!/usr/bin/env python
"""
_WorkUnit_t_

Unit tests for the WMBS WorkUnit class.
"""

from __future__ import absolute_import, division, print_function

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File import File
from WMCore.WMBS.WorkUnit import WorkUnit
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit

WF_NAME = 'Test'


class WMBSWorkUnitTest(unittest.TestCase):
    """
    _WorkUnit_t_

    Unit tests for the WMBS WorkUnit class.
    """

    def __init__(self, *args, **kwargs):
        super(WMBSWorkUnitTest, self).__init__(*args, **kwargs)
        self.testWorkflow = None
        self.testFile = None

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger, dbinterface=myThread.dbi)

        self.createPrerequisites()

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        return

    def createPrerequisites(self):
        """
        Create a dummy workflow we can use later
        """

        self.testWorkflow = Workflow(spec="spec.xml", owner="Simon", name="wf001", task=WF_NAME, wfType="ReReco")
        self.testWorkflow.create()

        self.testFile = File(lfn="/this/is/a/test/file0", size=1024, events=10)
        self.testFile.addRun(Run(1, *[44]))
        self.testFile.create()

        return

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the WorkUnit class
        by creating and deleting a WorkUnit.  The exists() method will be
        called before and after creation and after deletion.
        """

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))
        sameWU = WorkUnit(taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))

        self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")
        self.assertFalse(sameWU.exists(), "WorkUnit exists before it was created")

        testWU.create()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist after it was created")
        self.assertTrue(sameWU.exists(), "WorkUnit does not exist after it was created")

        testWU.delete()
        self.assertFalse(testWU.exists(), "WorkUnit exists after it has been deleted")
        self.assertFalse(sameWU.exists(), "WorkUnit exists after it has been deleted")

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Begin a transaction and then create a WorkUnit in the database.  Afterwards,
        rollback the transaction.  Use the WorkUnit class's exists() method to
        to verify that the file doesn't exist before it was created, exists
        after it was created and doesn't exist after the transaction was rolled
        back.
        """

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))

        self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")

        testWU.create()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist after it was created")

        myThread.transaction.rollback()
        self.assertFalse(testWU.exists(), "WorkUnit exists after rollback")

        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a WorkUnit and commit it to the database.  Start a new transaction
        and delete the WorkUnit.  Rollback the transaction after the WorkUnit has been
        deleted.  Use the WorkUnit class's exists() method to verify that the file
        does not exist after it has been deleted but does exist after the
        transaction is rolled back.
        """

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))

        self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")

        testWU.create()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist after it was created")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWU.delete()
        self.assertFalse(testWU.exists(), "WorkUnit exists after it was deleted")

        myThread.transaction.rollback()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist transaction was rolled back")

        return

    def testGetInfo(self):
        """
        _testGetInfo_

        Test the getInfo() method of the WorkUnit class to make sure that it
        returns the correct id. Everything else is tested by the tests in
        DataStructs_t which test the underlying getInfo()
        """

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))
        testWU.create()
        info = testWU.getInfo()
        self.assertEqual(info[0], testWU['id'], "Work unit returned wrong ID")

        return

    def testLoad(self):
        """
        _testLoad_

        Test the loading of WorkUnit data using the ID of a WorkUnit
        """

        testRetries = 10
        testSubmitTime = 10 * 365 * 24 * 3600
        testStatus = 4
        testFirstEvent = 100
        testLastEvent = 600
        testLastUnitCount = 4
        testRunLumi = Run(1, 44)

        intFields = ['id', 'taskid', 'retry_count', 'last_unit_count', 'last_submit_time', 'status',
                     'fileid', 'firstevent', 'lastevent']
        allFields = intFields + ['run_lumi']

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=testLastUnitCount,
                          fileid=self.testFile['id'],
                          runLumi=testRunLumi, retryCount=testRetries, lastSubmitTime=testSubmitTime,
                          status=testStatus, firstEvent=testFirstEvent, lastEvent=testLastEvent)
        testWU.create()

        loadedWU = WorkUnit(wuid=testWU['id'])
        loadedWU.load()

        # Make sure load() gets the type set correctly
        for field in intFields:
            self.assertIsInstance(loadedWU[field], int, 'Field "%s" is not of type int' % field)

        # Make sure the values com back OK
        for field in allFields:
            self.assertEqual(testWU[field], loadedWU[field],
                             'Field "%s" is not returned correctly (got %s instead of %s)' %
                             (field, loadedWU[field], testWU[field]))

        # Now load things by taskid, fileid, run/lumi and run the same tests

        loadByFRL = WorkUnit(taskID=self.testWorkflow.id, fileid=self.testFile['id'], runLumi=testRunLumi)
        loadByFRL.load()

        # Make sure load() gets the type set correctly
        for field in intFields:
            self.assertIsInstance(loadByFRL[field], int, 'Field "%s" is not of type int' % field)

        # Make sure the values com back OK
        for field in allFields:
            self.assertEqual(testWU[field], loadByFRL[field],
                             'Field "%s" is not returned correctly (got %s instead of %s)' %
                             (field, loadByFRL[field], testWU[field]))
        testWU.delete()
        return

        # def testLoadData(self):
        #     """
        #     _testLoadData_
        #
        #     Test the loading of all data from a file, including run/lumi
        #     associations, location information and parentage information.
        #     """
        #     testFileParentA = File(lfn="/this/is/a/parent/lfnA", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentA.addRun(Run(1, *[45]))
        #     testFileParentB = File(lfn="/this/is/a/parent/lfnB", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentB.addRun(Run(1, *[45]))
        #     testFileParentA.create()
        #     testFileParentB.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #     testFileA.setLocation(pnn="T1_US_FNAL_Disk", immediateSave=False)
        #     testFileA.setLocation(pnn="T2_CH_CERN", immediateSave=False)
        #     testFileA.addParent("/this/is/a/parent/lfnA")
        #     testFileA.addParent("/this/is/a/parent/lfnB")
        #     testFileA.updateLocations()
        #
        #     testFileB = File(lfn=testFileA["lfn"])
        #     testFileB.loadData(parentage=1)
        #     testFileC = File(id=testFileA["id"])
        #     testFileC.loadData(parentage=1)
        #
        #     assert testFileA == testFileB, \
        #         "ERROR: File load by LFN didn't work"
        #
        #     assert testFileA == testFileC, \
        #         "ERROR: File load by ID didn't work"
        #
        #     testFileA.delete()
        #     testFileParentA.delete()
        #     testFileParentB.delete()
        #     return
        #
        # def testAddChild(self):
        #     """
        #     _testAddChild_
        #
        #     Add a child to some parent files and make sure that all the parentage
        #     information is loaded/stored correctly from the database.
        #     """
        #     testFileParentA = File(lfn="/this/is/a/parent/lfnA", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentA.addRun(Run(1, *[45]))
        #     testFileParentB = File(lfn="/this/is/a/parent/lfnB", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentB.addRun(Run(1, *[45]))
        #     testFileParentA.create()
        #     testFileParentB.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testFileParentA.addChild("/this/is/a/lfn")
        #     testFileParentB.addChild("/this/is/a/lfn")
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA, testFileParentB]
        #     for parentFile in testFileB["parents"]:
        #         assert parentFile in goldenFiles, \
        #             "ERROR: Unknown parent file"
        #         goldenFiles.remove(parentFile)
        #
        #     assert len(goldenFiles) == 0, \
        #         "ERROR: Some parents are missing"
        #     return
        #
        # def testAddChildTransaction(self):
        #     """
        #     _testAddChildTransaction_
        #
        #     Add a child to some parent files and make sure that all the parentage
        #     information is loaded/stored correctly from the database.  Rollback the
        #     addition of one of the childs and then verify that it does in fact only
        #     have one parent.
        #     """
        #     testFileParentA = File(lfn="/this/is/a/parent/lfnA", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentA.addRun(Run(1, *[45]))
        #     testFileParentB = File(lfn="/this/is/a/parent/lfnB", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentB.addRun(Run(1, *[45]))
        #     testFileParentA.create()
        #     testFileParentB.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testFileParentA.addChild("/this/is/a/lfn")
        #
        #     myThread = threading.currentThread()
        #     myThread.transaction.begin()
        #
        #     testFileParentB.addChild("/this/is/a/lfn")
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA, testFileParentB]
        #     for parentFile in testFileB["parents"]:
        #         assert parentFile in goldenFiles, \
        #             "ERROR: Unknown parent file"
        #         goldenFiles.remove(parentFile)
        #
        #     assert len(goldenFiles) == 0, \
        #         "ERROR: Some parents are missing"
        #
        #     myThread.transaction.rollback()
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA]
        #     for parentFile in testFileB["parents"]:
        #         assert parentFile in goldenFiles, \
        #             "ERROR: Unknown parent file"
        #         goldenFiles.remove(parentFile)
        #
        #     assert len(goldenFiles) == 0, \
        #         "ERROR: Some parents are missing"
        #
        #     return
        #
        # def testCreateWithLocation(self):
        #     """
        #     _testCreateWithLocation_
        #
        #     Create a file and add a couple locations.  Load the file from the
        #     database to make sure that the locations were set correctly.
        #     """
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1},
        #                      locations=set(["T1_US_FNAL_Disk", "T2_CH_CERN"]))
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData()
        #
        #     goldenLocations = ["T1_US_FNAL_Disk", "T2_CH_CERN"]
        #
        #     for location in testFileB["locations"]:
        #         assert location in goldenLocations, \
        #             "ERROR: Unknown file location"
        #         goldenLocations.remove(location)
        #
        #     assert len(goldenLocations) == 0, \
        #         "ERROR: Some locations are missing"
        #     return
        #
        # def testSetLocation(self):
        #     """
        #     _testSetLocation_
        #
        #     Create a file and add a couple locations.  Load the file from the
        #     database to make sure that the locations were set correctly.
        #     """
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testFileA.setLocation(["T1_US_FNAL_Disk", "T2_CH_CERN"])
        #     testFileA.setLocation(["bunkT1_US_FNAL_Disk", "bunkT2_CH_CERN"],
        #                           immediateSave=False)
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData()
        #
        #     goldenLocations = ["T1_US_FNAL_Disk", "T2_CH_CERN"]
        #
        #     for location in testFileB["locations"]:
        #         assert location in goldenLocations, \
        #             "ERROR: Unknown file location"
        #         goldenLocations.remove(location)
        #
        #     assert len(goldenLocations) == 0, \
        #         "ERROR: Some locations are missing"
        #     return
        #
        # def testSetLocationTransaction(self):
        #     """
        #     _testSetLocationTransaction_
        #
        #     Create a file at specific locations and commit everything to the
        #     database.  Reload the file from the database and verify that the
        #     locations are correct.  Rollback the database transaction and once
        #     again reload the file.  Verify that the original locations are back.
        #     """
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #     testFileA.setLocation(["T1_US_FNAL_Disk"])
        #
        #     myThread = threading.currentThread()
        #     myThread.transaction.begin()
        #
        #     testFileA.setLocation(["T2_CH_CERN"])
        #     testFileA.setLocation(["bunkT1_US_FNAL_Disk", "bunkT2_CH_CERN"],
        #                           immediateSave=False)
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData()
        #
        #     goldenLocations = ["T1_US_FNAL_Disk", "T2_CH_CERN"]
        #
        #     for location in testFileB["locations"]:
        #         assert location in goldenLocations, \
        #             "ERROR: Unknown file location"
        #         goldenLocations.remove(location)
        #
        #     assert len(goldenLocations) == 0, \
        #         "ERROR: Some locations are missing"
        #
        #     myThread.transaction.rollback()
        #     testFileB.loadData()
        #
        #     goldenLocations = ["T1_US_FNAL_Disk"]
        #
        #     for location in testFileB["locations"]:
        #         assert location in goldenLocations, \
        #             "ERROR: Unknown file location"
        #         goldenLocations.remove(location)
        #
        #     assert len(goldenLocations) == 0, \
        #         "ERROR: Some locations are missing"
        #     return
        #
        # def testLocationsConstructor(self):
        #     """
        #     _testLocationsConstructor_
        #
        #     Test to make sure that locations passed into the File() constructor
        #     are loaded from and save to the database correctly.  Also test to make
        #     sure that the class behaves well when the location is passed in as a
        #     single string instead of a set.
        #     """
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations=set(["T1_US_FNAL_Disk"]))
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testFileB = File(lfn="/this/is/a/lfn2", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileB.addRun(Run(1, *[45]))
        #     testFileB.create()
        #
        #     testFileC = File(id=testFileA["id"])
        #     testFileC.loadData()
        #
        #     goldenLocations = ["T1_US_FNAL_Disk"]
        #     for location in testFileC["locations"]:
        #         assert location in goldenLocations, \
        #             "ERROR: Unknown file location"
        #         goldenLocations.remove(location)
        #
        #     assert len(goldenLocations) == 0, \
        #         "ERROR: Some locations are missing"
        #
        #     testFileC = File(id=testFileB["id"])
        #     testFileC.loadData()
        #
        #     goldenLocations = ["T1_US_FNAL_Disk"]
        #     for location in testFileC["locations"]:
        #         assert location in goldenLocations, \
        #             "ERROR: Unknown file location"
        #         goldenLocations.remove(location)
        #
        #     assert len(goldenLocations) == 0, \
        #         "ERROR: Some locations are missing"
        #     return
        #
        # def testSetLocationOrder(self):
        #     """
        #     _testSetLocationOrder_
        #
        #     This tests that you can specify a location before creating the file,
        #     instead of having to do it afterwards.
        #     """
        #     myThread = threading.currentThread()
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10)
        #     testFileA.setLocation("T2_CH_CERN")
        #     testFileA.create()
        #
        #     testFileB = File(lfn=testFileA["lfn"])
        #     testFileB.load()
        #
        #     daoFactory = DAOFactory(package="WMCore.WMBS", logger=logging, dbinterface=myThread.dbi)
        #
        #     locationFac = daoFactory(classname="Files.GetLocation")
        #     location = locationFac.execute(testFileB['lfn']).pop()
        #
        #     self.assertEqual(location, 'T2_CH_CERN')
        #
        #     return
        #
        # def testAddRunSet(self):
        #     """
        #     _testAddRunSet_
        #
        #     Test the ability to add run and lumi information to a file.
        #     """
        #     testFile = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                     checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFile.create()
        #     runSet = set()
        #     runSet.add(Run(1, *[45]))
        #     runSet.add(Run(2, *[67, 68]))
        #     testFile.addRunSet(runSet)
        #
        #     assert (runSet - testFile["runs"]) == set(), \
        #         "Error: addRunSet is not updating set correctly"
        #
        #     return
        #
        # def testGetAncestorLFNs(self):
        #     """
        #     _testGenAncestorLFNs_
        #
        #     Create a series of files that have several generations of parentage
        #     information.  Verify that the parentage information is reported
        #     correctly.
        #     """
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileA.create()
        #
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileB.create()
        #
        #     testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileC.create()
        #
        #     testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileD.create()
        #
        #     testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileE.create()
        #
        #     testFileE = File(lfn="/this/is/a/lfnF", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T1_US_FNAL_Disk")
        #     testFileE.create()
        #
        #     testFileA.addParent(lfn="/this/is/a/lfnB")
        #     testFileA.addParent(lfn="/this/is/a/lfnC")
        #     testFileB.addParent(lfn="/this/is/a/lfnD")
        #     testFileC.addParent(lfn="/this/is/a/lfnD")
        #     testFileD.addParent(lfn="/this/is/a/lfnE")
        #     testFileD.addParent(lfn="/this/is/a/lfnF")
        #
        #     level1 = ["/this/is/a/lfnB", "/this/is/a/lfnC"]
        #     level2 = ["/this/is/a/lfnD"]
        #     level3 = ["/this/is/a/lfnE", "/this/is/a/lfnF"]
        #     level4 = level5 = []
        #
        #     decs2 = ["/this/is/a/lfnA"]
        #
        #     assert testFileA.getAncestors(level=1, type='lfn') == level1, \
        #         "ERROR: level 1 test failed"
        #     assert testFileA.getAncestors(level=2, type='lfn') == level2, \
        #         "ERROR: level 2 test failed"
        #     assert testFileA.getAncestors(level=3, type='lfn') == level3, \
        #         "ERROR: level 3 test failed"
        #     assert testFileA.getAncestors(level=4, type='lfn') == level4, \
        #         "ERROR: level 4 test failed"
        #     assert testFileA.getAncestors(level=5, type='lfn') == level5, \
        #         "ERROR: level 5 test failed"
        #
        #     assert testFileD.getDescendants(level=1, type='lfn') == level1, \
        #         "ERROR: level 1 desc test failed"
        #     assert testFileD.getDescendants(level=2, type='lfn') == decs2, \
        #         "ERROR: level 2 desc test failed"
        #     assert testFileD.getDescendants(level=3, type='lfn') == level4, \
        #         "ERROR: level 3 desc test failed"
        #
        #     return
        #
        # def testGetLocationBulk(self):
        #     """
        #     _testGetLocationBulk_
        #
        #     Checks to see whether the code that we have will enable us to get the locations
        #     of all files at once
        #     """
        #     myThread = threading.currentThread()
        #
        #     daoFactory = DAOFactory(package="WMCore.WMBS", logger=logging, dbinterface=myThread.dbi)
        #     locationAction = daoFactory(classname="Locations.New")
        #     locationAction.execute(siteName="site3", pnn="T2_CH_CERN2")
        #     locationAction.execute(siteName="site4", pnn="T2_CH_CERN3")
        #     locationAction.execute(siteName="site5", pnn="T2_CH_CERN4")
        #     locationAction.execute(siteName="site6", pnn="T2_CH_CERN5")
        #     locationAction.execute(siteName="site7", pnn="T2_CH_CERN6")
        #
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T2_CH_CERN")
        #     testFileA.create()
        #
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T2_CH_CERN2")
        #     testFileB.create()
        #
        #     testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T2_CH_CERN3")
        #     testFileC.create()
        #
        #     testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T2_CH_CERN4")
        #     testFileD.create()
        #
        #     testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T2_CH_CERN5")
        #     testFileE.create()
        #
        #     testFileF = File(lfn="/this/is/a/lfnF", size=1024, events=10,
        #                      checksums={'cksum': 1}, locations="T2_CH_CERN6")
        #     testFileF.create()
        #
        #     files = [testFileA, testFileB, testFileC, testFileD, testFileE, testFileF]
        #
        #     locationFac = daoFactory(classname="Files.GetLocationBulk")
        #     location = locationFac.execute(files=files)
        #
        #     for f in files:
        #         self.assertEqual(location[f.exists()], list(f['locations']))
        #
        #     return
        #
        # def testBulkParentage(self):
        #     """
        #     _testBulkParentage_
        #
        #     Verify that the bulk parentage dao correctly sets file parentage.
        #     """
        #     testFileChildA = File(lfn="/this/is/a/child/lfnA", size=1024,
        #                           events=20, checksums={'cksum': 1})
        #     testFileChildB = File(lfn="/this/is/a/child/lfnB", size=1024,
        #                           events=20, checksums={'cksum': 1})
        #     testFileChildA.create()
        #     testFileChildB.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.create()
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileB.create()
        #     testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileC.create()
        #
        #     parentage = [{"child": testFileChildA["id"], "parent": testFileA["id"]},
        #                  {"child": testFileChildA["id"], "parent": testFileB["id"]},
        #                  {"child": testFileChildA["id"], "parent": testFileC["id"]},
        #                  {"child": testFileChildB["id"], "parent": testFileA["id"]},
        #                  {"child": testFileChildB["id"], "parent": testFileB["id"]}]
        #
        #     myThread = threading.currentThread()
        #     daofactory = DAOFactory(package="WMCore.WMBS",
        #                             logger=myThread.logger,
        #                             dbinterface=myThread.dbi)
        #     bulkParentageAction = daofactory(classname="Files.AddBulkParentage")
        #     bulkParentageAction.execute(parentage)
        #
        #     testFileD = File(id=testFileChildA["id"])
        #     testFileD.loadData(parentage=1)
        #     testFileE = File(id=testFileChildB["id"])
        #     testFileE.loadData(parentage=1)
        #
        #     goldenFiles = [testFileA, testFileB, testFileC]
        #     for parentFile in testFileD["parents"]:
        #         assert parentFile in goldenFiles, \
        #             "ERROR: Unknown parent file"
        #         goldenFiles.remove(parentFile)
        #
        #     assert len(goldenFiles) == 0, \
        #         "ERROR: Some parents are missing"
        #
        #     goldenFiles = [testFileA, testFileB]
        #     for parentFile in testFileE["parents"]:
        #         assert parentFile in goldenFiles, \
        #             "ERROR: Unknown parent file"
        #         goldenFiles.remove(parentFile)
        #
        #     assert len(goldenFiles) == 0, \
        #         "ERROR: Some parents are missing"
        #     return
        #
        # def testDataStructsFile(self):
        #     """
        #     _testDataStructsFile_
        #
        #     Tests our ability to create a WMBS file from a DataStructs File and vice versa
        #     """
        #
        #     myThread = threading.currentThread()
        #
        #     testLFN = "lfn1"
        #     testSize = 1024
        #     testEvents = 100
        #     testCksum = {"cksum": '1'}
        #     testParents = set(["lfn2"])
        #     testRun = Run(1, *[45])
        #     testSE = "T2_CH_CERN"
        #
        #     parentFile = File(lfn="lfn2")
        #     parentFile.create()
        #
        #     testFile = File()
        #
        #     inputFile = WMFile(lfn=testLFN, size=testSize, events=testEvents, checksums=testCksum, parents=testParents)
        #     inputFile.addRun(testRun)
        #     inputFile.setLocation(pnn=testSE)
        #
        #     testFile.loadFromDataStructsFile(file=inputFile)
        #     testFile.create()
        #     testFile.save()
        #
        #     loadFile = File(lfn="lfn1")
        #     loadFile.loadData(parentage=1)
        #
        #     self.assertEqual(loadFile['size'], testSize)
        #     self.assertEqual(loadFile['events'], testEvents)
        #     self.assertEqual(loadFile['checksums'], testCksum)
        #     self.assertEqual(loadFile['locations'], set([testSE]))
        #     # self.assertEqual(loadFile['parents'].pop()['lfn'], 'lfn2')
        #
        #     wmFile = loadFile.returnDataStructsFile()
        #     self.assertEqual(wmFile == inputFile, True)
        #
        #     return
        #
        # def testParentageByJob(self):
        #     """
        #     _testParentageByJob_
        #
        #     Tests the DAO that assigns parentage by Job
        #     """
        #
        #     testWorkflow = Workflow(spec='hello', owner="mnorman",
        #                             name="wf001", task="basicWorkload/Production")
        #     testWorkflow.create()
        #     testFileset = Fileset(name="TestFileset")
        #     testFileset.create()
        #     testSubscription = Subscription(fileset=testFileset, workflow=testWorkflow, type="Processing",
        #                                     split_algo="FileBased")
        #     testSubscription.create()
        #     testJobGroup = JobGroup(subscription=testSubscription)
        #     testJobGroup.create()
        #
        #     testFileParentA = File(lfn="/this/is/a/parent/lfnA", size=1024,
        #                            events=20, checksums={'cksum': 1},
        #                            locations=set(['T2_CH_CERN', 'T1_US_FNAL_Disk']))
        #     testFileParentA.addRun(Run(1, *[45]))
        #     testFileParentB = File(lfn="/this/is/a/parent/lfnB", size=1024,
        #                            events=20, checksums={'cksum': 1},
        #                            locations=set(['T2_CH_CERN', 'T1_US_FNAL_Disk']))
        #     testFileParentB.addRun(Run(1, *[45]))
        #     testFileParentA.create()
        #     testFileParentB.create()
        #     testFileset.addFile(testFileParentA)
        #     testFileset.addFile(testFileParentB)
        #     testFileset.commit()
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testJobA = Job()
        #     testJobA["outcome"] = 'success'
        #     testJobA.create(group=testJobGroup)
        #     testJobA.addFile(testFileParentA)
        #     testJobA.addFile(testFileParentB)
        #     testJobA.associateFiles()
        #     testSubscription.acquireFiles()
        #     testJobA.completeInputFiles()
        #
        #     parentAction = self.daofactory(classname="Files.SetParentageByJob")
        #     parentAction.execute(binds={'jobid': testJobA.exists(), 'child': testFileA['lfn']})
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA, testFileParentB]
        #     for parentFile in testFileB["parents"]:
        #         self.assertEqual(parentFile in goldenFiles, True,
        #                          "ERROR: Unknown parent file")
        #         goldenFiles.remove(parentFile)
        #
        #     self.assertEqual(len(goldenFiles), 0,
        #                      "ERROR: Some parents are missing")
        #
        #     testFileC = File(lfn="/this/is/c/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileC.addRun(Run(1, *[46]))
        #     testFileC.create()
        #
        #     testJobC = Job()
        #     testJobC["outcome"] = 'failure'
        #     testJobC.create(group=testJobGroup)
        #     testJobC.addFile(testFileParentA)
        #     testJobC.associateFiles()
        #     testSubscription.acquireFiles()
        #     testJobC.failInputFiles()
        #
        #     parentAction.execute(binds={'jobid': testJobC.exists(), 'child': testFileC['lfn']})
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA, testFileParentB]
        #     for parentFile in testFileB["parents"]:
        #         self.assertEqual(parentFile in goldenFiles, True,
        #                          "ERROR: Unknown parent file")
        #
        #     testFileC_1 = File(id=testFileC["id"])
        #     testFileC_1.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA]
        #     for parentFile in testFileC_1["parents"]:
        #         self.assertEqual(parentFile in goldenFiles, True,
        #                          "ERROR: Unknown parent file")
        #
        # def testParentageByMergeJob(self):
        #     """
        #     _testParentageByJob_
        #
        #     Tests the DAO that assigns parentage by Job
        #     """
        #
        #     testWorkflow = Workflow(spec='hello', owner="mnorman",
        #                             name="wf001", task="basicWorkload/Production")
        #     testWorkflow.create()
        #     testFileset = Fileset(name="TestFileset")
        #     testFileset.create()
        #     testSubscription = Subscription(fileset=testFileset, workflow=testWorkflow, type="Processing",
        #                                     split_algo="FileBased")
        #     testSubscription.create()
        #     testJobGroup = JobGroup(subscription=testSubscription)
        #     testJobGroup.create()
        #
        #     testFileParentA = File(lfn="/this/is/a/parent/lfnA", size=1024,
        #                            events=20, checksums={'cksum': 1},
        #                            locations=set(['T2_CH_CERN', 'T1_US_FNAL_Disk']))
        #     testFileParentA.addRun(Run(1, *[45]))
        #     testFileParentB = File(lfn="/this/is/a/parent/lfnB", size=1024,
        #                            events=20, checksums={'cksum': 1},
        #                            locations=set(['T2_CH_CERN', 'T1_US_FNAL_Disk']))
        #     testFileParentB.addRun(Run(1, *[45]))
        #     testFileParentA.create()
        #     testFileParentB.create()
        #     testFileset.addFile(testFileParentA)
        #     testFileset.addFile(testFileParentB)
        #     testFileset.commit()
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #
        #     testJobA = Job()
        #     testJobA["outcome"] = 'success'
        #     testJobA.create(group=testJobGroup)
        #     testJobA.addFile(testFileParentA)
        #     testJobA.addFile(testFileParentB)
        #     testJobA.associateFiles()
        #     testSubscription.acquireFiles()
        #     testJobA.completeInputFiles()
        #
        #     parentAction = self.daofactory(classname="Files.SetParentageByMergeJob")
        #     parentAction.execute(binds={'jobid': testJobA.exists(), 'child': testFileA['lfn']})
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA, testFileParentB]
        #     for parentFile in testFileB["parents"]:
        #         self.assertEqual(parentFile in goldenFiles, True,
        #                          "ERROR: Unknown parent file")
        #         goldenFiles.remove(parentFile)
        #
        #     self.assertEqual(len(goldenFiles), 0,
        #                      "ERROR: Some parents are missing")
        #
        #     testFileC = File(lfn="/this/is/c/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileC.addRun(Run(1, *[46]))
        #     testFileC.create()
        #
        #     testJobC = Job()
        #     testJobC["outcome"] = 'failure'
        #     testJobC.create(group=testJobGroup)
        #     testJobC.addFile(testFileParentA)
        #     testJobC.associateFiles()
        #     testSubscription.acquireFiles()
        #     testJobC.failInputFiles()
        #
        #     parentAction.execute(binds={'jobid': testJobC.exists(), 'child': testFileC['lfn']})
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA]
        #     for parentFile in testFileB["parents"]:
        #         self.assertEqual(parentFile in goldenFiles, False,
        #                          "ERROR: Unknown parent file")
        #
        #     testFileC_1 = File(id=testFileC["id"])
        #     testFileC_1.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA]
        #     for parentFile in testFileC_1["parents"]:
        #         self.assertEqual(parentFile in goldenFiles, False,
        #                          "ERROR: Unknown parent file")
        #
        # def testAddChecksumsByLFN(self):
        #     """
        #     _testAddChecksumsByLFN_
        #
        #     Tests for adding checksums by DAO by LFN
        #     """
        #
        #     testWorkflow = Workflow(spec='hello', owner="mnorman",
        #                             name="wf001", task="basicWorkload/Production")
        #     testWorkflow.create()
        #     testFileset = Fileset(name="TestFileset")
        #     testFileset.create()
        #     testSubscription = Subscription(fileset=testFileset, workflow=testWorkflow, type="Processing",
        #                                     split_algo="FileBased")
        #     testSubscription.create()
        #     testJobGroup = JobGroup(subscription=testSubscription)
        #     testJobGroup.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        #     testFileB.addRun(Run(1, *[45]))
        #     testFileB.create()
        #
        #     testJobA = Job()
        #     testJobA.create(group=testJobGroup)
        #     testJobA.associateFiles()
        #
        #     parentAction = self.daofactory(classname="Files.AddChecksumByLFN")
        #     binds = [{'lfn': testFileA['lfn'], 'cktype': 'cksum', 'cksum': 101},
        #              {'lfn': testFileA['lfn'], 'cktype': 'adler32', 'cksum': 201},
        #              {'lfn': testFileB['lfn'], 'cktype': 'cksum', 'cksum': 101}]
        #     parentAction.execute(bulkList=binds)
        #
        #     testFileC = File(id=testFileA["id"])
        #     testFileC.loadData()
        #     testFileD = File(id=testFileB["id"])
        #     testFileD.loadData()
        #
        #     self.assertEqual(testFileC['checksums'], {'adler32': '201', 'cksum': '101'})
        #     self.assertEqual(testFileD['checksums'], {'cksum': '101'})
        #
        #     return
        #
        # def testSetLocationByLFN(self):
        #     """
        #     _testSetLocationByLFN_
        #
        #     Create a file and add a couple locations.  Load the file from the
        #     database to make sure that the locations were set correctly.
        #     """
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10,
        #                      checksums={'cksum': 1})
        #     testFileB.addRun(Run(1, *[45]))
        #     testFileB.create()
        #
        #     parentAction = self.daofactory(classname="Files.SetLocationByLFN")
        #     binds = [{'lfn': "/this/is/a/lfnA", 'location': 'T1_US_FNAL_Disk'},
        #              {'lfn': "/this/is/a/lfnB", 'location': 'T1_US_FNAL_Disk'}]
        #     parentAction.execute(lfn=binds)
        #
        #     testFileC = File(id=testFileA["id"])
        #     testFileC.loadData()
        #     testFileD = File(id=testFileB["id"])
        #     testFileD.loadData()
        #
        #     self.assertEqual(testFileC['locations'], set(['T1_US_FNAL_Disk']))
        #     self.assertEqual(testFileD['locations'], set(['T1_US_FNAL_Disk']))
        #
        #     return
        #
        # def testCreateWithParent(self):
        #
        #     """
        #     Test passing parnents arguments in file creation.
        #     check if parent file does not exist, it create the file and set the parentage
        #     """
        #
        #     # create parent file before it got added to child file.
        #     testFileParentA = File(lfn="/this/is/a/parent/lfnA", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentA.addRun(Run(1, *[45]))
        #     testFileParentA.create()
        #
        #     # don't create create parent file before it got added to child file.
        #     testFileParentB = File(lfn="/this/is/a/parent/lfnB", size=1024,
        #                            events=20, checksums={'cksum': 1})
        #     testFileParentB.addRun(Run(1, *[45]))
        #
        #     testFileA = File(lfn="/this/is/a/lfn", size=1024, events=10,
        #                      checksums={'cksum': 1},
        #                      parents=[testFileParentA, testFileParentB])
        #     testFileA.addRun(Run(1, *[45]))
        #
        #     testFileA.create()
        #
        #     testFileB = File(id=testFileA["id"])
        #     testFileB.loadData(parentage=1)
        #
        #     goldenFiles = [testFileParentA, testFileParentB]
        #     for parentFile in testFileB["parents"]:
        #         assert parentFile in goldenFiles, \
        #             "ERROR: Unknown parent file"
        #         goldenFiles.remove(parentFile)
        #
        #     assert len(goldenFiles) == 0, \
        #         "ERROR: Some parents are missing"
        #
        # def testAddToFileset(self):
        #     """
        #     _AddToFileset_
        #
        #     Test to see if we can add to a fileset using the DAO
        #     """
        #     testFileset = Fileset(name="inputFileset")
        #     testFileset.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        #     testFileB.addRun(Run(1, *[45]))
        #     testFileB.create()
        #
        #     addToFileset = self.daofactory(classname="Files.AddToFileset")
        #     addToFileset.execute(file=[testFileA['lfn'], testFileB['lfn']],
        #                          fileset=testFileset.id)
        #
        #     testFileset2 = Fileset(name="inputFileset")
        #     testFileset2.loadData()
        #
        #     self.assertEqual(len(testFileset2.files), 2)
        #     for file in testFileset2.files:
        #         self.assertTrue(file in [testFileA, testFileB])
        #
        #     # Check that adding twice doesn't crash
        #     addToFileset.execute(file=[testFileA['lfn'], testFileB['lfn']],
        #                          fileset=testFileset.id)
        #
        # def testAddDupsToFileset(self):
        #     """
        #     _AddToDupsFileset_
        #
        #     Verify the the dups version of the AddToFileset DAO will not add files
        #     to a fileset if they're already associated to another fileset with the
        #     same workflow.
        #     """
        #     testWorkflowA = Workflow(spec='hello', owner="mnorman",
        #                              name="wf001", task="basicWorkload/Production")
        #     testWorkflowA.create()
        #     testWorkflowB = Workflow(spec='hello', owner="mnorman",
        #                              name="wf001", task="basicWorkload/Production2")
        #     testWorkflowB.create()
        #
        #     testFilesetA = Fileset(name="inputFilesetA")
        #     testFilesetA.create()
        #     testFilesetB = Fileset(name="inputFilesetB")
        #     testFilesetB.create()
        #
        #     testSubscriptionA = Subscription(workflow=testWorkflowA, fileset=testFilesetA)
        #     testSubscriptionA.create()
        #     testSubscriptionB = Subscription(workflow=testWorkflowB, fileset=testFilesetB)
        #     testSubscriptionB.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileA.create()
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        #     testFileB.addRun(Run(1, *[45]))
        #     testFileB.create()
        #
        #     addToFileset = self.daofactory(classname="Files.AddDupsToFileset")
        #     addToFileset.execute(file=[testFileA['lfn'], testFileB['lfn']],
        #                          fileset=testFilesetA.id, workflow="wf001")
        #
        #     testFileset2 = Fileset(name="inputFilesetA")
        #     testFileset2.loadData()
        #
        #     self.assertEqual(len(testFileset2.files), 2)
        #     for file in testFileset2.files:
        #         self.assertTrue(file in [testFileA, testFileB])
        #
        #     # Check that adding twice doesn't crash
        #     addToFileset.execute(file=[testFileA['lfn'], testFileB['lfn']],
        #                          fileset=testFilesetA.id, workflow="wf001")
        #
        #     # Files should not get added to fileset B because fileset A is associated
        #     # with wf001.
        #     addToFileset.execute(file=[testFileA['lfn'], testFileB['lfn']],
        #                          fileset=testFilesetB.id, workflow="wf001")
        #
        #     testFileset2 = Fileset(name="inputFilesetB")
        #     testFileset2.loadData()
        #
        #     self.assertEqual(len(testFileset2.files), 0)
        #     return
        #
        # def testAddDupsToFilesetBulk(self):
        #     """
        #     _AddToDupsFilesetBulk_
        #
        #     Same as testAddDupsToFileset() but faster
        #     """
        #     testWorkflowA = Workflow(spec='hello', owner="mnorman",
        #                              name="wf001", task="basicWorkload/Production")
        #     testWorkflowA.create()
        #     testWorkflowB = Workflow(spec='hello', owner="mnorman",
        #                              name="wf001", task="basicWorkload/Production2")
        #     testWorkflowB.create()
        #
        #     testFilesetA = Fileset(name="inputFilesetA")
        #     testFilesetA.create()
        #     testFilesetB = Fileset(name="inputFilesetB")
        #     testFilesetB.create()
        #
        #     testSubscriptionA = Subscription(workflow=testWorkflowA, fileset=testFilesetA)
        #     testSubscriptionA.create()
        #     testSubscriptionB = Subscription(workflow=testWorkflowB, fileset=testFilesetB)
        #     testSubscriptionB.create()
        #
        #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10, locations=['SiteA'])
        #     testFileA.addRun(Run(1, *[45]))
        #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10, locations=['SiteB'])
        #     testFileB.addRun(Run(1, *[45]))
        #
        #     addFilesToWMBSInBulk(testFilesetA.id, "wf001",
        #                          [testFileA, testFileB],
        #                          conn=testFileA.getDBConn(),
        #                          transaction=testFileA.existingTransaction())
        #
        #     testFileset2 = Fileset(name="inputFilesetA")
        #     testFileset2.loadData()
        #
        #     self.assertEqual(len(testFileset2.files), 2)
        #     for file in testFileset2.files:
        #         self.assertTrue(file in [testFileA, testFileB])
        #
        #     # Check that adding twice doesn't crash
        #     addFilesToWMBSInBulk(testFilesetA.id, "wf001",
        #                          [testFileA, testFileB],
        #                          conn=testFileA.getDBConn(),
        #                          transaction=testFileA.existingTransaction())
        #
        #     # Files should not get added to fileset B because fileset A is associated
        #     # with wf001.
        #     addFilesToWMBSInBulk(testFilesetB.id, "wf001",
        #                          [testFileA, testFileB],
        #                          conn=testFileA.getDBConn(),
        #                          transaction=testFileA.existingTransaction())
        #
        #     testFileset2 = Fileset(name="inputFilesetB")
        #     testFileset2.loadData()
        #
        #     self.assertEqual(len(testFileset2.files), 0)
        #     return
        #
        # def test_SetLocationsForWorkQueue(self):
        #     """
        #     _SetLocationsForWorkQueue_
        #
        #     Test the code that sets locations for the WorkQueue
        #     This is more complicated then it seems.
        #     """
        #
        #     action = self.daofactory(classname="Files.SetLocationForWorkQueue")
        #
        #     testFile = File(lfn="myLFN", size=1024,
        #                     events=10, checksums={'cksum': 1111})
        #     testFile.create()
        #
        #     tFile1 = File(lfn="myLFN")
        #     tFile1.loadData()
        #     locations = tFile1.getLocations()
        #
        #     self.assertEqual(locations, [])
        #
        #     binds = [{'lfn': 'myLFN', 'location': 'T2_CH_CERN'}]
        #     action.execute(lfns=['myLFN'], locations=binds)
        #
        #     tFile1.loadData()
        #     locations = tFile1.getLocations()
        #     self.assertEqual(locations, ['T2_CH_CERN'])
        #
        #     binds = [{'lfn': 'myLFN', 'location': 'T1_US_FNAL_Disk'}]
        #     action.execute(lfns=['myLFN'], locations=binds)
        #
        #     tFile1.loadData()
        #     locations = tFile1.getLocations()
        #     self.assertEqual(locations, ['T1_US_FNAL_Disk'])
        #
        #     return


if __name__ == "__main__":
    unittest.main()
