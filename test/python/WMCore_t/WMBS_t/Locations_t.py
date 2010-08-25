#!/usr/bin/env python
""" 
Locations_t

Unit tests for the Locations DAO objects.
"""

__revision__ = "$Id: Locations_t.py,v 1.7 2009/09/10 16:03:49 mnorman Exp $"
__version__ = "$Revision: 1.7 $"

import os
import unittest
import threading

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class LocationsTest(unittest.TestCase):
    _setup = False
    _teardown = False

    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_
        
        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        if self._setup:
            return
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        self._setup = True
        return
                                                                
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return
        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        
        self._teardown = True
        return                                                                                

    def testCreateDeleteList(self):
        """
        _testCreateDeleteList_

        Test the creation, listing and deletion of locations in WMBS.
        """

        print "testCreateDeleteList"
        
        goldenLocations = ["goodse.cern.ch", "goodse.fnal.gov"]
        
        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationNew = daoFactory(classname = "Locations.New")
        for location in goldenLocations:
            # The following is intentional, I want to test that inserting the
            # same location multiple times does not cause problems.
            locationNew.execute(siteName = location, jobSlots = 300)
            locationNew.execute(siteName = location, jobSlots = 300)
        
        locationNew.execute(siteName = "empty_site")
        goldenLocations.append("empty_site")

        locationList = daoFactory(classname = "Locations.List")
        currentLocations = locationList.execute()
        for location in currentLocations:
            assert location[1] in goldenLocations, \
                   "ERROR: Unknown location was returned"

            if location[1] == "empty_site":
                assert location[2] == 0, \
                    "ERROR: Site has wrong number of job slots."
            else:
                assert location[2] == 300, \
                    "ERROR: Site has wrong number of job slots."

            goldenLocations.remove(location[1])

        assert len(goldenLocations) == 0, \
               "ERROR: Some locations are missing..."
        
        locationDelete = daoFactory(classname = "Locations.Delete")
        locationDelete.execute(siteName = "goodse.fnal.gov")
        locationDelete.execute(siteName = "goodse.cern.ch")

        currentLocations = locationList.execute()
        assert len(currentLocations) == 1, \
            "ERROR: Not all locations were deleted"
        assert currentLocations[0][1] == "empty_site", \
            "ERROR: The wrong sites were deleted."

        return


    def testListSites(self):
        """

        _testListSites

        Test the ability to list all sites in the database.

        """

        print "testListSites"

        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationNew = daoFactory(classname = "Locations.New")

        locationNew.execute("Satsuma")
        locationNew.execute("Choshu")
        locationNew.execute("Tosa")

        listSites = daoFactory(classname = "Locations.ListSites")
        sites     = listSites.execute()

        self.assertEqual("Satsuma" in sites, True)
        self.assertEqual("Choshu" in sites, True)
        self.assertEqual("Tosa" in sites, True)


    def testJobSlots(self):
        """
        _testJobSlots_
        
        Test our ability to set jobSlots
        """

        print "testJobSlots"

        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationNew = daoFactory(classname = "Locations.New")

        locationNew.execute("Satsuma")
        locationNew.execute("Choshu")
        locationNew.execute("Tosa")

        setSlots = daoFactory(classname = "Locations.SetJobSlots")
        setSlots.execute(siteName = 'Satsuma', jobSlots = 1868)
        setSlots.execute(siteName = 'Choshu',  jobSlots = 1868)
        setSlots.execute(siteName = 'Tosa',    jobSlots = 1868)

        locationList = daoFactory(classname = "Locations.List")
        currentLocations = locationList.execute()

        for location in currentLocations:
            self.assertEqual(location[2], 1868)

        return

        
        
if __name__ == "__main__":
        unittest.main()
