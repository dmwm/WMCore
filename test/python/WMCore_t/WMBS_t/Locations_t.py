#!/usr/bin/env python
""" 
Locations_t

Unit tests for the Locations DAO objects.
"""

__revision__ = "$Id: Locations_t.py,v 1.11 2009/12/16 18:55:42 sfoulkes Exp $"
__version__ = "$Revision: 1.11 $"

import os
import unittest
import threading

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class LocationsTest(unittest.TestCase):

    
    def setUp(self):
        """
        _setUp_
        
        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        return
                                                                
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        return

    def testCreateDeleteList(self):
        """
        _testCreateDeleteList_

        Test the creation, listing and deletion of locations in WMBS.
        """

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
