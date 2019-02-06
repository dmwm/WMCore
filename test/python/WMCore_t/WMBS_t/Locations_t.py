#!/usr/bin/env python
"""
Locations_t

Unit tests for the Locations DAO objects.
"""

import threading
import time
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.Transaction import Transaction
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
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
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
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationNew = daoFactory(classname="Locations.New")
        for location in goldenLocations:
            # The following is intentional, I want to test that inserting the
            # same location multiple times does not cause problems.
            locationNew.execute(siteName=location, runningSlots=300, pendingSlots=150)
            locationNew.execute(siteName=location, runningSlots=300, pendingSlots=150)

        locationNew.execute(siteName="empty_site")
        goldenLocations.append("empty_site")

        locationList = daoFactory(classname="Locations.List")
        currentLocations = locationList.execute()
        for location in currentLocations:
            assert location[1] in goldenLocations, \
                "ERROR: Unknown location was returned"

            if location[1] == "empty_site":
                assert location[2] == 0, \
                    "ERROR: Site has wrong number of pending slots."
                self.assertEqual(location[3], 0,
                                 "ERROR: Site has wrong number of running slots")
            else:
                assert location[2] == 150, \
                    "ERROR: Site has wrong number of pending slots."
                self.assertEqual(location[3], 300,
                                 "ERROR: Site has wrong number of running slots")

            goldenLocations.remove(location[1])

        assert len(goldenLocations) == 0, \
            "ERROR: Some locations are missing..."

        locationDelete = daoFactory(classname="Locations.Delete")
        locationDelete.execute(siteName="goodse.fnal.gov")
        locationDelete.execute(siteName="goodse.cern.ch")

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
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationNew = daoFactory(classname="Locations.New")

        locationNew.execute("Satsuma")
        locationNew.execute("Choshu")
        locationNew.execute("Tosa")

        listSites = daoFactory(classname="Locations.ListSites")
        sites = listSites.execute()

        self.assertEqual("Satsuma" in sites, True)
        self.assertEqual("Choshu" in sites, True)
        self.assertEqual("Tosa" in sites, True)

    def testListSitesTransaction(self):
        """
        _testListSitesTransaction_

        Verify that select behave appropriately when dealing with transactions.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        myThread.transaction.begin()

        localTransaction = Transaction(myThread.dbi)
        localTransaction.begin()

        locationNew = daoFactory(classname="Locations.New")

        locationNew.execute("Satsuma", conn=myThread.transaction.conn, transaction=True)
        locationNew.execute("Choshu", conn=myThread.transaction.conn, transaction=True)

        listSites = daoFactory(classname="Locations.ListSites")
        nonTransSites = listSites.execute(conn=localTransaction.conn, transaction=True)
        transSites = listSites.execute(conn=myThread.transaction.conn, transaction=True)

        assert len(nonTransSites) == 0, \
            "Error: Wrong number of sites in non transaction list."
        assert len(transSites) == 2, \
            "Error: Wrong number of sites in transaction list."
        assert "Satsuma" in transSites, \
            "Error: Site missing in transaction list."
        assert "Choshu" in transSites, \
            "Error: Site missing in transaction list."

        localTransaction.commit()
        myThread.transaction.commit()
        return

    def testJobSlots(self):
        """
        _testJobSlots_

        Test our ability to set/get pending and running slots
        """

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationNew = daoFactory(classname="Locations.New")

        locationNew.execute("Satsuma")
        locationNew.execute("Choshu")
        locationNew.execute("Tosa")

        setPendingSlots = daoFactory(classname="Locations.SetPendingSlots")
        setPendingSlots.execute(siteName='Satsuma', jobSlots=1868)
        setPendingSlots.execute(siteName='Choshu', jobSlots=1868)
        setPendingSlots.execute(siteName='Tosa', jobSlots=1868)

        setRunningSlots = daoFactory(classname="Locations.SetRunningSlots")
        setRunningSlots.execute(siteName='Satsuma', jobSlots=42)
        setRunningSlots.execute(siteName='Choshu', jobSlots=42)
        setRunningSlots.execute(siteName='Tosa', jobSlots=42)

        locationList = daoFactory(classname="Locations.List")
        currentLocations = locationList.execute()

        for location in currentLocations:
            self.assertEqual(location[2], 1868)
            self.assertEqual(location[3], 42)

        jobSlotDAO = daoFactory(classname="Locations.GetJobSlots")
        jobSlots = jobSlotDAO.execute("Satsuma")
        self.assertEqual(jobSlots, [{'running_slots': 42, 'pending_slots': 1868}])

        pendingSlotDAO = daoFactory(classname="Locations.GetPendingSlots")
        pendingSlots = pendingSlotDAO.execute("Satsuma")
        self.assertEqual(pendingSlots, 1868)

        runningSlotDAO = daoFactory(classname="Locations.GetRunningSlots")
        runningSlots = runningSlotDAO.execute("Satsuma")
        self.assertEqual(runningSlots, 42)

        return

    def testGetSiteInfo(self):
        """
        _testGetSiteInfo_

        Test our ability to retrieve ce, se names, etc.
        """

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationNew = daoFactory(classname="Locations.New")

        locationNew.execute(siteName="Satsuma", ceName="Satsuma", pnn="Satsuma", runningSlots=50, pendingSlots=20)
        locationNew.execute(siteName="Choshu", ceName="Choshu", pnn="Choshu", runningSlots=50, pendingSlots=20)
        locationNew.execute(siteName="Tosa", ceName="Tosa", pnn="Choshu", runningSlots=50, pendingSlots=20)

        locationInfo = daoFactory(classname="Locations.GetSiteInfo")

        result = locationInfo.execute(siteName="Choshu")

        self.assertEqual(result[0]['ce_name'], 'Choshu')
        self.assertEqual(result[0]['pnn'], 'Choshu')
        self.assertEqual(result[0]['site_name'], 'Choshu')
        self.assertEqual(result[0]['pending_slots'], 20)
        self.assertEqual(result[0]['running_slots'], 50)

        return

    def testDrain(self):
        """Drain sites"""
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        locationNew = daoFactory(classname="Locations.New")
        locationInfo = daoFactory(classname="Locations.GetSiteInfo")
        setState = daoFactory(classname="Locations.SetState")

        # add site and check it's in normal
        locationNew.execute(siteName="Satsuma", ceName="Satsuma", pnn="Satsuma",
                            stateTime=int(time.time()))
        result = locationInfo.execute(siteName="Satsuma")
        self.assertEqual(result[0]['site_name'], 'Satsuma')
        self.assertEqual(result[0]['state'], 'Normal')

        # Now set it to down, it's dead jim
        setState.execute(siteName='Satsuma', state='Down', stateTime=int(time.time()))
        result = locationInfo.execute(siteName="Satsuma")
        self.assertEqual(result[0]['site_name'], 'Satsuma')
        self.assertEqual(result[0]['state'], 'Down')

        # Now let's set it to aborted mode
        setState.execute(siteName='Satsuma', state='Aborted', stateTime=int(time.time()))
        result = locationInfo.execute(siteName="Satsuma")
        self.assertEqual(result[0]['site_name'], 'Satsuma')
        self.assertEqual(result[0]['state'], 'Aborted')

        # Now let's set it to drain mode
        setState.execute(siteName='Satsuma', state='Draining', stateTime=int(time.time()))
        result = locationInfo.execute(siteName="Satsuma")
        self.assertEqual(result[0]['site_name'], 'Satsuma')
        self.assertEqual(result[0]['state'], 'Draining')


if __name__ == "__main__":
    unittest.main()
