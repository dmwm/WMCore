#!/usr/bin/env python
""" 
_Monitoring_t_

Unit tests for the WMBS Monitoring DAO objects.
"""

__revision__ = "$Id: Monitoring_t.py,v 1.1 2010/01/25 20:15:12 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import threading

from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class MonitoringTest(unittest.TestCase):
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

    def testListJobStates(self):
        """
        _testListJobStates_

        Verify that the ListJobStates DAO works correctly.
        """
        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        listJobStates = daoFactory(classname = "Monitoring.ListJobStates")
        jobStates = listJobStates.execute()

        transitionStates = Transitions().states()
        assert len(jobStates) == len(transitionStates), \
               "Error: Number of states don't match."

        for jobState in jobStates:
            assert jobState in transitionStates, \
                   "Error: Missing job state %s" % jobState

        return

    def testListSubTypes(self):
        """
        _testSubTypes_

        Verify that the ListSubTypes DAO works correctly.
        """
        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        listSubTypes = daoFactory(classname = "Monitoring.ListSubTypes")
        subTypes = listSubTypes.execute()

        schemaTypes = CreateWMBSBase().subTypes
        assert len(subTypes) == len(schemaTypes), \
               "Error: Number of subscription types don't match."

        for subType in subTypes:
            assert subType in schemaTypes, \
                   "Error: Missing subscription type: %s" % subType

        return    
        
if __name__ == "__main__":
        unittest.main()
